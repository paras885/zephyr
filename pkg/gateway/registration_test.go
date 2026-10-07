package gateway

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeRegistrationVersionContractsAndExecution(t *testing.T) {
	api, _, _, _, _ := newTestGateway(t)
	source := `type Input { value: string } type Output { status: string }
workflow Live(input: Input) -> Output { return Output { status: input.value }; }`
	register := func(source string, version int) *httptest.ResponseRecorder {
		body, err := json.Marshal(WorkflowRegistration{Source: source, Version: version})
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflows/register", bytes.NewReader(body)))
		return response
	}
	response := register(source, 2)
	if response.Code != 200 {
		t.Fatalf("register = %d %s", response.Code, response.Body)
	}
	var output RegistrationResult
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.Files["client.go"], `"Live", 2, input`) || len(output.Files) != 7 {
		t.Fatalf("missing versioned contracts: %+v", output)
	}
	if repeated := register(source, 2); repeated.Code != 200 {
		t.Fatalf("identical registration = %d", repeated.Code)
	}
	if conflict := register(source+"\n", 2); conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting registration = %d", conflict.Code)
	}
	if invalid := register("invalid DSL", 3); invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid registration = %d", invalid.Code)
	}
	if invalid := register(source, 0); invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid version = %d", invalid.Code)
	}
	run, err := api.StartWorkflow(context.Background(), "Live", 2, map[string]any{"value": "registered"})
	if err != nil || run.Result["status"] != "registered" {
		t.Fatalf("runtime execution = %+v, %v", run, err)
	}
	response = httptest.NewRecorder()
	api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/workflows/Live/contracts?version=2", nil))
	archive, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
	if response.Code != 200 || err != nil || len(archive.File) != 7 {
		t.Fatalf("contract archive = %d, %v", response.Code, err)
	}
	for _, file := range archive.File {
		if file.Name != "client.go" {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || !strings.Contains(string(body), `"Live", 2, input`) {
			t.Fatal("archive client has wrong workflow version")
		}
	}
}
