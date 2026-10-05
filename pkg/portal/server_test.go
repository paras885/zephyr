package portal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServesEmbeddedPortalAndForwardsAPI(t *testing.T) {
	api := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"path":"` + request.URL.Path + `"}`))
	})
	handler, err := New(api)
	if err != nil {
		t.Fatal(err)
	}

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Workflow operations") {
		t.Fatalf("portal page returned %d: %s", page.Code, page.Body.String())
	}
	asset := httptest.NewRecorder()
	handler.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, "/portal.js", nil))
	if asset.Code != http.StatusOK || !strings.Contains(asset.Body.String(), "loadRuns") {
		t.Fatalf("portal JavaScript returned %d", asset.Code)
	}
	apiResponse := httptest.NewRecorder()
	handler.ServeHTTP(apiResponse, httptest.NewRequest(http.MethodGet, "/v1/workflows", nil))
	if apiResponse.Code != http.StatusOK || !strings.Contains(apiResponse.Body.String(), "/v1/workflows") {
		t.Fatalf("API forwarding returned %d: %s", apiResponse.Code, apiResponse.Body.String())
	}
}

func TestRequiresAPIHandler(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("portal accepted a nil API handler")
	}
}
