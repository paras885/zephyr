package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoadsExampleWorkflowDefinitions(t *testing.T) {
	definitions, err := loadDefinitions("../../workflows")
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 1 || definitions[0].Name != "Checkout" || definitions[0].Version != 1 {
		t.Fatalf("loaded definitions = %#v", definitions)
	}
}

func TestHealthRoutesSeparateLivenessAndReadiness(t *testing.T) {
	ready := false
	handler := withHealthRoutes(http.NotFoundHandler(), func(_ context.Context) error {
		if !ready {
			return errors.New("dependencies are not ready")
		}
		return nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	health, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("liveness status = %d, want 200", health.StatusCode)
	}

	readiness, err := server.Client().Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	readiness.Body.Close()
	if readiness.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d, want 503", readiness.StatusCode)
	}
	ready = true
	readiness, err = server.Client().Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	readiness.Body.Close()
	if readiness.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d, want 200", readiness.StatusCode)
	}
}

func TestMigratePostgresRequiresConnectionString(t *testing.T) {
	if err := migratePostgres(""); err == nil {
		t.Fatal("migratePostgres accepted an empty connection string")
	}
}
