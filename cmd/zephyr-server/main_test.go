package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestValidateDistributedAuthConfigFailsClosed(t *testing.T) {
	if err := validateDistributedAuthConfig(distributedConfig{}); err == nil || !strings.Contains(err.Error(), "OIDC_ISSUER_URL") {
		t.Fatalf("missing OIDC config error = %v", err)
	}
	config := distributedConfig{
		OIDCIssuerURL: "https://identity.example.com/", OIDCClientID: "zephyr-portal",
		OIDCAudience: "zephyr-api", OIDCRedirectURL: "https://zephyr.example.com/auth/callback",
		OIDCCookieHashKey:  base64.StdEncoding.EncodeToString([]byte(strings.Repeat("h", 32))),
		OIDCCookieBlockKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", 32))),
	}
	if err := validateDistributedAuthConfig(config); err != nil {
		t.Fatalf("valid OIDC configuration rejected: %v", err)
	}
	config.Token = "legacy-token"
	if err := validateDistributedAuthConfig(config); err == nil {
		t.Fatal("ungated static token was accepted in distributed mode")
	}
	config.Token = "development-token"
	config.Environment = "development"
	config.DevelopmentStaticAuth = true
	if err := validateDistributedAuthConfig(config); err != nil {
		t.Fatalf("explicit development static-token mode rejected: %v", err)
	}
	config.Environment = "production"
	if err := validateDistributedAuthConfig(config); err == nil {
		t.Fatal("static token was accepted outside development")
	}
}

func TestValidateDistributedAuthConfigRejectsWeakOIDCCookieKeys(t *testing.T) {
	config := distributedConfig{
		OIDCIssuerURL: "https://identity.example.com/", OIDCClientID: "zephyr-portal",
		OIDCAudience: "zephyr-api", OIDCRedirectURL: "https://zephyr.example.com/auth/callback",
		OIDCCookieHashKey:  base64.StdEncoding.EncodeToString([]byte("short")),
		OIDCCookieBlockKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", 32))),
	}
	if err := validateDistributedAuthConfig(config); err == nil || !strings.Contains(err.Error(), "OIDC_COOKIE_HASH_KEY") {
		t.Fatalf("weak cookie key error = %v", err)
	}
}
