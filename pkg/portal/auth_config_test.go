package portal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicCLIAuthDiscovery(t *testing.T) {
	handler, err := NewOIDC(http.NotFoundHandler(), OIDCOptions{
		Client:        &fakeOIDCClient{redirectURL: "https://platform.example/auth/callback"},
		CookieHashKey: []byte(strings.Repeat("h", 32)), CookieBlockKey: []byte(strings.Repeat("b", 32)),
		CLIIssuerURL: "https://identity.example/realms/zephyr", CLIClientID: "consumer-cli",
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/auth/config", nil))
	if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("discovery = %d", response.Code)
	}
	var config map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if len(config) != 3 || config["issuer"] != "https://identity.example/realms/zephyr" || config["client_id"] != "consumer-cli" ||
		config["grant_type"] != "urn:ietf:params:oauth:grant-type:device_code" {
		t.Fatal(config)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/auth/config", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatal(response.Code)
	}
}
