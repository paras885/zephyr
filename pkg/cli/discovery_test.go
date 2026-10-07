package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestPlatformAuthDiscovery(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_client_%v", explicit), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/auth/config" || r.Header.Get("Authorization") != "" {
					t.Error("public discovery request must not carry tokens")
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"issuer":"https://identity.example/realm","client_id":"platform-cli","grant_type":"urn:ietf:params:oauth:grant-type:device_code"}`)
			}))
			defer server.Close()
			t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
			opts := options{endpoint: server.URL}
			if explicit {
				opts.clientID = "override-cli"
			}
			source, _, err := newSessionSource(opts, server.Client(), &bytes.Buffer{})
			if err != nil {
				t.Fatal(err)
			}
			if err := source.discoverAuth(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := "platform-cli"
			if explicit {
				want = "override-cli"
			}
			if source.config.Issuer != "https://identity.example/realm" || source.config.ClientID != want {
				t.Fatal(source.config.ClientID)
			}
			if err := source.save(); err != nil {
				t.Fatal(err)
			}
			saved, _, err := newSessionSource(options{}, server.Client(), &bytes.Buffer{})
			if err != nil || saved.config.Issuer != source.config.Issuer {
				t.Fatal("discovery not persisted")
			}
		})
	}
}

func TestDiscoveryRejectsInvalidResponses(t *testing.T) {
	for _, body := range []string{
		`{"issuer":"http://external.example","client_id":"cli","grant_type":"urn:ietf:params:oauth:grant-type:device_code"}`,
		`{"issuer":"https://identity.example","client_id":"","grant_type":"urn:ietf:params:oauth:grant-type:device_code"}`,
		`{"issuer":"https://identity.example","client_id":"cli","grant_type":"password"}`,
		`{broken`,
		`{"issuer":"https://identity.example","client_id":"cli","grant_type":"urn:ietf:params:oauth:grant-type:device_code"} {}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, body)
		}))
		source := sessionSource{config: credentials{Endpoint: server.URL}, http: server.Client()}
		if err := source.discoverAuth(context.Background()); err == nil {
			t.Errorf("invalid metadata accepted: %s", body)
		}
		server.Close()
	}
	for _, status := range []int{302, 404, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
		source := sessionSource{config: credentials{Endpoint: server.URL}, http: server.Client()}
		if err := source.discoverAuth(context.Background()); err == nil {
			t.Errorf("status %d accepted", status)
		}
		server.Close()
	}
}

func TestNewEndpointDiscardsOldIssuerAndCA(t *testing.T) {
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	source, _, err := newSessionSource(options{endpoint: "https://old.example", issuer: "https://old-identity.example", clientID: "old-cli"}, http.DefaultClient, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}

	source.config.CAFile = "/missing/old-trust.crt"
	source.config.Token = &oauth2.Token{AccessToken: "old-token", Expiry: time.Now().Add(time.Hour)}
	if err := source.save(); err != nil {
		t.Fatal(err)
	}
	changed, _, err := newSessionSource(options{endpoint: "https://new.example"}, http.DefaultClient, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.config.Issuer != "" || changed.config.CAFile != "" || changed.config.Token != nil || changed.config.ClientID != "zephyr-cli" {
		t.Fatal("new endpoint inherited old provider/trust")
	}
}

func TestDiscoveryUsesExistingTLSRootTrustWithoutCAFlag(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"issuer":"https://identity.example","client_id":"zephyr-cli","grant_type":"urn:ietf:params:oauth:grant-type:device_code"}`)
	}))
	defer server.Close()
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	source, _, err := newSessionSource(options{endpoint: server.URL}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.discoverAuth(context.Background()); err != nil {
		t.Fatalf("existing TLS trust should need no CA flag: %v", err)
	}
	untrusted, err := httpClient("")
	if err != nil {
		t.Fatal(err)
	}
	source.http = untrusted
	if err := source.discoverAuth(context.Background()); err == nil {
		t.Fatal("discovery must not disable verification for an untrusted certificate")
	}
}
