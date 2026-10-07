package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestSessionRefreshSerializedAcrossSources(t *testing.T) {
	var calls atomic.Int32
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			fmt.Fprintf(w, `{"issuer":%q,"authorization_endpoint":%q,"token_endpoint":%q,"device_authorization_endpoint":%q,"jwks_uri":%q}`, base, base+"/authorize", base+"/token", base+"/device", base+"/keys")
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("refresh_token") != "original" || r.Form.Get("client_id") != "zephyr-cli" {
				t.Errorf("incorrect refresh request")
			}
			calls.Add(1)
			fmt.Fprint(w, `{"access_token":"fresh","refresh_token":"rotated","token_type":"Bearer","expires_in":3600}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	base = server.URL
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	first, _, err := newSessionSource(options{endpoint: base, issuer: base, noBrowser: true}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	first.config.Token = &oauth2.Token{AccessToken: "expired", RefreshToken: "original", Expiry: time.Now().Add(-time.Minute)}
	first.config.Until = time.Now().Add(time.Hour)
	if err := first.save(); err != nil {
		t.Fatal(err)
	}
	second, _, err := newSessionSource(options{}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, source := range []*sessionSource{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := source.Token(context.Background())
			if err != nil || value != "fresh" {
				t.Errorf("token = %q %v", value, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("refresh requests = %d", calls.Load())
	}
	state, err := readCredentials(first.path)
	if err != nil || state.Token.RefreshToken != "rotated" {
		t.Fatalf("rotation not persisted: %v", err)
	}
	info, err := os.Stat(first.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe session permissions")
	}
	var out bytes.Buffer
	if err := runAuth(context.Background(), "status", options{}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "fresh") || strings.Contains(out.String(), "rotated") {
		t.Fatal("status leaked credentials")
	}
	var metadata map[string]any
	if err := json.Unmarshal(out.Bytes(), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["session_present"] != true {
		t.Fatal(metadata)
	}
}

func TestTemporaryRefreshErrorRetainsSession(t *testing.T) {
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/.well-known/openid-configuration" {
			fmt.Fprintf(w, `{"issuer":%q,"token_endpoint":%q,"device_authorization_endpoint":%q}`, base, base+"/token", base+"/device")
			return
		}
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":"temporarily_unavailable","error_description":"sensitive-provider-detail"}`)
	}))
	defer server.Close()
	base = server.URL
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	source, _, err := newSessionSource(options{endpoint: base, issuer: base}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	source.config.Token = &oauth2.Token{AccessToken: "expired", RefreshToken: "original", Expiry: time.Now().Add(-time.Minute)}
	source.config.Until = time.Now().Add(time.Hour)
	if err := source.save(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err == nil || strings.Contains(err.Error(), "sensitive-provider-detail") {
		t.Fatalf("unsafe error = %v", err)
	}
	stored, err := readCredentials(source.path)
	if err != nil || stored.Token.RefreshToken != "original" {
		t.Fatal("temporary failure destroyed session")
	}
}

func TestSessionSafetyAndLockCancellation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	t.Setenv("ZEPHYR_CONFIG_DIR", dir)
	source, _, err := newSessionSource(options{endpoint: "https://platform.example"}, http.DefaultClient, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	unlock, err := source.lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := source.lock(ctx); err == nil {
		t.Fatal("lock did not respect cancellation")
	}
	unlock()
	if err := os.WriteFile(source.path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readCredentials(source.path); err == nil {
		t.Fatal("world-readable credentials accepted")
	}
	for _, address := range []string{"http://external.example", "https://user:pass@example.test", "https://example.test?query=1", "file:///tmp/token"} {
		if err := validateURL(address); err == nil {
			t.Errorf("unsafe URL accepted: %s", address)
		}
	}
	if _, err := httpClient(filepath.Join(dir, "missing-ca")); err == nil {
		t.Fatal("missing CA ignored")
	}
}
