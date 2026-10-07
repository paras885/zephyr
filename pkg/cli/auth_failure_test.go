package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestRevokedRefreshAndExplicitLoginRequireDeviceSignIn(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_login_%v", force), func(t *testing.T) {
			var base string
			var devices, refreshes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					fmt.Fprintf(w, `{"issuer":%q,"token_endpoint":%q,"device_authorization_endpoint":%q}`, base, base+"/token", base+"/device")
				case "/token":
					refreshes.Add(1)
					w.WriteHeader(400)
					fmt.Fprint(w, `{"error":"invalid_grant"}`)
				case "/device":
					devices.Add(1)
					w.WriteHeader(403)
					fmt.Fprint(w, `{"error":"access_denied","error_description":"do-not-log-provider-secrets"}`)
				default:
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			base = server.URL
			t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
			source, _, err := newSessionSource(options{endpoint: base, issuer: base, noBrowser: true}, server.Client(), &bytes.Buffer{})
			if err != nil {
				t.Fatal(err)
			}
			expiry := time.Now().Add(-time.Minute)
			if force {
				expiry = time.Now().Add(time.Hour)
			}
			source.config.Token = &oauth2.Token{AccessToken: "existing", RefreshToken: "revoked", Expiry: expiry}
			source.config.Until = time.Now().Add(time.Hour)
			if err := source.save(); err != nil {
				t.Fatal(err)
			}
			if _, err := source.getToken(context.Background(), force); err == nil || strings.Contains(err.Error(), "do-not-log") {
				t.Fatalf("sign-in denial=%v", err)
			}
			if devices.Load() != 1 || (!force && refreshes.Load() != 1) || (force && refreshes.Load() != 0) {
				t.Fatal("did not start new sign-in correctly")
			}
			state, err := readCredentials(source.path)
			if err != nil || state.Token != nil {
				t.Fatal("revoked/discarded token must not remain cached")
			}
		})
	}
}

func TestLogoutRevocationFailureRetainsSession(t *testing.T) {
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/.well-known/openid-configuration" {
			fmt.Fprintf(w, `{"issuer":%q,"token_endpoint":%q,"device_authorization_endpoint":%q,"revocation_endpoint":%q}`, base, base+"/token", base+"/device", base+"/revoke")
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("token") != "refresh" || r.Form.Get("client_id") != "zephyr-cli" {
			t.Error("invalid revocation request")
		}
		w.WriteHeader(503)
	}))
	defer server.Close()
	base = server.URL
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	source, _, err := newSessionSource(options{endpoint: base, issuer: base}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	source.config.Token = &oauth2.Token{AccessToken: "access", RefreshToken: "refresh", Expiry: time.Now().Add(time.Hour)}
	source.config.Until = time.Now().Add(time.Hour)
	if err := source.save(); err != nil {
		t.Fatal(err)
	}
	if err := runAuth(context.Background(), "logout", options{}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("revocation outage returned success")
	}
	state, err := readCredentials(source.path)
	if err != nil || state.Token == nil {
		t.Fatal("revocation outage destroyed retryable session")
	}
	changed, _, err := newSessionSource(options{endpoint: "https://other.example"}, server.Client(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.config.Token != nil {
		t.Fatal("different endpoint reused identity")
	}
}

func TestAtomicNewFileDoesNotClobber(t *testing.T) {
	file := filepath.Join(t.TempDir(), "client.go")
	if err := os.WriteFile(file, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteMode(file, []byte("replacement"), 0644, false); err == nil {
		t.Fatal("concurrent pre-existing output was clobbered")
	}
	body, err := os.ReadFile(file)
	if err != nil || string(body) != "original" {
		t.Fatal("original file changed")
	}
}
