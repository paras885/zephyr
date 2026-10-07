package token

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientCredentialsCachesAndRenews(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, secret, ok := r.BasicAuth()
		if !ok || id != "consumer" || secret != "test-secret" {
			t.Error("missing client authentication")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "client_credentials" {
			t.Error("wrong OAuth grant")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":60}`, calls.Add(1))
	}))
	defer server.Close()
	source, err := NewClientCredentialsSource(ClientCredentialsConfig{TokenURL: server.URL, ClientID: "consumer", ClientSecret: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := source.Token(context.Background())
			if err != nil || value != "token-1" {
				t.Errorf("token = %q, error = %v", value, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("token requests = %d", calls.Load())
	}
	cached := source.(*clientCredentialsSource)
	cached.cached.Expiry = time.Now().Add(5 * time.Second)
	value, err := source.Token(context.Background())
	if err != nil || value != "token-2" {
		t.Fatalf("renewed token = %q, error = %v", value, err)
	}
}

func TestClientCredentialsErrors(t *testing.T) {
	if _, err := NewClientCredentialsSource(ClientCredentialsConfig{}); err == nil {
		t.Fatal("expected invalid configuration error")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid_client", http.StatusUnauthorized)
	}))
	defer server.Close()
	source, err := NewClientCredentialsSource(ClientCredentialsConfig{TokenURL: server.URL, ClientID: "consumer", ClientSecret: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("expected provider error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Token(ctx); err == nil {
		t.Fatal("expected cancellation")
	}
}

func TestClientCredentialsRejectsMissingExpiry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"non-expiring-token","token_type":"Bearer"}`)
	}))
	defer server.Close()
	source, err := NewClientCredentialsSource(ClientCredentialsConfig{TokenURL: server.URL, ClientID: "consumer", ClientSecret: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("expected missing token expiry rejection")
	}
}
