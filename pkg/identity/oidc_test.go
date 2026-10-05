package identity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

func TestOIDCClientDiscoversAndVerifiesJWTs(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var issuerURL string
	var keyMu sync.RWMutex
	currentKey := &privateKey.PublicKey
	currentKeyID := "test-key"
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(response http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(response).Encode(map[string]any{
			"issuer": issuerURL, "authorization_endpoint": issuerURL + "/authorize",
			"token_endpoint": issuerURL + "/token", "jwks_uri": issuerURL + "/keys",
			"response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(response http.ResponseWriter, _ *http.Request) {
		keyMu.RLock()
		publicKey, keyID := currentKey, currentKeyID
		keyMu.RUnlock()
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: publicKey, KeyID: keyID, Algorithm: string(jose.RS256), Use: "sig",
		}}})
	})
	issuer := httptest.NewServer(mux)
	defer issuer.Close()
	issuerURL = issuer.URL

	client, err := NewOIDCClient(context.Background(), OIDCConfig{
		IssuerURL: issuerURL, ClientID: "zephyr-portal", Audience: "zephyr-api",
		RedirectURL: issuerURL + "/auth/callback", Scopes: []string{"zephyr:worker:execute"},
	})
	if err != nil {
		t.Fatal(err)
	}
	validAccess := signTestJWT(t, privateKey, map[string]any{
		"iss": issuerURL, "aud": "zephyr-api", "sub": "worker-1",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
		"scope": "zephyr:worker:execute",
	})
	if err := client.Authenticate(context.Background(), validAccess); err != nil {
		t.Fatalf("valid access token rejected: %v", err)
	}
	if err := client.Authorize(context.Background(), validAccess, "zephyr:worker:execute"); err != nil {
		t.Fatalf("valid worker scope rejected: %v", err)
	}
	if err := client.Authorize(context.Background(), validAccess, "zephyr:workflow:read"); !errors.Is(err, ErrInsufficientScope) {
		t.Fatalf("missing scope error = %v", err)
	}
	validID := signTestJWT(t, privateKey, map[string]any{
		"iss": issuerURL, "aud": "zephyr-portal", "sub": "user-1", "nonce": "nonce-1",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	})
	if err := client.VerifyIDToken(context.Background(), validID, "nonce-1"); err != nil {
		t.Fatalf("valid ID token rejected: %v", err)
	}
	if err := client.VerifyIDToken(context.Background(), validID, "wrong-nonce"); err == nil {
		t.Fatal("ID token with a mismatched nonce was accepted")
	}

	wrongAudience := signTestJWT(t, privateKey, map[string]any{
		"iss": issuerURL, "aud": "another-api", "sub": "worker-1",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	})
	if err := client.Authenticate(context.Background(), wrongAudience); err == nil {
		t.Fatal("access token with the wrong audience was accepted")
	}
	expired := signTestJWT(t, privateKey, map[string]any{
		"iss": issuerURL, "aud": "zephyr-api", "sub": "worker-1",
		"iat": time.Now().Add(-time.Hour).Unix(), "exp": time.Now().Add(-time.Minute).Unix(),
	})
	if err := client.Authenticate(context.Background(), expired); err == nil {
		t.Fatal("expired access token was accepted")
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	wrongSignature := signTestJWT(t, otherKey, map[string]any{
		"iss": issuerURL, "aud": "zephyr-api", "sub": "worker-1",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	})
	if err := client.Authenticate(context.Background(), wrongSignature); err == nil {
		t.Fatal("access token with an unknown signing key was accepted")
	}
	keyMu.Lock()
	currentKey = &otherKey.PublicKey
	currentKeyID = "rotated-key"
	keyMu.Unlock()
	rotated := signTestJWTWithKid(t, otherKey, "rotated-key", map[string]any{
		"iss": issuerURL, "aud": "zephyr-api", "sub": "worker-1",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	})
	if err := client.Authenticate(context.Background(), rotated); err != nil {
		t.Fatalf("access token signed by a rotated JWKS key rejected: %v", err)
	}
}

func signTestJWT(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	return signTestJWTWithKid(t, key, "test-key", claims)
}

func signTestJWTWithKid(t *testing.T, key *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, &jose.SignerOptions{
		ExtraHeaders: map[jose.HeaderKey]any{jose.HeaderKey("kid"): keyID},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := signed.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return compact
}
