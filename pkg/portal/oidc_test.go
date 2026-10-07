package portal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/identity"
	"golang.org/x/oauth2"
)

func TestOIDCLoginCallbackSessionAndScopedAPI(t *testing.T) {
	client := &fakeOIDCClient{
		redirectURL: "https://zephyr.example/auth/callback",
		scopes: map[string]map[string]bool{
			"portal-access": {"zephyr:workflow:read": true, "zephyr:workflow:start": true},
		},
	}
	apiCalls := 0
	handler, err := NewOIDC(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		apiCalls++
		response.WriteHeader(http.StatusNoContent)
	}), OIDCOptions{
		Client: client, CookieHashKey: []byte(strings.Repeat("h", 32)),
		CookieBlockKey: []byte(strings.Repeat("b", 32)), SecureCookies: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	login := httptest.NewRecorder()
	handler.ServeHTTP(login, httptest.NewRequest(http.MethodGet, "/auth/login", nil))
	if login.Code != http.StatusFound {
		t.Fatalf("login status = %d", login.Code)
	}
	loginURL, err := url.Parse(login.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	query := loginURL.Query()
	if query.Get("state") == "" || query.Get("nonce") == "" || query.Get("code_challenge") == "" || query.Get("code_challenge_method") != "S256" {
		t.Fatalf("authorization URL lacks state, nonce, or S256 PKCE: %s", loginURL)
	}
	flowCookie := login.Result().Cookies()[0]
	if flowCookie.Name != oidcFlowCookie || !flowCookie.HttpOnly || !flowCookie.Secure || flowCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("unsafe OIDC flow cookie: %#v", flowCookie)
	}

	callbackRequest := httptest.NewRequest(http.MethodGet, "/auth/callback?state="+url.QueryEscape(query.Get("state"))+"&code=approved", nil)
	callbackRequest.AddCookie(flowCookie)
	callback := httptest.NewRecorder()
	handler.ServeHTTP(callback, callbackRequest)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/" {
		t.Fatalf("callback returned %d and location %q: %s", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	if client.exchangedCode != "approved" || client.exchangedVerifier == "" || client.verifiedNonce != query.Get("nonce") {
		t.Fatalf("callback did not complete PKCE and nonce checks: %#v", client)
	}
	var sessionCookie *http.Cookie
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == oidcSessionCookie {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil || !sessionCookie.HttpOnly || !sessionCookie.Secure || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("callback did not create a secure session cookie: %#v", sessionCookie)
	}
	if len(sessionCookie.Value) != 43 || strings.Contains(sessionCookie.Value, "portal-access") {
		t.Fatal("browser cookie must contain only an opaque 32-byte session ID")
	}
	if sessionCookie.MaxAge > int(time.Hour.Seconds()) {
		t.Fatal("provider without refresh tokens must not create an eight-hour session")
	}
	for _, path := range []string{"/", "/portal.js", "/portal.css"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(sessionCookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.Len() == 0 || apiCalls != 0 {
			t.Fatalf("authenticated portal asset %s returned %d; api calls=%d", path, response.Code, apiCalls)
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	request.AddCookie(sessionCookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || apiCalls != 1 {
		t.Fatalf("read API with OIDC session returned %d, api calls=%d", response.Code, apiCalls)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/workflows/Checkout/instances", strings.NewReader(`{"version":1}`))
	request.AddCookie(sessionCookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || apiCalls != 1 {
		t.Fatalf("cookie-authenticated cross-origin write returned %d; api calls=%d", response.Code, apiCalls)
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/workflows/Checkout/instances", strings.NewReader(`{"version":1}`))
	request.Header.Set("Origin", "https://zephyr.example")
	request.AddCookie(sessionCookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || apiCalls != 2 {
		t.Fatalf("same-origin workflow start returned %d; api calls=%d", response.Code, apiCalls)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/tasks/heartbeat", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer portal-access")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || apiCalls != 2 {
		t.Fatalf("read-only bearer token worker request returned %d; api calls=%d", response.Code, apiCalls)
	}
}

func TestOIDCCallbackRejectsInvalidStateAndLogoutChecksOrigin(t *testing.T) {
	client := &fakeOIDCClient{redirectURL: "https://zephyr.example/auth/callback"}
	handler, err := NewOIDC(http.NotFoundHandler(), OIDCOptions{
		Client: client, CookieHashKey: []byte(strings.Repeat("h", 32)), CookieBlockKey: []byte(strings.Repeat("b", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	portalRequest := httptest.NewRecorder()
	handler.ServeHTTP(portalRequest, httptest.NewRequest(http.MethodGet, "/", nil))
	if portalRequest.Code != http.StatusFound || portalRequest.Header().Get("Location") != "/auth/login" {
		t.Fatalf("unauthenticated portal request returned %d with location %q", portalRequest.Code, portalRequest.Header().Get("Location"))
	}
	apiRequest := httptest.NewRecorder()
	handler.ServeHTTP(apiRequest, httptest.NewRequest(http.MethodGet, "/v1/workflows", nil))
	if apiRequest.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated API request returned %d", apiRequest.Code)
	}
	login := httptest.NewRecorder()
	handler.ServeHTTP(login, httptest.NewRequest(http.MethodGet, "/auth/login", nil))
	flowCookie := login.Result().Cookies()[0]
	request := httptest.NewRequest(http.MethodGet, "/auth/callback?state=wrong&code=approved", nil)
	request.AddCookie(flowCookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || client.exchangedCode != "" {
		t.Fatalf("invalid state returned %d or exchanged the code", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	request.Header.Set("Origin", "https://attacker.example")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin logout returned %d", response.Code)
	}
}

func TestOIDCRequiresStrongCookieKeys(t *testing.T) {
	client := &fakeOIDCClient{redirectURL: "https://zephyr.example/auth/callback"}
	if _, err := NewOIDC(http.NotFoundHandler(), OIDCOptions{Client: client, CookieHashKey: []byte("short"), CookieBlockKey: []byte(strings.Repeat("b", 32))}); err == nil {
		t.Fatal("OIDC portal accepted a weak cookie hash key")
	}
	if _, err := NewOIDC(http.NotFoundHandler(), OIDCOptions{Client: client, CookieHashKey: []byte(strings.Repeat("h", 32)), CookieBlockKey: []byte("short")}); err == nil {
		t.Fatal("OIDC portal accepted a weak cookie block key")
	}
}

type fakeOIDCClient struct {
	redirectURL       string
	scopes            map[string]map[string]bool
	exchangedCode     string
	exchangedVerifier string
	verifiedNonce     string
	refresh           func(context.Context, string) (*oauth2.Token, error)
}

func (client *fakeOIDCClient) AuthorizationURL(state, nonce, verifier string) string {
	values := url.Values{
		"state": {state}, "nonce": {nonce},
		"code_challenge": {verifier}, "code_challenge_method": {"S256"},
	}
	return "https://identity.example/authorize?" + values.Encode()
}

func (client *fakeOIDCClient) RedirectURL() string { return client.redirectURL }

func (client *fakeOIDCClient) Exchange(_ context.Context, code, verifier string) (*oauth2.Token, error) {
	client.exchangedCode = code
	client.exchangedVerifier = verifier
	return (&oauth2.Token{AccessToken: "portal-access", Expiry: time.Now().Add(time.Hour)}).WithExtra(map[string]any{"id_token": "verified-id"}), nil
}

func (client *fakeOIDCClient) Refresh(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	if client.refresh != nil {
		return client.refresh(ctx, refreshToken)
	}
	return &oauth2.Token{AccessToken: "portal-access", RefreshToken: "rotated-refresh", Expiry: time.Now().Add(time.Hour)}, nil
}

func (client *fakeOIDCClient) VerifyIDToken(_ context.Context, rawToken, nonce string) error {
	if rawToken != "verified-id" {
		return context.Canceled
	}
	client.verifiedNonce = nonce
	return nil
}

func (client *fakeOIDCClient) Authorize(_ context.Context, rawToken, requiredScope string) error {
	if client.scopes[rawToken][requiredScope] || client.scopes[rawToken]["zephyr:admin"] {
		return nil
	}
	return identity.ErrInsufficientScope
}
