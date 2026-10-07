package portal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/securecookie"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
	"golang.org/x/oauth2"
)

func testRenewalHandler(t *testing.T, sessions identity.SessionStore, client *fakeOIDCClient) *oidcHandler {
	t.Helper()
	redirect, err := url.Parse(client.redirectURL)
	if err != nil {
		t.Fatal(err)
	}
	return &oidcHandler{
		client: client, sessions: sessions, redirect: redirect,
		codec: securecookie.New([]byte(strings.Repeat("h", 32)), []byte(strings.Repeat("b", 32))),
	}
}

func seedSession(t *testing.T, handler *oidcHandler, token oidcSession) *http.Request {
	t.Helper()
	body, err := handler.codec.Encode("session-data", token)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.sessions.CreateSession(context.Background(), "session-id", identity.Session{Data: body, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/auth/session", nil)
	request.AddCookie(&http.Cookie{Name: oidcSessionCookie, Value: "session-id"})
	return request
}

func TestPortalRefreshSerializedAcrossHandlersAndLogoutRevokes(t *testing.T) {
	sessions := identity.NewMemorySessionStore()
	var calls atomic.Int32
	client := &fakeOIDCClient{
		redirectURL: "https://zephyr.example/auth/callback",
		scopes:      map[string]map[string]bool{"renewed": {"zephyr:workflow:read": true}},
		refresh: func(ctx context.Context, refresh string) (*oauth2.Token, error) {
			if refresh != "original-refresh" {
				t.Errorf("unexpected refresh token %q", refresh)
			}
			calls.Add(1)
			return &oauth2.Token{AccessToken: "renewed", RefreshToken: "rotated", Expiry: time.Now().Add(time.Hour)}, nil
		},
	}
	first := testRenewalHandler(t, sessions, client)
	second := testRenewalHandler(t, sessions, client)
	request := seedSession(t, first, oidcSession{AccessToken: "expired", RefreshToken: "original-refresh", ExpiresAt: time.Now().Add(-time.Minute).Unix()})
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler := first
			if i%2 == 0 {
				handler = second
			}
			session, ok, err := handler.session(request)
			if err != nil || !ok || session.AccessToken != "renewed" || session.RefreshToken != "rotated" {
				t.Errorf("renewal = %+v, %v, %v", session, ok, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("refresh requests = %d, want one", calls.Load())
	}
	logout := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logout.Header.Set("Origin", "https://zephyr.example")
	logout.AddCookie(request.Cookies()[0])
	response := httptest.NewRecorder()
	first.logout(response, logout)
	if response.Code != http.StatusNoContent {
		t.Fatalf("logout = %d", response.Code)
	}
	if _, ok, err := second.session(request); ok || err != nil {
		t.Fatalf("replayed cookie survived logout: %v %v", ok, err)
	}
}

func TestRefreshFailurePreservesTransientSessionAndRevokesInvalidGrant(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "temporary outage", true: "invalid grant"}[invalid], func(t *testing.T) {
			failure := errors.New("provider unavailable")
			if invalid {
				failure = &oauth2.RetrieveError{ErrorCode: "invalid_grant"}
			}
			client := &fakeOIDCClient{redirectURL: "https://zephyr.example/auth/callback", refresh: func(context.Context, string) (*oauth2.Token, error) {
				return nil, failure
			}}
			sessions := identity.NewMemorySessionStore()
			handler := testRenewalHandler(t, sessions, client)
			request := seedSession(t, handler, oidcSession{AccessToken: "expired", RefreshToken: "refresh", ExpiresAt: time.Now().Add(-time.Minute).Unix()})
			_, ok, err := handler.session(request)
			if ok || (invalid && err != nil) || (!invalid && err == nil) {
				t.Fatalf("session result = %v, %v", ok, err)
			}
			storedErr := sessions.UpdateSession(context.Background(), "session-id", func(*identity.Session) error { return nil })
			if invalid && !errors.Is(storedErr, identity.ErrSessionNotFound) {
				t.Fatal("invalid refresh session not revoked")
			}
			if !invalid && storedErr != nil {
				t.Fatal("temporary failure destroyed session")
			}
		})
	}
}

func TestSessionStatusReportsVerifiedCapabilities(t *testing.T) {
	client := &fakeOIDCClient{redirectURL: "https://zephyr.example/auth/callback", scopes: map[string]map[string]bool{
		"viewer": {"zephyr:workflow:read": true},
	}}
	handler := testRenewalHandler(t, identity.NewMemorySessionStore(), client)
	request := seedSession(t, handler, oidcSession{AccessToken: "viewer", ExpiresAt: time.Now().Add(time.Hour).Unix()})
	response := httptest.NewRecorder()
	handler.sessionStatus(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"zephyr:workflow:start":false`) || !strings.Contains(response.Body.String(), `"zephyr:workflow:register":false`) {
		t.Fatalf("capabilities = %d %s", response.Code, response.Body)
	}
}

func TestRefreshRetainsTokenAndAbsoluteLifetime(t *testing.T) {
	client := &fakeOIDCClient{
		redirectURL: "https://zephyr.example/auth/callback",
		scopes:      map[string]map[string]bool{"renewed": {"zephyr:workflow:read": true}},
		refresh: func(context.Context, string) (*oauth2.Token, error) {
			return &oauth2.Token{AccessToken: "renewed", Expiry: time.Now().Add(time.Hour)}, nil
		},
	}
	sessions := identity.NewMemorySessionStore()
	handler := testRenewalHandler(t, sessions, client)
	request := seedSession(t, handler, oidcSession{AccessToken: "expired", RefreshToken: "original", ExpiresAt: time.Now().Add(-time.Minute).Unix()})
	var absoluteExpiry time.Time
	if err := sessions.UpdateSession(context.Background(), "session-id", func(stored *identity.Session) error {
		absoluteExpiry = stored.ExpiresAt
		if strings.Contains(stored.Data, "original") || strings.Contains(stored.Data, "expired") {
			t.Fatal("stored credentials are not encrypted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	session, ok, err := handler.session(request)
	if err != nil || !ok || session.RefreshToken != "original" {
		t.Fatalf("omitted replacement must preserve original refresh token: %+v %v %v", session, ok, err)
	}
	if err := sessions.UpdateSession(context.Background(), "session-id", func(stored *identity.Session) error {
		if !stored.ExpiresAt.Equal(absoluteExpiry) {
			t.Fatal("refresh extended the absolute session lifetime")
		}
		stored.ExpiresAt = time.Now().Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := handler.session(request); ok || err != nil {
		t.Fatalf("expired absolute lifetime must require login: %v %v", ok, err)
	}
}
