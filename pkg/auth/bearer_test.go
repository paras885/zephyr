package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddlewareRequiresValidBearerToken(t *testing.T) {
	authenticator, err := NewStaticTokenAuthenticator("zephyr-secret-token")
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := Middleware(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := middleware(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		called = true
		response.WriteHeader(http.StatusNoContent)
	}))
	for _, authorization := range []string{"", "Basic zephyr-secret-token", "Bearer wrong-token", "Bearer zephyr-secret-token extra"} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("authorization %q returned %d without Bearer challenge", authorization, response.Code)
		}
	}
	if called {
		t.Fatal("protected handler ran for an invalid token")
	}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Authorization", "bEaReR zephyr-secret-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !called {
		t.Fatalf("valid bearer token returned status %d; handler called=%t", response.Code, called)
	}
}

func TestMiddlewareAcceptsPluggableAuthenticator(t *testing.T) {
	expectedContextValue := "tenant-1"
	authenticator := BearerAuthenticatorFunc(func(ctx context.Context, token string) error {
		if token != "signed-token" || ctx.Value(contextKey{}) != expectedContextValue {
			return errors.New("token rejected")
		}
		return nil
	})
	middleware, err := Middleware(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	handler := middleware(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	request = request.WithContext(context.WithValue(request.Context(), contextKey{}, expectedContextValue))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("pluggable authenticator returned status %d", response.Code)
	}
}

func TestStaticTokenAuthenticatorRejectsEmptyConfiguration(t *testing.T) {
	for _, token := range []string{"", " token", "token ", "two words", "line\nbreak"} {
		if _, err := NewStaticTokenAuthenticator(token); err == nil {
			t.Errorf("NewStaticTokenAuthenticator(%q) succeeded", token)
		}
	}
	if _, err := Middleware(nil); err == nil {
		t.Fatal("Middleware accepted a nil authenticator")
	}
}

type contextKey struct{}
