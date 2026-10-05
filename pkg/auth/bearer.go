package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type BearerAuthenticator interface {
	Authenticate(ctx context.Context, token string) error
}

type BearerAuthenticatorFunc func(ctx context.Context, token string) error

func (authenticate BearerAuthenticatorFunc) Authenticate(ctx context.Context, token string) error {
	return authenticate(ctx, token)
}

type StaticTokenAuthenticator struct {
	token []byte
}

func NewStaticTokenAuthenticator(token string) (*StaticTokenAuthenticator, error) {
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("static bearer token must be non-empty and contain no whitespace")
	}
	return &StaticTokenAuthenticator{token: []byte(token)}, nil
}

func (authenticator *StaticTokenAuthenticator) Authenticate(_ context.Context, token string) error {
	provided := []byte(token)
	if len(provided) != len(authenticator.token) || subtle.ConstantTimeCompare(provided, authenticator.token) != 1 {
		return fmt.Errorf("invalid bearer token")
	}
	return nil
}

func Middleware(authenticator BearerAuthenticator) (func(http.Handler) http.Handler, error) {
	if authenticator == nil {
		return nil, fmt.Errorf("bearer authenticator is required")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodOptions {
				next.ServeHTTP(response, request)
				return
			}
			token, ok := bearerToken(request.Header.Values("Authorization"))
			if !ok || authenticator.Authenticate(request.Context(), token) != nil {
				writeUnauthorized(response)
				return
			}
			next.ServeHTTP(response, request)
		})
	}, nil
}

func bearerToken(headers []string) (string, bool) {
	if len(headers) != 1 {
		return "", false
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func writeUnauthorized(response http.ResponseWriter) {
	response.Header().Set("WWW-Authenticate", `Bearer realm="zephyr", error="invalid_token"`)
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(response).Encode(map[string]string{"error": "unauthorized"})
}
