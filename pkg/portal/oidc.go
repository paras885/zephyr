package portal

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/securecookie"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
	"golang.org/x/oauth2"
)

const (
	oidcFlowCookie    = "zephyr_oidc_flow"
	oidcSessionCookie = "zephyr_oidc_session"
	oidcCookieMaxAge  = 8 * time.Hour
)

type OIDCOptions struct {
	Client         identity.OIDCAuthenticator
	CookieHashKey  []byte
	CookieBlockKey []byte
	SecureCookies  bool
	Sessions       identity.SessionStore
	CLIIssuerURL   string
	CLIClientID    string
}

type oidcHandler struct {
	api      http.Handler
	portal   http.Handler
	client   identity.OIDCAuthenticator
	codec    *securecookie.SecureCookie
	secure   bool
	redirect *url.URL
	sessions identity.SessionStore
}

type oidcFlow struct {
	State     string
	Nonce     string
	Verifier  string
	ExpiresAt int64
}

type oidcSession struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    int64
}

func NewOIDC(api http.Handler, options OIDCOptions) (http.Handler, error) {
	if api == nil {
		return nil, fmt.Errorf("portal API handler is required")
	}
	if options.Client == nil {
		return nil, fmt.Errorf("OIDC client is required")
	}
	if len(options.CookieHashKey) < 32 || len(options.CookieBlockKey) != 32 {
		return nil, fmt.Errorf("OIDC cookie hash key must be at least 32 bytes and block key must be exactly 32 bytes")
	}
	redirect, err := url.Parse(options.Client.RedirectURL())
	if err != nil || redirect.Scheme == "" || redirect.Host == "" {
		return nil, fmt.Errorf("OIDC redirect URL must be absolute")
	}
	codec := securecookie.New(options.CookieHashKey, options.CookieBlockKey)
	codec.MaxAge(int(oidcCookieMaxAge.Seconds()))
	codec.MaxLength(64 << 10)
	portal, err := New(api)
	if err != nil {
		return nil, err
	}
	sessions := options.Sessions
	if sessions == nil {
		sessions = identity.NewMemorySessionStore()
	}
	handler := &oidcHandler{api: api, portal: portal, client: options.Client, codec: codec, secure: options.SecureCookies, redirect: redirect, sessions: sessions}
	mux := http.NewServeMux()
	if options.CLIIssuerURL != "" {
		issuer, err := url.Parse(options.CLIIssuerURL)
		if err != nil || issuer.Host == "" || issuer.User != nil || issuer.RawQuery != "" || issuer.Fragment != "" ||
			(issuer.Scheme != "https" && !(issuer.Scheme == "http" && (issuer.Hostname() == "localhost" || issuer.Hostname() == "127.0.0.1" || issuer.Hostname() == "::1"))) {
			return nil, fmt.Errorf("CLI issuer must be HTTPS (or loopback HTTP) without credentials, query or fragment")
		}
		clientID := options.CLIClientID
		if clientID == "" {
			clientID = "zephyr-cli"
		}
		mux.HandleFunc("/auth/config", func(response http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet {
				http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(response).Encode(map[string]string{
				"issuer": options.CLIIssuerURL, "client_id": clientID,
				"grant_type": "urn:ietf:params:oauth:grant-type:device_code",
			})
		})
	}
	mux.HandleFunc("/auth/login", handler.login)
	mux.HandleFunc("/auth/callback", handler.callback)
	mux.HandleFunc("/auth/logout", handler.logout)
	mux.HandleFunc("/auth/session", handler.sessionStatus)
	mux.HandleFunc("/v1/", handler.apiRequest)
	mux.Handle("/", http.HandlerFunc(handler.portalRequest))
	return mux, nil
}

func (handler *oidcHandler) login(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state, err := randomURLValue(32)
	if err != nil {
		http.Error(response, "could not start sign-in", http.StatusInternalServerError)
		return
	}
	nonce, err := randomURLValue(32)
	if err != nil {
		http.Error(response, "could not start sign-in", http.StatusInternalServerError)
		return
	}
	verifier, err := randomURLValue(32)
	if err != nil {
		http.Error(response, "could not start sign-in", http.StatusInternalServerError)
		return
	}
	flow := oidcFlow{State: state, Nonce: nonce, Verifier: verifier, ExpiresAt: time.Now().Add(10 * time.Minute).Unix()}
	encoded, err := handler.codec.Encode(oidcFlowCookie, flow)
	if err != nil {
		http.Error(response, "could not start sign-in", http.StatusInternalServerError)
		return
	}
	http.SetCookie(response, handler.cookie(oidcFlowCookie, encoded, "/auth/callback", 10*time.Minute))
	http.Redirect(response, request, handler.client.AuthorizationURL(state, nonce, verifier), http.StatusFound)
}

func (handler *oidcHandler) callback(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flowCookie, err := request.Cookie(oidcFlowCookie)
	if err != nil {
		http.Error(response, "sign-in session expired", http.StatusBadRequest)
		return
	}
	var flow oidcFlow
	if err := handler.codec.Decode(oidcFlowCookie, flowCookie.Value, &flow); err != nil || flow.ExpiresAt < time.Now().Unix() {
		http.SetCookie(response, handler.expiredCookie(oidcFlowCookie, "/auth/callback"))
		http.Error(response, "sign-in session expired", http.StatusBadRequest)
		return
	}
	http.SetCookie(response, handler.expiredCookie(oidcFlowCookie, "/auth/callback"))
	if request.URL.Query().Get("error") != "" || !constantTimeEqual(request.URL.Query().Get("state"), flow.State) {
		http.Error(response, "OIDC sign-in was not authorized", http.StatusUnauthorized)
		return
	}
	code := request.URL.Query().Get("code")
	if code == "" {
		http.Error(response, "OIDC authorization code is required", http.StatusBadRequest)
		return
	}
	token, err := handler.client.Exchange(request.Context(), code, flow.Verifier)
	if err != nil {
		http.Error(response, "OIDC token exchange failed", http.StatusUnauthorized)
		return
	}
	rawIDToken, _ := token.Extra("id_token").(string)
	if rawIDToken == "" || handler.client.VerifyIDToken(request.Context(), rawIDToken, flow.Nonce) != nil {
		http.Error(response, "OIDC identity token is invalid", http.StatusUnauthorized)
		return
	}
	if err := handler.client.Authorize(request.Context(), token.AccessToken, "zephyr:workflow:read"); err != nil {
		http.Error(response, "account lacks workflow read permission", http.StatusForbidden)
		return
	}
	if token.Expiry.IsZero() || !token.Expiry.After(time.Now()) {
		http.Error(response, "OIDC access token has no valid expiry", http.StatusUnauthorized)
		return
	}
	encoded, err := handler.codec.Encode("session-data", oidcSession{AccessToken: token.AccessToken, RefreshToken: token.RefreshToken, ExpiresAt: token.Expiry.Unix()})
	if err != nil {
		http.Error(response, "could not create sign-in session", http.StatusInternalServerError)
		return
	}
	id, err := randomURLValue(32)
	if err != nil {
		http.Error(response, "could not create sign-in session", http.StatusInternalServerError)
		return
	}
	maxAge := oidcCookieMaxAge
	if token.RefreshToken == "" && time.Until(token.Expiry) < maxAge {
		maxAge = time.Until(token.Expiry)
	}
	if err := handler.sessions.CreateSession(request.Context(), id, identity.Session{Data: encoded, ExpiresAt: time.Now().Add(maxAge)}); err != nil {
		handler.sessionError(response, err)
		return
	}
	http.SetCookie(response, handler.cookie(oidcSessionCookie, id, "/", maxAge))
	http.Redirect(response, request, "/", http.StatusSeeOther)
}

func (handler *oidcHandler) logout(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !sameOrigin(request.Header.Get("Origin"), handler.redirect) {
		http.Error(response, "origin check failed", http.StatusForbidden)
		return
	}
	if cookie, err := request.Cookie(oidcSessionCookie); err == nil {
		if err := handler.sessions.DeleteSession(request.Context(), cookie.Value); err != nil {
			handler.sessionError(response, err)
			return
		}
	}
	http.SetCookie(response, handler.expiredCookie(oidcSessionCookie, "/"))
	response.WriteHeader(http.StatusNoContent)
}

func (handler *oidcHandler) sessionStatus(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	session, ok, err := handler.session(request)
	if err != nil {
		handler.sessionError(response, err)
		return
	}
	if !ok || handler.client.Authorize(request.Context(), session.AccessToken, "zephyr:workflow:read") != nil {
		writeUnauthorized(response)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	permissions := make(map[string]bool)
	for _, scope := range []string{"zephyr:workflow:read", "zephyr:workflow:start", "zephyr:workflow:register"} {
		err := handler.client.Authorize(request.Context(), session.AccessToken, scope)
		if err != nil && !errors.Is(err, identity.ErrInsufficientScope) {
			handler.sessionError(response, err)
			return
		}
		permissions[scope] = err == nil
	}
	response.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(response).Encode(map[string]any{"authenticated": true, "permissions": permissions})
}

func (handler *oidcHandler) portalRequest(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	session, ok, err := handler.session(request)
	if err != nil {
		handler.sessionError(response, err)
		return
	}
	if !ok || handler.client.Authorize(request.Context(), session.AccessToken, "zephyr:workflow:read") != nil {
		http.Redirect(response, request, "/auth/login", http.StatusFound)
		return
	}
	handler.portal.ServeHTTP(response, request)
}

func (handler *oidcHandler) apiRequest(response http.ResponseWriter, request *http.Request) {
	token := ""
	fromCookie := len(request.Header.Values("Authorization")) == 0
	if fromCookie && request.Method != http.MethodGet && request.Method != http.MethodHead && !sameOrigin(request.Header.Get("Origin"), handler.redirect) {
		http.Error(response, "origin check failed", http.StatusForbidden)
		return
	}
	if len(request.Header.Values("Authorization")) > 0 {
		token = bearerTokenFromRequest(request)
	} else {
		session, ok, err := handler.session(request)
		if err != nil {
			handler.sessionError(response, err)
			return
		}
		if ok {
			token = session.AccessToken
		}
	}
	if token == "" {
		writeUnauthorized(response)
		return
	}
	requiredScope := requiredScope(request.Method, request.URL.Path)
	if err := handler.client.Authorize(request.Context(), token, requiredScope); err != nil {
		if errors.Is(err, identity.ErrInsufficientScope) {
			response.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope="%s"`, requiredScope))
			http.Error(response, "forbidden", http.StatusForbidden)
			return
		}
		writeUnauthorized(response)
		return
	}
	handler.api.ServeHTTP(response, request)
}

func (handler *oidcHandler) session(request *http.Request) (oidcSession, bool, error) {
	cookie, err := request.Cookie(oidcSessionCookie)
	if err != nil {
		return oidcSession{}, false, nil
	}
	var session oidcSession
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	err = handler.sessions.UpdateSession(ctx, cookie.Value, func(stored *identity.Session) error {
		if err := handler.codec.Decode("session-data", stored.Data, &session); err != nil {
			slog.Warn("invalid encrypted portal session")
			return identity.ErrSessionNotFound
		}
		if session.AccessToken == "" {
			return identity.ErrSessionNotFound
		}
		if session.ExpiresAt > time.Now().Add(15*time.Second).Unix() {
			return nil
		}
		if session.RefreshToken == "" {
			if session.ExpiresAt <= time.Now().Unix() {
				return identity.ErrSessionNotFound
			}
			return nil
		}
		fresh, err := handler.client.Refresh(ctx, session.RefreshToken)
		if err != nil {
			var failure *oauth2.RetrieveError
			if errors.As(err, &failure) && failure.ErrorCode == "invalid_grant" {
				return identity.ErrSessionNotFound
			}
			return fmt.Errorf("portal renewal failed: %w", err)
		}
		if fresh.AccessToken == "" || fresh.Expiry.IsZero() || !fresh.Expiry.After(time.Now()) {
			return fmt.Errorf("identity provider returned invalid renewed token")
		}
		if err := handler.client.Authorize(ctx, fresh.AccessToken, "zephyr:workflow:read"); err != nil {
			return identity.ErrSessionNotFound
		}
		session.AccessToken = fresh.AccessToken
		session.ExpiresAt = fresh.Expiry.Unix()
		if fresh.RefreshToken != "" {
			session.RefreshToken = fresh.RefreshToken
		}
		stored.Data, err = handler.codec.Encode("session-data", session)
		return err
	})
	if errors.Is(err, identity.ErrSessionNotFound) {
		if deleteErr := handler.sessions.DeleteSession(ctx, cookie.Value); deleteErr != nil {
			return oidcSession{}, false, deleteErr
		}
		return oidcSession{}, false, nil
	}
	if err != nil {
		return oidcSession{}, false, err
	}
	return session, true, nil
}

func (handler *oidcHandler) sessionError(response http.ResponseWriter, err error) {
	slog.Error("portal session operation failed", "error_type", fmt.Sprintf("%T", err))
	http.Error(response, "sign-in service temporarily unavailable; retry", http.StatusServiceUnavailable)
}

func (handler *oidcHandler) cookie(name, value, path string, maxAge time.Duration) *http.Cookie {
	seconds := int(maxAge.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return &http.Cookie{
		Name: name, Value: value, Path: path, MaxAge: seconds,
		Expires: time.Now().Add(maxAge), HttpOnly: true, Secure: handler.secure,
		SameSite: http.SameSiteLaxMode,
	}
}

func (handler *oidcHandler) expiredCookie(name, path string) *http.Cookie {
	return &http.Cookie{Name: name, Value: "", Path: path, MaxAge: -1, Expires: time.Unix(1, 0), HttpOnly: true, Secure: handler.secure, SameSite: http.SameSiteLaxMode}
}

func requiredScope(method, path string) string {
	if method == http.MethodPost {
		switch path {
		case "/v1/workflows/register":
			return "zephyr:workflow:register"
		case "/v1/tasks/receive", "/v1/tasks/poll", "/v1/tasks/heartbeat", "/v1/tasks/complete", "/v1/tasks/fail":
			return "zephyr:worker:execute"
		}
		if strings.HasPrefix(path, "/v1/workflows/") && strings.HasSuffix(path, "/instances") {
			return "zephyr:workflow:start"
		}
	}
	if method == http.MethodGet || method == http.MethodHead {
		if path == "/v1/metrics" || path == "/v1/workflows" || path == "/v1/instances" || strings.HasPrefix(path, "/v1/workflows/") || strings.HasPrefix(path, "/v1/instances/") {
			return "zephyr:workflow:read"
		}
	}
	return "zephyr:admin"
}

func bearerTokenFromRequest(request *http.Request) string {
	headers := request.Header.Values("Authorization")
	if len(headers) != 1 {
		return ""
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return ""
	}
	return parts[1]
}

func randomURLValue(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func constantTimeEqual(left, right string) bool {
	if len(left) == 0 || len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func sameOrigin(origin string, expected *url.URL) bool {
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Scheme == expected.Scheme && strings.EqualFold(parsed.Host, expected.Host)
}

func writeUnauthorized(response http.ResponseWriter) {
	response.Header().Set("WWW-Authenticate", `Bearer realm="zephyr", error="invalid_token"`)
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(response).Encode(map[string]string{"error": "unauthorized"})
}
