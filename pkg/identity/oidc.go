package identity

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type OIDCConfig struct {
	IssuerURL    string
	ClientID     string
	ClientSecret string
	Audience     string
	RedirectURL  string
	Scopes       []string
}

type OIDCClient struct {
	provider        *oidc.Provider
	authorization   oauth2.Config
	idTokenVerifier *oidc.IDTokenVerifier
	accessVerifier  *oidc.IDTokenVerifier
}

type OIDCAuthenticator interface {
	AuthorizationURL(state, nonce, verifier string) string
	RedirectURL() string
	Exchange(ctx context.Context, code, verifier string) (*oauth2.Token, error)
	VerifyIDToken(ctx context.Context, rawToken, expectedNonce string) error
	Authorize(ctx context.Context, rawToken, requiredScope string) error
}

var ErrInsufficientScope = errors.New("OIDC access token lacks required scope")

func NewOIDCClient(ctx context.Context, config OIDCConfig) (*OIDCClient, error) {
	if ctx == nil {
		return nil, fmt.Errorf("OIDC context is required")
	}
	if config.IssuerURL == "" || config.ClientID == "" || config.Audience == "" || config.RedirectURL == "" {
		return nil, fmt.Errorf("OIDC issuer, client ID, API audience, and redirect URL are required")
	}
	redirect, err := url.Parse(config.RedirectURL)
	if err != nil || redirect.Scheme == "" || redirect.Host == "" {
		return nil, fmt.Errorf("OIDC redirect URL must be absolute")
	}
	provider, err := oidc.NewProvider(ctx, config.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider: %w", err)
	}
	scopes := append([]string{"openid", "profile", "email"}, config.Scopes...)
	return &OIDCClient{
		provider: provider,
		authorization: oauth2.Config{
			ClientID:     config.ClientID,
			ClientSecret: config.ClientSecret,
			Endpoint:     provider.Endpoint(),
			RedirectURL:  config.RedirectURL,
			Scopes:       uniqueScopes(scopes),
		},
		idTokenVerifier: provider.Verifier(&oidc.Config{ClientID: config.ClientID}),
		accessVerifier:  provider.Verifier(&oidc.Config{ClientID: config.Audience}),
	}, nil
}

func (client *OIDCClient) AuthorizationURL(state, nonce, verifier string) string {
	return client.authorization.AuthCodeURL(
		state,
		oauth2.SetAuthURLParam("nonce", nonce),
		oauth2.S256ChallengeOption(verifier),
	)
}

func (client *OIDCClient) RedirectURL() string {
	return client.authorization.RedirectURL
}

func (client *OIDCClient) Exchange(ctx context.Context, code, verifier string) (*oauth2.Token, error) {
	token, err := client.authorization.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("exchange OIDC authorization code: %w", err)
	}
	if token.AccessToken == "" {
		return nil, fmt.Errorf("OIDC provider returned no access token")
	}
	return token, nil
}

func (client *OIDCClient) VerifyIDToken(ctx context.Context, rawToken, expectedNonce string) error {
	token, err := client.idTokenVerifier.Verify(ctx, rawToken)
	if err != nil {
		return fmt.Errorf("verify OIDC ID token: %w", err)
	}
	if expectedNonce == "" || token.Nonce != expectedNonce {
		return fmt.Errorf("OIDC nonce mismatch")
	}
	return nil
}

func (client *OIDCClient) Authenticate(ctx context.Context, rawToken string) error {
	_, err := client.accessVerifier.Verify(ctx, rawToken)
	if err != nil {
		return fmt.Errorf("verify OIDC access token: %w", err)
	}
	return nil
}

func (client *OIDCClient) Authorize(ctx context.Context, rawToken, requiredScope string) error {
	token, err := client.accessVerifier.Verify(ctx, rawToken)
	if err != nil {
		return fmt.Errorf("verify OIDC access token: %w", err)
	}
	claims := make(map[string]any)
	if err := token.Claims(&claims); err != nil {
		return fmt.Errorf("read OIDC access token claims: %w", err)
	}
	scopes := tokenScopes(claims)
	if scopes["zephyr:admin"] || scopes[requiredScope] {
		return nil
	}
	return fmt.Errorf("%w %q", ErrInsufficientScope, requiredScope)
}

func tokenScopes(claims map[string]any) map[string]bool {
	scopes := make(map[string]bool)
	for _, key := range []string{"scope", "scp", "roles"} {
		switch value := claims[key].(type) {
		case string:
			for _, scope := range strings.Fields(value) {
				scopes[scope] = true
			}
		case []any:
			for _, rawScope := range value {
				if scope, ok := rawScope.(string); ok && scope != "" {
					scopes[scope] = true
				}
			}
		case []string:
			for _, scope := range value {
				if scope != "" {
					scopes[scope] = true
				}
			}
		}
	}
	return scopes
}

func uniqueScopes(scopes []string) []string {
	seen := make(map[string]bool, len(scopes))
	result := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope != "" && !seen[scope] {
			seen[scope] = true
			result = append(result, scope)
		}
	}
	return result
}
