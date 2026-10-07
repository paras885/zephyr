package token

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type ClientCredentialsConfig struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
}

type clientCredentialsSource struct {
	config clientcredentials.Config
	mu     sync.Mutex
	cached *oauth2.Token
}

func NewClientCredentialsSource(config ClientCredentialsConfig) (Source, error) {
	endpoint, err := url.Parse(config.TokenURL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") {
		return nil, fmt.Errorf("OAuth token URL must be an absolute HTTP or HTTPS URL")
	}
	if config.ClientID == "" || config.ClientSecret == "" {
		return nil, fmt.Errorf("OAuth client ID and secret are required")
	}
	return &clientCredentialsSource{config: clientcredentials.Config{
		TokenURL: config.TokenURL, ClientID: config.ClientID, ClientSecret: config.ClientSecret,
		Scopes: append([]string(nil), config.Scopes...),
	}}, nil
}

func (source *clientCredentialsSource) Token(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if source.cached != nil && source.cached.AccessToken != "" && !source.cached.Expiry.IsZero() && time.Until(source.cached.Expiry) > 10*time.Second {
		return source.cached.AccessToken, nil
	}
	requestContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fresh, err := source.config.TokenSource(requestContext).Token()
	if err != nil {
		return "", fmt.Errorf("obtain service access token: %w", err)
	}
	if fresh.AccessToken == "" || fresh.Expiry.IsZero() || !fresh.Expiry.After(time.Now()) {
		return "", fmt.Errorf("identity provider returned no usable expiring access token")
	}
	source.cached = fresh
	return fresh.AccessToken, nil
}
