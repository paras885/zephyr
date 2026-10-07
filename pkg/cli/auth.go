package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type credentials struct {
	Endpoint string        `json:"endpoint"`
	Issuer   string        `json:"issuer"`
	ClientID string        `json:"client_id"`
	CAFile   string        `json:"ca_file,omitempty"`
	Token    *oauth2.Token `json:"token,omitempty"`
	Until    time.Time     `json:"session_expires_at,omitempty"`
}

type sessionSource struct {
	path           string
	config         credentials
	http           *http.Client
	stderr         io.Writer
	noBrowser      bool
	clientOverride bool
}

func httpClient(caFile string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		body, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read trusted CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("read system trust: %w", err)
		}
		if !roots.AppendCertsFromPEM(body) {
			return nil, fmt.Errorf("CA file contains no certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Transport: transport, Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func privateDirectory() (string, error) {
	dir := os.Getenv("ZEPHYR_CONFIG_DIR")
	if dir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(base, "zephyr")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("CLI configuration directory must be a real directory accessible only to its owner (0700): %s", dir)
	}
	return dir, nil
}

func readCredentials(path string) (credentials, error) {
	var state credentials
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		return state, fmt.Errorf("unsafe session file; expected a private regular file (0600)")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return state, fmt.Errorf("invalid session file; remove it or restore a valid session")
	}
	return state, nil
}

func newSessionSource(opts options, transport *http.Client, stderr io.Writer) (*sessionSource, string, error) {
	dir, err := privateDirectory()
	if err != nil {
		return nil, "", err
	}
	path := filepath.Join(dir, "session.json")
	state, err := readCredentials(path)
	if err != nil {
		return nil, "", err
	}
	previous := state
	if opts.endpoint != "" {
		state.Endpoint = strings.TrimRight(opts.endpoint, "/")
		if state.Endpoint != previous.Endpoint {
			state.Issuer = ""
			state.ClientID = ""
			state.CAFile = ""
		}
	}
	if opts.issuer != "" {
		state.Issuer = strings.TrimRight(opts.issuer, "/")
	}
	if opts.clientID != "" {
		state.ClientID = opts.clientID
	}
	if opts.caFile != "" {
		state.CAFile, err = filepath.Abs(opts.caFile)
		if err != nil {
			return nil, "", err
		}
	}
	if state.ClientID == "" {
		state.ClientID = "zephyr-cli"
	}
	if previous.Endpoint != state.Endpoint || previous.Issuer != state.Issuer || previous.ClientID != state.ClientID {
		state.Token = nil
		state.Until = time.Time{}
	}
	if state.Endpoint == "" {
		return nil, "", fmt.Errorf("provide --endpoint or ZEPHYR_ENDPOINT (or run zephyr auth login with --endpoint)")
	}
	if err := validateURL(state.Endpoint); err != nil {
		return nil, "", fmt.Errorf("invalid platform endpoint: %w", err)
	}
	if opts.caFile == "" && state.CAFile != "" {
		transport, err = httpClient(state.CAFile)
		if err != nil {
			return nil, "", err
		}
	}
	return &sessionSource{path: path, config: state, http: transport, stderr: stderr, noBrowser: opts.noBrowser, clientOverride: opts.clientID != ""}, state.Endpoint, nil
}

func validateURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("expected an absolute URL without credentials, query or fragment")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && (parsed.Hostname() == "localhost" || parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1")) {
		return fmt.Errorf("HTTPS is required except for loopback development")
	}
	return nil
}

func atomicWrite(path string, body []byte, mode os.FileMode) error {
	return atomicWriteMode(path, body, mode, true)
}

func atomicWriteMode(path string, body []byte, mode os.FileMode, replace bool) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".zephyr-write-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(body); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if !replace {
		return os.Link(file.Name(), path)
	}
	return os.Rename(file.Name(), path)
}

func (source *sessionSource) lock(ctx context.Context) (func(), error) {
	// Single-use refresh tokens must not be replayed by concurrent CLI processes.
	path := source.path + ".lock"
	for {
		err := os.Mkdir(path, 0700)
		if err == nil {
			return func() {
				if err := os.Remove(path); err != nil {
					fmt.Fprintln(source.stderr, "Could not release session lock:", err)
				}
			}, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("session is locked by another CLI process; if it crashed, remove %s after confirming no CLI is running: %w", path, ctx.Err())
		case <-timer.C:
		}
	}
}

func (source *sessionSource) save() error {
	body, err := json.Marshal(source.config)
	if err != nil {
		return err
	}
	if err := atomicWrite(source.path, body, 0600); err != nil {
		return fmt.Errorf("could not persist renewed session; re-login may be required: %w", err)
	}
	return nil
}

func (source *sessionSource) provider(ctx context.Context) (*oidc.Provider, *oauth2.Config, error) {
	if source.config.Issuer == "" {
		if err := source.discoverAuth(ctx); err != nil {
			return nil, nil, err
		}
	}
	if err := validateURL(source.config.Issuer); err != nil {
		return nil, nil, fmt.Errorf("invalid issuer: %w", err)
	}
	provider, err := oidc.NewProvider(ctx, source.config.Issuer)
	if err != nil {
		return nil, nil, fmt.Errorf("OIDC discovery failed: %w", err)
	}
	endpoint := provider.Endpoint()
	endpoint.AuthStyle = oauth2.AuthStyleInParams
	for _, value := range []string{endpoint.TokenURL, endpoint.DeviceAuthURL} {
		if value == "" {
			return nil, nil, fmt.Errorf("provider does not support device authorization")
		}
		if err := validateURL(value); err != nil {
			return nil, nil, fmt.Errorf("unsafe identity endpoint: %w", err)
		}
	}
	return provider, &oauth2.Config{ClientID: source.config.ClientID, Endpoint: endpoint, Scopes: []string{oidc.ScopeOpenID, "profile", "email"}}, nil
}

func (source *sessionSource) discoverAuth(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.config.Endpoint+"/auth/config", nil)
	if err != nil {
		return err
	}
	response, err := source.http.Do(request)
	if err != nil {
		return fmt.Errorf("platform auth discovery failed (check endpoint and certificate trust): %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("platform auth discovery returned %d; configure /auth/config or supply --issuer explicitly", response.StatusCode)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		return fmt.Errorf("platform auth discovery did not return JSON")
	}
	var metadata struct {
		Issuer    string `json:"issuer"`
		ClientID  string `json:"client_id"`
		GrantType string `json:"grant_type"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 16<<10))
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("invalid platform auth discovery response")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("invalid platform auth discovery response: expected one object")
	}
	if err := validateURL(metadata.Issuer); err != nil {
		return fmt.Errorf("unsafe discovered issuer: %w", err)
	}
	if strings.TrimSpace(metadata.ClientID) == "" || metadata.GrantType != "urn:ietf:params:oauth:grant-type:device_code" {
		return fmt.Errorf("platform auth discovery lacks a public device-flow client")
	}
	source.config.Issuer = strings.TrimRight(metadata.Issuer, "/")
	if !source.clientOverride {
		source.config.ClientID = metadata.ClientID
	}
	return nil
}

func (source *sessionSource) Token(ctx context.Context) (string, error) {
	return source.getToken(ctx, false)
}

func (source *sessionSource) getToken(ctx context.Context, forceLogin bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, source.http)
	unlock, err := source.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	stored, err := readCredentials(source.path)
	if err != nil {
		return "", err
	}
	if !forceLogin && stored.Endpoint == source.config.Endpoint && stored.Issuer == source.config.Issuer && stored.ClientID == source.config.ClientID {
		source.config.Token, source.config.Until = stored.Token, stored.Until
	} else {
		source.config.Token = nil
	}
	token := source.config.Token
	if token != nil && time.Now().Before(source.config.Until) && token.Expiry.After(time.Now().Add(15*time.Second)) && token.AccessToken != "" {
		return token.AccessToken, nil
	}
	provider, config, err := source.provider(ctx)
	if err != nil {
		return "", err
	}
	if token != nil && token.RefreshToken != "" && time.Now().Before(source.config.Until) {
		fresh, err := config.TokenSource(ctx, &oauth2.Token{RefreshToken: token.RefreshToken}).Token()
		if err == nil {
			if err := validToken(ctx, provider, source.config.ClientID, fresh, false); err != nil {
				return "", err
			}
			if fresh.RefreshToken == "" {
				fresh.RefreshToken = token.RefreshToken
			}
			source.config.Token = fresh
			if err := source.save(); err != nil {
				return "", err
			}
			return fresh.AccessToken, nil
		}
		var failure *oauth2.RetrieveError
		if !errors.As(err, &failure) || failure.ErrorCode != "invalid_grant" {
			return "", fmt.Errorf("identity provider renewal failed; session retained, retry later")
		}
		fmt.Fprintln(source.stderr, "Your CLI session was revoked or expired; sign in again.")
	}
	source.config.Token = nil
	if err := source.save(); err != nil {
		return "", err
	}
	device, err := config.DeviceAuth(ctx)
	if err != nil {
		return "", fmt.Errorf("could not start device sign-in (check provider/client device-flow configuration)")
	}
	link := device.VerificationURIComplete
	if link == "" {
		link = device.VerificationURI
	}
	if err := validateURL(link); err != nil {
		// Device verification links may include a user-code query.
		parsed, parseErr := url.Parse(link)
		if parseErr != nil {
			return "", fmt.Errorf("invalid device sign-in URL")
		}
		parsed.RawQuery = ""
		if validateURL(parsed.String()) != nil {
			return "", fmt.Errorf("unsafe device sign-in URL")
		}
	}
	fmt.Fprintf(source.stderr, "Sign in at %s\nEnter code: %s\n", link, device.UserCode)
	if !source.noBrowser {
		if err := openBrowser(ctx, link); err != nil {
			fmt.Fprintln(source.stderr, "Could not open browser; open the sign-in URL manually.")
		}
	}
	fresh, err := config.DeviceAccessToken(ctx, device)
	if err != nil {
		return "", fmt.Errorf("device sign-in did not complete; retry zephyr auth login")
	}
	if err := validToken(ctx, provider, source.config.ClientID, fresh, true); err != nil {
		return "", err
	}
	source.config.Token, source.config.Until = fresh, time.Now().Add(8*time.Hour)
	if err := source.save(); err != nil {
		return "", err
	}
	fmt.Fprintln(source.stderr, "CLI sign-in complete.")
	return fresh.AccessToken, nil
}

func validToken(ctx context.Context, provider *oidc.Provider, clientID string, token *oauth2.Token, requireIdentity bool) error {
	if token == nil || token.AccessToken == "" || token.Expiry.IsZero() || !token.Expiry.After(time.Now()) {
		return fmt.Errorf("provider returned a token without a valid access-token expiry")
	}
	raw, _ := token.Extra("id_token").(string)
	if raw == "" && requireIdentity {
		return fmt.Errorf("provider did not return an OIDC identity token")
	}
	if raw != "" {
		if _, err := provider.Verifier(&oidc.Config{ClientID: clientID}).Verify(ctx, raw); err != nil {
			return fmt.Errorf("provider returned an invalid identity token")
		}
	}
	return nil
}

func openBrowser(ctx context.Context, link string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.CommandContext(ctx, "open", link).Run()
	case "windows":
		return exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", link).Run()
	default:
		return exec.CommandContext(ctx, "xdg-open", link).Run()
	}
}

func runAuth(ctx context.Context, command string, opts options, out, stderr io.Writer) error {
	if command != "login" && command != "logout" && command != "status" {
		return fmt.Errorf("unknown auth command %q", command)
	}
	transport, err := httpClient(opts.caFile)
	if err != nil {
		return err
	}
	source, endpoint, err := newSessionSource(opts, transport, stderr)
	if err != nil {
		return err
	}
	switch command {
	case "login":
		if _, err := source.getToken(ctx, true); err != nil {
			return err
		}
		return printJSON(out, map[string]any{"authenticated": true, "endpoint": endpoint, "session_expires_at": source.config.Until})
	case "status":
		token := source.config.Token
		return printJSON(out, map[string]any{"endpoint": endpoint, "issuer": source.config.Issuer, "session_present": token != nil, "session_expires_at": source.config.Until, "access_token_expired": token == nil || !token.Expiry.After(time.Now())})
	case "logout":
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		ctx = context.WithValue(ctx, oauth2.HTTPClient, source.http)
		unlock, err := source.lock(ctx)
		if err != nil {
			return err
		}
		defer unlock()
		stored, err := readCredentials(source.path)
		if err != nil {
			return err
		}
		if stored.Token != nil {
			if stored.Endpoint != source.config.Endpoint || stored.Issuer != source.config.Issuer || stored.ClientID != source.config.ClientID {
				return fmt.Errorf("logout configuration differs from saved session; omit overrides to log out that session")
			}
			provider, _, err := source.provider(ctx)
			if err != nil {
				return err
			}
			var metadata struct {
				RevocationURL string `json:"revocation_endpoint"`
			}
			if err := provider.Claims(&metadata); err != nil {
				return err
			}
			if metadata.RevocationURL != "" {
				if err := validateURL(metadata.RevocationURL); err != nil {
					return err
				}
				value, hint := stored.Token.RefreshToken, "refresh_token"
				if value == "" {
					value, hint = stored.Token.AccessToken, "access_token"
				}
				form := url.Values{"client_id": {stored.ClientID}, "token": {value}, "token_type_hint": {hint}}
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, metadata.RevocationURL, strings.NewReader(form.Encode()))
				if err != nil {
					return err
				}
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				response, err := source.http.Do(request)
				if err != nil {
					return fmt.Errorf("provider logout unavailable; local session retained so revocation can be retried")
				}
				response.Body.Close()
				if response.StatusCode != 200 {
					return fmt.Errorf("provider revocation returned %d; local session retained", response.StatusCode)
				}
			} else {
				fmt.Fprintln(stderr, "Provider does not advertise revocation; deleting only the local CLI session.")
			}
		}
		if err := os.Remove(source.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return printJSON(out, map[string]bool{"logged_out": true})
	}
	return nil
}
