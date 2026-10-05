package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/token"
)

const (
	EndpointEnv = "ZEPHYR_ENDPOINT"
	TokenEnv    = "ZEPHYR_TOKEN"
	TimeoutEnv  = "ZEPHYR_TIMEOUT"
)

var ErrEndpointRequired = errors.New("Zephyr endpoint is required")

type Config struct {
	Endpoint    string
	Token       string
	TokenSource token.Source
	Timeout     time.Duration
	HTTPClient  *http.Client
}

type WorkflowRun struct {
	ID     string         `json:"id"`
	Status string         `json:"status"`
	Result map[string]any `json:"result,omitempty"`
}

type Client struct {
	endpoint    string
	token       string
	tokenSource token.Source
	httpClient  *http.Client
}

type APIError struct {
	StatusCode int
	Message    string
}

func (err *APIError) Error() string {
	return fmt.Sprintf("Zephyr API returned %d: %s", err.StatusCode, err.Message)
}

func ConfigFromEnv() (Config, error) {
	config := Config{
		Endpoint: strings.TrimSpace(os.Getenv(EndpointEnv)),
		Token:    os.Getenv(TokenEnv),
		Timeout:  10 * time.Second,
	}
	if config.Endpoint == "" {
		return Config{}, ErrEndpointRequired
	}
	if timeout := strings.TrimSpace(os.Getenv(TimeoutEnv)); timeout != "" {
		parsed, err := time.ParseDuration(timeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", TimeoutEnv, err)
		}
		config.Timeout = parsed
	}
	return config, nil
}

func New(config Config) (*Client, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	if endpoint == "" {
		return nil, ErrEndpointRequired
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid Zephyr endpoint %q: expected an http or https URL", config.Endpoint)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid Zephyr endpoint %q: query and fragment are not allowed", config.Endpoint)
	}
	if config.Timeout <= 0 && config.HTTPClient == nil {
		return nil, fmt.Errorf("HTTP timeout must be positive")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: config.Timeout}
	}
	if config.Token != "" && config.TokenSource != nil {
		return nil, fmt.Errorf("configure either a static token or a token source, not both")
	}
	return &Client{endpoint: endpoint, token: config.Token, tokenSource: config.TokenSource, httpClient: httpClient}, nil
}

func (client *Client) StartWorkflow(ctx context.Context, name string, version int, workflowContext any) (WorkflowRun, error) {
	return client.startWorkflow(ctx, name, version, workflowContext, "")
}

func (client *Client) StartWorkflowWithIdempotencyKey(ctx context.Context, name string, version int, workflowContext any, idempotencyKey string) (WorkflowRun, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return WorkflowRun{}, fmt.Errorf("idempotency key is required")
	}
	return client.startWorkflow(ctx, name, version, workflowContext, idempotencyKey)
}

func (client *Client) startWorkflow(ctx context.Context, name string, version int, workflowContext any, idempotencyKey string) (WorkflowRun, error) {
	if name == "" {
		return WorkflowRun{}, fmt.Errorf("workflow name is required")
	}
	if version < 0 {
		return WorkflowRun{}, fmt.Errorf("workflow version cannot be negative")
	}
	body, err := json.Marshal(struct {
		Version int `json:"version"`
		Context any `json:"context"`
	}{Version: version, Context: workflowContext})
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("encode workflow input: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint+"/v1/workflows/"+url.PathEscape(name)+"/instances", bytes.NewReader(body))
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("create workflow request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if err := client.authorize(request); err != nil {
		return WorkflowRun{}, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("start workflow %q: %w", name, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(response.StatusCode)
		}
		return WorkflowRun{}, &APIError{StatusCode: response.StatusCode, Message: failure.Error}
	}
	var run WorkflowRun
	if err := json.NewDecoder(response.Body).Decode(&run); err != nil {
		return WorkflowRun{}, fmt.Errorf("decode workflow response: %w", err)
	}
	if run.ID == "" {
		return WorkflowRun{}, fmt.Errorf("decode workflow response: workflow ID is missing")
	}
	return run, nil
}

func (client *Client) GetWorkflowRun(ctx context.Context, workflowID string) (WorkflowRun, error) {
	if strings.TrimSpace(workflowID) == "" {
		return WorkflowRun{}, fmt.Errorf("workflow ID is required")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint+"/v1/instances/"+url.PathEscape(workflowID), nil)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("create workflow result request: %w", err)
	}
	if err := client.authorize(request); err != nil {
		return WorkflowRun{}, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("get workflow run %q: %w", workflowID, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return WorkflowRun{}, decodeAPIError(response)
	}
	var run WorkflowRun
	if err := json.NewDecoder(response.Body).Decode(&run); err != nil {
		return WorkflowRun{}, fmt.Errorf("decode workflow run: %w", err)
	}
	if run.ID == "" {
		return WorkflowRun{}, fmt.Errorf("decode workflow run: workflow ID is missing")
	}
	return run, nil
}

func (client *Client) authorize(request *http.Request) error {
	token := client.token
	if client.tokenSource != nil {
		var err error
		token, err = client.tokenSource.Token(request.Context())
		if err != nil {
			return fmt.Errorf("get Zephyr API access token: %w", err)
		}
		if strings.TrimSpace(token) == "" {
			return fmt.Errorf("Zephyr API token source returned an empty token")
		}
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

func (client *Client) WaitWorkflow(ctx context.Context, workflowID string, pollInterval time.Duration) (WorkflowRun, error) {
	if pollInterval <= 0 {
		return WorkflowRun{}, fmt.Errorf("workflow poll interval must be positive")
	}
	for {
		run, err := client.GetWorkflowRun(ctx, workflowID)
		if err != nil {
			return WorkflowRun{}, err
		}
		switch run.Status {
		case "COMPLETED", "FAILED", "COMPENSATED":
			return run, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return WorkflowRun{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func decodeAPIError(response *http.Response) error {
	var failure struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure)
	if failure.Error == "" {
		failure.Error = http.StatusText(response.StatusCode)
	}
	return &APIError{StatusCode: response.StatusCode, Message: failure.Error}
}
