package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/token"
)

type HTTPTransport struct {
	baseURL     string
	client      *http.Client
	tokenSource token.Source
}

func NewHTTPTransport(baseURL string, client *http.Client) (*HTTPTransport, error) {
	return NewHTTPTransportWithTokenSource(baseURL, client, nil)
}

func NewHTTPTransportWithTokenSource(baseURL string, client *http.Client, tokenSource token.Source) (*HTTPTransport, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" {
		return nil, fmt.Errorf("gateway URL is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPTransport{baseURL: baseURL, client: client, tokenSource: tokenSource}, nil
}

func (transport *HTTPTransport) Receive(ctx context.Context, request gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error) {
	var delivery gateway.WorkDelivery
	err := transport.post(ctx, gateway.TaskReceivePath, request, &delivery)
	return delivery, err
}

func (transport *HTTPTransport) Heartbeat(ctx context.Context, heartbeat gateway.TaskHeartbeat) error {
	return transport.post(ctx, gateway.TaskHeartbeatPath, heartbeat, nil)
}

func (transport *HTTPTransport) Complete(ctx context.Context, completion gateway.TaskCompletion) error {
	return transport.post(ctx, gateway.TaskCompletePath, completion, nil)
}

func (transport *HTTPTransport) Fail(ctx context.Context, failure gateway.TaskFailure) error {
	return transport.post(ctx, gateway.TaskFailPath, failure, nil)
}

func (transport *HTTPTransport) post(ctx context.Context, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode gateway request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, transport.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create gateway request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if transport.tokenSource != nil {
		token, err := transport.tokenSource.Token(ctx)
		if err != nil {
			return fmt.Errorf("get worker API access token: %w", err)
		}
		if strings.TrimSpace(token) == "" {
			return fmt.Errorf("worker API token source returned an empty token")
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := transport.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return fmt.Errorf("gateway returned HTTP %d: %w", response.StatusCode, readErr)
		}
		var errorResponse struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(message, &errorResponse) == nil && errorResponse.Error != "" {
			return &HTTPError{StatusCode: response.StatusCode, Message: errorResponse.Error}
		}
		return &HTTPError{StatusCode: response.StatusCode, Message: strings.TrimSpace(string(message))}
	}
	if output == nil {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(output); err != nil {
		return fmt.Errorf("decode gateway response: %w", err)
	}
	return nil
}

type HTTPError struct {
	StatusCode int
	Message    string
}

func (err *HTTPError) Error() string {
	return fmt.Sprintf("gateway returned HTTP %d: %s", err.StatusCode, err.Message)
}
