package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type WorkflowRegistration struct {
	Name    string            `json:"name"`
	Version int               `json:"version"`
	Files   map[string]string `json:"files"`
}

type WorkflowRunDetails struct {
	WorkflowRun
	WorkflowName  string            `json:"workflow_name"`
	Version       int               `json:"version"`
	FailureReason string            `json:"failure_reason,omitempty"`
	Context       map[string]any    `json:"context"`
	Tasks         []json.RawMessage `json:"tasks"`
	Events        []json.RawMessage `json:"events"`
	StartedAt     time.Time         `json:"started_at,omitempty"`
	UpdatedAt     time.Time         `json:"updated_at,omitempty"`
	NextSequence  uint64            `json:"next_sequence"`
}

type WorkflowRunSummary struct {
	ID             string    `json:"id"`
	WorkflowName   string    `json:"workflow_name"`
	Version        int       `json:"version"`
	Status         string    `json:"status"`
	TaskCount      int       `json:"task_count"`
	CompletedTasks int       `json:"completed_tasks"`
	FailedTasks    int       `json:"failed_tasks"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type WorkflowRunPage struct {
	Items  []WorkflowRunSummary `json:"items"`
	Total  int                  `json:"total"`
	Limit  int                  `json:"limit"`
	Offset int                  `json:"offset"`
}

func (client *Client) RegisterWorkflow(ctx context.Context, source string, version int) (WorkflowRegistration, error) {
	var output WorkflowRegistration
	if source == "" || version < 1 {
		return output, fmt.Errorf("workflow source and positive version are required")
	}
	err := client.workflowRequest(ctx, http.MethodPost, "/v1/workflows/register", struct {
		Source  string `json:"source"`
		Version int    `json:"version"`
	}{source, version}, &output)
	if err == nil && (output.Name == "" || output.Version != version || len(output.Files) == 0) {
		err = fmt.Errorf("registration response lacks valid workflow artifacts")
	}
	return output, err
}

func (client *Client) WorkflowRunDetails(ctx context.Context, id string) (WorkflowRunDetails, error) {
	var output WorkflowRunDetails
	if id == "" {
		return output, fmt.Errorf("workflow instance ID is required")
	}
	err := client.workflowRequest(ctx, http.MethodGet, "/v1/instances/"+url.PathEscape(id), nil, &output)
	if err == nil && output.ID == "" {
		err = fmt.Errorf("workflow status response lacks an instance ID")
	}
	return output, err
}

func (client *Client) ListWorkflowRuns(ctx context.Context, name string, limit, offset int, status string) (WorkflowRunPage, error) {
	var output WorkflowRunPage
	if name == "" || limit < 1 || limit > 100 || offset < 0 {
		return output, fmt.Errorf("workflow name, limit between 1 and 100, and non-negative offset are required")
	}
	query := url.Values{"limit": {strconv.Itoa(limit)}, "offset": {strconv.Itoa(offset)}}
	if status != "" {
		query.Set("status", status)
	}
	err := client.workflowRequest(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(name)+"/instances?"+query.Encode(), nil, &output)
	return output, err
}

func (client *Client) workflowRequest(ctx context.Context, method, path string, input, output any) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode workflow request: %w", err)
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if err := client.authorize(request); err != nil {
		return err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("workflow API request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return decodeAPIError(response)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(output); err != nil {
		return fmt.Errorf("decode workflow API response: %w", err)
	}
	return nil
}
