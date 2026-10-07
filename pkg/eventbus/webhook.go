package eventbus

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

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type WebhookConfig struct {
	Endpoint    string
	Token       string
	HTTPClient  *http.Client
	MaxAttempts int
	RetryDelay  time.Duration
}

type Webhook struct {
	endpoint    string
	token       string
	client      *http.Client
	maxAttempts int
	retryDelay  time.Duration
}

type WebhookError struct {
	StatusCode int
	Message    string
}

func (err *WebhookError) Error() string {
	return fmt.Sprintf("event webhook returned HTTP %d: %s", err.StatusCode, err.Message)
}

func NewWebhook(config WebhookConfig) (*Webhook, error) {
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("valid HTTP webhook endpoint is required")
	}
	if config.MaxAttempts < 0 || config.RetryDelay < 0 {
		return nil, fmt.Errorf("webhook retry settings cannot be negative")
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 50 * time.Millisecond
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Webhook{
		endpoint:    config.Endpoint,
		token:       config.Token,
		client:      config.HTTPClient,
		maxAttempts: config.MaxAttempts,
		retryDelay:  config.RetryDelay,
	}, nil
}

func (webhook *Webhook) Publish(ctx context.Context, event domain.Event) error {
	if ctx == nil {
		return fmt.Errorf("publish context is required")
	}
	if event.WorkflowID == "" || event.Sequence == 0 {
		return fmt.Errorf("workflow ID and persisted event sequence are required")
	}
	endpoint, _ := url.Parse(webhook.endpoint)
	ctx, span := otel.Tracer("zephyr/eventbus").Start(ctx, "webhook.deliver", trace.WithAttributes(
		attribute.String("workflow.id", event.WorkflowID), attribute.Int64("event.sequence", int64(event.Sequence)),
		attribute.String("event.type", string(event.Type)), attribute.String("server.address", endpoint.Hostname()),
	))
	defer span.End()
	body, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "encode webhook event failed")
		return fmt.Errorf("encode workflow event: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt < webhook.maxAttempts; attempt++ {
		done, err := webhook.deliverAttempt(ctx, span, body, event)
		if done {
			return err
		}
		lastErr = err
		span.AddEvent("webhook.attempt", trace.WithAttributes(attribute.Int("attempt", attempt+1)))
		if attempt+1 == webhook.maxAttempts {
			break
		}
		if err := webhook.waitBeforeRetry(ctx); err != nil {
			return err
		}
	}
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "webhook delivery failed")
	return fmt.Errorf("event webhook delivery failed after %d attempts: %w", webhook.maxAttempts, lastErr)
}

// deliverAttempt performs one delivery attempt. done is true when the retry loop should stop
// immediately, returning err as the final Publish result (nil on success).
func (webhook *Webhook) deliverAttempt(ctx context.Context, span trace.Span, body []byte, event domain.Event) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook.endpoint, bytes.NewReader(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "create webhook request failed")
		return true, fmt.Errorf("create event webhook request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", event.WorkflowID+":"+strconv.FormatUint(event.Sequence, 10))
	if webhook.token != "" {
		request.Header.Set("Authorization", "Bearer "+webhook.token)
	}
	response, requestErr := webhook.client.Do(request)
	if requestErr != nil {
		return false, requestErr
	}
	message, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	_ = response.Body.Close()
	if readErr != nil {
		return false, fmt.Errorf("read event webhook response: %w", readErr)
	}
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return true, nil
	}
	webhookErr := &WebhookError{StatusCode: response.StatusCode, Message: string(bytes.TrimSpace(message))}
	if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError && response.StatusCode != http.StatusTooManyRequests {
		return true, webhookErr
	}
	return false, webhookErr
}

func (webhook *Webhook) waitBeforeRetry(ctx context.Context) error {
	timer := time.NewTimer(webhook.retryDelay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ Publisher = (*Webhook)(nil)
