package generated

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	zephyrclient "github.com/zephyr-workflow/zephyr/pkg/client"
)

type CheckoutClient struct {
	client *zephyrclient.Client
}

func NewCheckoutClient(config zephyrclient.Config) (*CheckoutClient, error) {
	client, err := zephyrclient.New(config)
	if err != nil {
		return nil, err
	}
	return &CheckoutClient{client: client}, nil
}

func NewCheckoutClientFromEnv() (*CheckoutClient, error) {
	config, err := zephyrclient.ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return NewCheckoutClient(config)
}

func (client *CheckoutClient) StartCheckout(ctx context.Context, input CheckoutInput) (string, error) {
	run, err := client.client.StartWorkflow(ctx, "Checkout", 1, input)
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

func (client *CheckoutClient) StartCheckoutWithIdempotencyKey(ctx context.Context, input CheckoutInput, key string) (string, error) {
	run, err := client.client.StartWorkflowWithIdempotencyKey(ctx, "Checkout", 1, input, key)
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

func (client *CheckoutClient) GetCheckoutResult(ctx context.Context, workflowID string) (TaskOutput, error) {
	run, err := client.client.GetWorkflowRun(ctx, workflowID)
	if err != nil {
		return TaskOutput{}, err
	}
	return client.decodeCheckoutResult(run)
}

func (client *CheckoutClient) WaitForCheckout(ctx context.Context, workflowID string, pollInterval time.Duration) (TaskOutput, error) {
	run, err := client.client.WaitWorkflow(ctx, workflowID, pollInterval)
	if err != nil {
		return TaskOutput{}, err
	}
	return client.decodeCheckoutResult(run)
}

func (client *CheckoutClient) decodeCheckoutResult(run zephyrclient.WorkflowRun) (TaskOutput, error) {
	if run.Status != "COMPLETED" {
		return TaskOutput{}, fmt.Errorf("workflow %s finished with status %s", run.ID, run.Status)
	}
	encoded, err := json.Marshal(run.Result)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("encode workflow result: %w", err)
	}
	var result TaskOutput
	if err := json.Unmarshal(encoded, &result); err != nil {
		return TaskOutput{}, fmt.Errorf("decode workflow result: %w", err)
	}
	return result, nil
}
