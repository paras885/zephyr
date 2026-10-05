package store

import (
	"context"
	"errors"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

var (
	ErrNotFound            = errors.New("workflow not found")
	ErrAlreadyExists       = errors.New("workflow already exists")
	ErrConflict            = errors.New("workflow changed concurrently")
	ErrDefinitionConflict  = errors.New("workflow definition version is immutable")
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
)

type ExecutionStore interface {
	Create(instance *domain.WorkflowInstance) error
	Get(workflowID string) (*domain.WorkflowInstance, error)
	Append(workflowID string, event domain.Event) error
	AppendMany(workflowID string, events []domain.Event) error
}

type WorkflowLocker interface {
	WithWorkflowLock(ctx context.Context, workflowID string, operation func() error) error
}

type IdempotencyStore interface {
	CreateIdempotent(ctx context.Context, instance *domain.WorkflowInstance, key, requestHash string) (created bool, existingID string, err error)
}

type idempotencyIdentity struct {
	workflowName string
	version      int
	key          string
}

type idempotencyClaim struct {
	requestHash string
	workflowID  string
}

type OutboxRecord struct {
	Event      domain.Event
	ClaimToken string
}

type OutboxStore interface {
	ClaimOutbox(ctx context.Context, limit int, leaseDuration time.Duration) ([]OutboxRecord, error)
	MarkOutboxDelivered(ctx context.Context, record OutboxRecord) error
	RetryOutbox(ctx context.Context, record OutboxRecord, delay time.Duration) error
}

type TaskPublicationRecord struct {
	Item       queue.WorkItem
	ClaimToken string
}

type TaskPublicationStore interface {
	ClaimTaskPublications(ctx context.Context, limit int, leaseDuration time.Duration) ([]TaskPublicationRecord, error)
	MarkTaskPublished(ctx context.Context, record TaskPublicationRecord) error
	RetryTaskPublication(ctx context.Context, record TaskPublicationRecord, delay time.Duration) error
}

type EventHistoryStore interface {
	ListEvents(ctx context.Context, workflowID string, afterSequence uint64, limit int) ([]domain.Event, error)
}

type ExecutionFilter struct {
	WorkflowName string
	Status       domain.WorkflowStatus
	Limit        int
	Offset       int
}

type ExecutionQueryStore interface {
	ListExecutions(ctx context.Context, filter ExecutionFilter) ([]*domain.WorkflowInstance, int, error)
	CountExecutions(ctx context.Context) (map[domain.WorkflowStatus]int, error)
}
