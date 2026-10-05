package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

type testOutbox struct {
	records   []store.OutboxRecord
	delivered []store.OutboxRecord
	retried   []store.OutboxRecord
}

func (outbox *testOutbox) ClaimOutbox(_ context.Context, _ int, _ time.Duration) ([]store.OutboxRecord, error) {
	records := outbox.records
	outbox.records = nil
	return records, nil
}

func (outbox *testOutbox) MarkOutboxDelivered(_ context.Context, record store.OutboxRecord) error {
	outbox.delivered = append(outbox.delivered, record)
	return nil
}

func (outbox *testOutbox) RetryOutbox(_ context.Context, record store.OutboxRecord, _ time.Duration) error {
	outbox.retried = append(outbox.retried, record)
	return nil
}

type testPublisher struct {
	events []domain.Event
	err    error
}

func (publisher *testPublisher) Publish(_ context.Context, event domain.Event) error {
	publisher.events = append(publisher.events, event)
	return publisher.err
}

func TestOutboxDispatcherMarksOnlyPublishedEvents(t *testing.T) {
	event := domain.Event{WorkflowID: "workflow-1", Sequence: 3, Type: domain.EventTaskCompleted}
	record := store.OutboxRecord{Event: event, ClaimToken: "claim-1"}
	outbox := &testOutbox{records: []store.OutboxRecord{record}}
	publisher := &testPublisher{err: errors.New("endpoint unavailable")}
	dispatcher, err := NewOutboxDispatcher(outbox, publisher, OutboxDispatcherConfig{RetryDelay: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := dispatcher.DispatchOnce(context.Background())
	if delivered != 0 || err == nil {
		t.Fatalf("DispatchOnce() = %d, %v; want 0 and publication error", delivered, err)
	}
	if len(outbox.delivered) != 0 || len(outbox.retried) != 1 || outbox.retried[0].ClaimToken != "claim-1" {
		t.Fatalf("outbox delivered/retried = %#v/%#v", outbox.delivered, outbox.retried)
	}

	outbox.records = []store.OutboxRecord{record}
	publisher.err = nil
	delivered, err = dispatcher.DispatchOnce(context.Background())
	if err != nil || delivered != 1 {
		t.Fatalf("successful DispatchOnce() = %d, %v; want 1, nil", delivered, err)
	}
	if len(outbox.delivered) != 1 || len(publisher.events) != 2 {
		t.Fatalf("published/delivered events = %d/%d, want 2/1", len(publisher.events), len(outbox.delivered))
	}
}
