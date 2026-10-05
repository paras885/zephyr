package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
)

type heartbeatTransport struct {
	mu         sync.Mutex
	heartbeats []gateway.TaskHeartbeat
	calls      chan struct{}
	err        error
}

func (transport *heartbeatTransport) Receive(context.Context, gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error) {
	return gateway.WorkDelivery{}, nil
}

func (transport *heartbeatTransport) Heartbeat(ctx context.Context, heartbeat gateway.TaskHeartbeat) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	transport.mu.Lock()
	transport.heartbeats = append(transport.heartbeats, heartbeat)
	transport.mu.Unlock()
	if transport.calls != nil {
		transport.calls <- struct{}{}
	}
	return transport.err
}

func (transport *heartbeatTransport) Complete(context.Context, gateway.TaskCompletion) error {
	return nil
}

func (transport *heartbeatTransport) Fail(context.Context, gateway.TaskFailure) error {
	return nil
}

func (transport *heartbeatTransport) count() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return len(transport.heartbeats)
}

func testDelivery() gateway.WorkDelivery {
	return gateway.WorkDelivery{LeaseID: "lease-1", LeaseToken: 7}
}

func TestHeartbeatLoopRenewsAtConfiguredCadence(t *testing.T) {
	transport := &heartbeatTransport{calls: make(chan struct{}, 3)}
	client, err := NewClient(transport, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	loop, err := client.StartHeartbeat(context.Background(), testDelivery(), 10*time.Millisecond, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case <-transport.calls:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("heartbeat was not sent at the configured cadence")
		}
	}
	if err := loop.Stop(); err != nil {
		t.Fatal(err)
	}
	if transport.count() != 3 {
		t.Fatalf("heartbeat count = %d, want 3", transport.count())
	}

	transport.mu.Lock()
	heartbeat := transport.heartbeats[0]
	transport.mu.Unlock()
	if heartbeat.LeaseID != "lease-1" || heartbeat.LeaseToken != 7 || heartbeat.LeaseDurationMS != 2000 {
		t.Fatalf("heartbeat = %#v", heartbeat)
	}
}

func TestHeartbeatLoopStopsOnContextCancellation(t *testing.T) {
	transport := &heartbeatTransport{}
	client, err := NewClient(transport, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	loop, err := client.StartHeartbeat(ctx, testDelivery(), time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := loop.Wait(); err != nil {
		t.Fatalf("Wait after cancellation = %v, want nil", err)
	}
}

func TestHeartbeatLoopExplicitStopIsIdempotentAndDoesNotRenewAgain(t *testing.T) {
	transport := &heartbeatTransport{}
	client, err := NewClient(transport, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	loop, err := client.StartHeartbeat(context.Background(), testDelivery(), time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := loop.Stop(); err != nil {
		t.Fatal(err)
	}
	if transport.count() != 0 {
		t.Fatalf("heartbeat count = %d, want 0", transport.count())
	}
}

func TestHeartbeatLoopPropagatesFirstHeartbeatError(t *testing.T) {
	staleErr := errors.New("stale lease token")
	transport := &heartbeatTransport{err: staleErr, calls: make(chan struct{}, 1)}
	client, err := NewClient(transport, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	loop, err := client.StartHeartbeat(context.Background(), testDelivery(), time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.calls:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("heartbeat was not sent")
	}
	if err := loop.Wait(); !errors.Is(err, staleErr) {
		t.Fatalf("Wait error = %v, want %v", err, staleErr)
	}
	if !errors.Is(loop.Err(), staleErr) {
		t.Fatalf("Err = %v, want %v", loop.Err(), staleErr)
	}
	if transport.count() != 1 {
		t.Fatalf("heartbeat count = %d, want 1", transport.count())
	}
}
