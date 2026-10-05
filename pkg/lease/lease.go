package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

var (
	ErrLeaseNotFound = errorString("lease not found")
	ErrLeaseExpired  = errorString("lease expired")
	ErrStaleToken    = errorString("stale lease token")
	ErrLeaseConflict = errorString("task already has an active lease")
	ErrClosed        = errorString("lease manager is closed")
)

type errorString string

func (err errorString) Error() string { return string(err) }

type Lease struct {
	ID         string
	DeliveryID string
	Item       queue.WorkItem
	Token      uint64
	ExpiresAt  time.Time
	State      string
}

type MutationStore interface {
	Get(workflowID string) (*domain.WorkflowInstance, error)
	Append(workflowID string, event domain.Event) error
	AppendMany(workflowID string, events []domain.Event) error
}

type Expiration struct {
	Lease Lease
}

type StateStore interface {
	Claim(ctx context.Context, candidate Lease, duration time.Duration) (Lease, error)
	Renew(ctx context.Context, leaseID string, token uint64, duration time.Duration) (Lease, error)
	CompleteWith(ctx context.Context, leaseID string, token uint64, operation func(Lease, MutationStore) error) (Lease, error)
	ExpireWith(ctx context.Context, leaseID string, token uint64, operation func(Lease, MutationStore) error) (Lease, bool, error)
	ListExpired(ctx context.Context, limit int) ([]Lease, error)
}

type Manager struct {
	queue             queue.QueueDAO
	timers            *timer.Service
	timerEvents       <-chan timer.Deadline
	unsubscribe       func()
	generator         queue.FencingTokenGenerator
	stateStore        StateStore
	expirationHandler func(context.Context, Expiration, MutationStore) error
	mu                sync.Mutex
	leases            map[string]Lease
	expired           map[string]uint64
	expirations       chan Expiration
	done              chan struct{}
	close             sync.Once
}

func NewManager(workQueue queue.QueueDAO, timers *timer.Service, buffer int) (*Manager, error) {
	return NewManagerWithStateStore(workQueue, timers, buffer, nil)
}

func NewManagerWithStateStore(workQueue queue.QueueDAO, timers *timer.Service, buffer int, stateStore StateStore) (*Manager, error) {
	if workQueue == nil {
		return nil, fmt.Errorf("work queue is required")
	}
	if timers == nil {
		return nil, fmt.Errorf("timer service is required")
	}
	if buffer < 1 {
		buffer = 1
	}
	timerEvents, unsubscribe := timers.Subscribe(buffer)
	manager := &Manager{
		queue:       workQueue,
		timers:      timers,
		timerEvents: timerEvents,
		unsubscribe: unsubscribe,
		stateStore:  stateStore,
		leases:      make(map[string]Lease),
		expired:     make(map[string]uint64),
		expirations: make(chan Expiration, buffer),
		done:        make(chan struct{}),
	}
	go manager.run()
	return manager, nil
}

func (manager *Manager) Acquire(ctx context.Context, delivery queue.Delivery, duration time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if duration <= 0 {
		return Lease{}, fmt.Errorf("lease duration must be positive")
	}
	manager.mu.Lock()
	if manager.isClosed() {
		manager.mu.Unlock()
		return Lease{}, ErrClosed
	}
	lease := Lease{
		ID:         domain.NewID("lease"),
		DeliveryID: delivery.ID,
		Item:       delivery.Item,
		ExpiresAt:  time.Now().Add(duration),
	}
	if manager.stateStore != nil {
		var err error
		lease, err = manager.stateStore.Claim(ctx, lease, duration)
		if err != nil {
			manager.mu.Unlock()
			return Lease{}, err
		}
	} else {
		lease.Token = manager.generator.Next()
	}
	lease.Item.LeaseToken = lease.Token
	if err := manager.timers.Schedule(timer.Deadline{ID: lease.ID, At: lease.ExpiresAt}); err != nil {
		manager.mu.Unlock()
		return Lease{}, err
	}
	manager.leases[lease.ID] = lease
	manager.mu.Unlock()
	return lease, nil
}

func (manager *Manager) Heartbeat(ctx context.Context, leaseID string, token uint64, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	if manager.stateStore != nil {
		manager.mu.Lock()
		if manager.isClosed() {
			manager.mu.Unlock()
			return ErrClosed
		}
		manager.mu.Unlock()
		lease, err := manager.stateStore.Renew(ctx, leaseID, token, duration)
		if err != nil {
			return err
		}
		manager.mu.Lock()
		if localLease, ok := manager.leases[leaseID]; ok && localLease.Token == token {
			manager.leases[leaseID] = lease
			if err := manager.timers.Schedule(timer.Deadline{ID: lease.ID, At: lease.ExpiresAt}); err != nil {
				manager.mu.Unlock()
				return err
			}
		}
		manager.mu.Unlock()
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	lease, ok := manager.leases[leaseID]
	if !ok {
		if _, wasExpired := manager.expired[leaseID]; wasExpired {
			return ErrLeaseExpired
		}
		return ErrLeaseNotFound
	}
	if lease.Token != token {
		return ErrStaleToken
	}
	lease.ExpiresAt = time.Now().Add(duration)
	if err := manager.timers.Schedule(timer.Deadline{ID: lease.ID, At: lease.ExpiresAt}); err != nil {
		return err
	}
	manager.leases[leaseID] = lease
	return nil
}

func (manager *Manager) Complete(ctx context.Context, leaseID string, token uint64) error {
	return manager.CompleteWith(ctx, leaseID, token, nil)
}

func (manager *Manager) CompleteWith(ctx context.Context, leaseID string, token uint64, operation func(Lease, MutationStore) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if manager.stateStore != nil {
		if _, err := manager.stateStore.CompleteWith(ctx, leaseID, token, operation); err != nil {
			return err
		}
		manager.mu.Lock()
		delete(manager.leases, leaseID)
		manager.mu.Unlock()
		manager.timers.Cancel(leaseID)
		return nil
	}
	manager.mu.Lock()
	activeLease, ok := manager.leases[leaseID]
	if !ok {
		_, wasExpired := manager.expired[leaseID]
		manager.mu.Unlock()
		if wasExpired {
			return ErrLeaseExpired
		}
		return ErrLeaseNotFound
	}
	if activeLease.Token != token {
		manager.mu.Unlock()
		return ErrStaleToken
	}
	if operation != nil {
		if err := operation(activeLease, nil); err != nil {
			manager.mu.Unlock()
			return err
		}
	}
	delete(manager.leases, leaseID)
	manager.mu.Unlock()
	manager.timers.Cancel(leaseID)
	return manager.queue.Ack(ctx, activeLease.DeliveryID)
}

func (manager *Manager) Expirations() <-chan Expiration {
	return manager.expirations
}

func (manager *Manager) SetExpirationHandler(handler func(context.Context, Expiration, MutationStore) error) {
	manager.mu.Lock()
	manager.expirationHandler = handler
	manager.mu.Unlock()
}

func (manager *Manager) UsesSharedStateStore() bool {
	return manager.stateStore != nil
}

func (manager *Manager) ScanExpired(ctx context.Context, limit int) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("lease scan context is required")
	}
	if limit < 1 {
		return 0, fmt.Errorf("lease scan limit must be positive")
	}
	if manager.stateStore == nil {
		return 0, fmt.Errorf("lease scan requires a shared state store")
	}
	expired, err := manager.stateStore.ListExpired(ctx, limit)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, activeLease := range expired {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		if err := manager.expireShared(ctx, activeLease); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (manager *Manager) RunExpiryScanner(ctx context.Context, limit int, pollInterval time.Duration, onError func(error)) error {
	if ctx == nil {
		return fmt.Errorf("lease scanner context is required")
	}
	if limit < 1 || pollInterval <= 0 {
		return fmt.Errorf("lease scan limit and poll interval must be positive")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		_, err := manager.ScanExpired(ctx, limit)
		if err != nil && ctx.Err() == nil && onError != nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (manager *Manager) Close() {
	manager.close.Do(func() {
		close(manager.done)
		manager.unsubscribe()
	})
}

func (manager *Manager) run() {
	defer close(manager.expirations)
	for {
		select {
		case deadline, open := <-manager.timerEvents:
			if !open {
				return
			}
			manager.expire(deadline.ID)
		case <-manager.done:
			return
		}
	}
}

func (manager *Manager) expire(leaseID string) {
	manager.mu.Lock()
	lease, ok := manager.leases[leaseID]
	handler := manager.expirationHandler
	stateStore := manager.stateStore
	if stateStore != nil {
		manager.mu.Unlock()
		if !ok {
			return
		}
		if err := manager.expireShared(context.Background(), lease); err != nil {
			manager.reschedule(lease, 100*time.Millisecond)
		}
		return
	}
	if !ok {
		manager.mu.Unlock()
		return
	}
	delete(manager.leases, leaseID)
	manager.expired[leaseID] = lease.Token
	manager.mu.Unlock()
	if handler != nil {
		if err := handler(context.Background(), Expiration{Lease: lease}, nil); err == nil {
			_ = manager.queue.Ack(context.Background(), lease.DeliveryID)
		} else {
			_ = manager.queue.Reject(context.Background(), lease.DeliveryID, true)
		}
	} else {
		_ = manager.queue.Reject(context.Background(), lease.DeliveryID, true)
	}
	select {
	case manager.expirations <- Expiration{Lease: lease}:
	case <-manager.done:
	}
}

func (manager *Manager) expireShared(ctx context.Context, activeLease Lease) error {
	manager.mu.Lock()
	handler := manager.expirationHandler
	manager.mu.Unlock()
	updatedLease, expired, err := manager.stateStore.ExpireWith(ctx, activeLease.ID, activeLease.Token, func(lockedLease Lease, mutation MutationStore) error {
		if handler == nil {
			return nil
		}
		return handler(ctx, Expiration{Lease: lockedLease}, mutation)
	})
	if errors.Is(err, ErrLeaseExpired) || errors.Is(err, ErrLeaseNotFound) || errors.Is(err, ErrStaleToken) {
		manager.mu.Lock()
		if current, exists := manager.leases[activeLease.ID]; exists && current.Token == activeLease.Token {
			delete(manager.leases, activeLease.ID)
		}
		manager.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	if !expired {
		manager.mu.Lock()
		if current, exists := manager.leases[activeLease.ID]; exists && current.Token == activeLease.Token {
			manager.leases[activeLease.ID] = updatedLease
		}
		manager.mu.Unlock()
		return manager.timers.Schedule(timer.Deadline{ID: updatedLease.ID, At: updatedLease.ExpiresAt})
	}
	manager.mu.Lock()
	delete(manager.leases, activeLease.ID)
	manager.expired[activeLease.ID] = activeLease.Token
	manager.mu.Unlock()
	select {
	case manager.expirations <- Expiration{Lease: updatedLease}:
	case <-manager.done:
	}
	return nil
}

func (manager *Manager) reschedule(lease Lease, delay time.Duration) {
	lease.ExpiresAt = time.Now().Add(delay)
	manager.mu.Lock()
	if current, exists := manager.leases[lease.ID]; exists && current.Token == lease.Token {
		manager.leases[lease.ID] = lease
	}
	manager.mu.Unlock()
	_ = manager.timers.Schedule(timer.Deadline{ID: lease.ID, At: lease.ExpiresAt})
}

func (manager *Manager) isClosed() bool {
	select {
	case <-manager.done:
		return true
	default:
		return false
	}
}
