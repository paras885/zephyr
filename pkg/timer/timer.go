package timer

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type Deadline struct {
	ID      string
	At      time.Time
	Payload any
}

type timerEntry struct {
	deadline Deadline
	index    int
}

type deadlineHeap []*timerEntry

func (deadlines deadlineHeap) Len() int { return len(deadlines) }
func (deadlines deadlineHeap) Less(left, right int) bool {
	return deadlines[left].deadline.At.Before(deadlines[right].deadline.At)
}
func (deadlines deadlineHeap) Swap(left, right int) {
	deadlines[left], deadlines[right] = deadlines[right], deadlines[left]
	deadlines[left].index = left
	deadlines[right].index = right
}
func (deadlines *deadlineHeap) Push(value any) {
	entry := value.(*timerEntry)
	entry.index = len(*deadlines)
	*deadlines = append(*deadlines, entry)
}
func (deadlines *deadlineHeap) Pop() any {
	items := *deadlines
	last := len(items) - 1
	entry := items[last]
	items[last] = nil
	entry.index = -1
	*deadlines = items[:last]
	return entry
}

type Service struct {
	mu             sync.Mutex
	deadlines      deadlineHeap
	entries        map[string]*timerEntry
	subscribers    map[uint64]*subscription
	nextSubscriber uint64
	wake           chan struct{}
	done           chan struct{}
	fired          chan Deadline
	closed         bool
	close          sync.Once
}

type subscription struct {
	events chan Deadline
	done   chan struct{}
	close  sync.Once
}

func NewService(buffer int) *Service {
	if buffer < 1 {
		buffer = 1
	}
	service := &Service{
		entries:     make(map[string]*timerEntry),
		subscribers: make(map[uint64]*subscription),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
		fired:       make(chan Deadline, buffer),
	}
	heap.Init(&service.deadlines)
	go service.run()
	return service
}

func (service *Service) Schedule(deadline Deadline) error {
	if deadline.ID == "" {
		return fmt.Errorf("deadline ID is required")
	}
	if deadline.At.IsZero() {
		return fmt.Errorf("deadline time is required")
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return fmt.Errorf("timer service is closed")
	}
	if entry, ok := service.entries[deadline.ID]; ok {
		entry.deadline = deadline
		heap.Fix(&service.deadlines, entry.index)
	} else {
		entry := &timerEntry{deadline: deadline}
		service.entries[deadline.ID] = entry
		heap.Push(&service.deadlines, entry)
	}
	service.mu.Unlock()
	service.signal()
	return nil
}

func (service *Service) Cancel(id string) bool {
	service.mu.Lock()
	entry, ok := service.entries[id]
	if ok {
		heap.Remove(&service.deadlines, entry.index)
		delete(service.entries, id)
	}
	service.mu.Unlock()
	if ok {
		service.signal()
	}
	return ok
}

func (service *Service) Events() <-chan Deadline {
	return service.fired
}

func (service *Service) Subscribe(buffer int) (<-chan Deadline, func()) {
	if buffer < 1 {
		buffer = 1
	}
	sub := &subscription{events: make(chan Deadline, buffer), done: make(chan struct{})}
	service.mu.Lock()
	if service.closed {
		sub.close.Do(func() { close(sub.done) })
		service.mu.Unlock()
		return sub.events, func() {}
	}
	service.nextSubscriber++
	id := service.nextSubscriber
	service.subscribers[id] = sub
	service.mu.Unlock()
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			service.mu.Lock()
			delete(service.subscribers, id)
			service.mu.Unlock()
			sub.close.Do(func() { close(sub.done) })
		})
	}
	return sub.events, unsubscribe
}

func (service *Service) Close() {
	service.close.Do(func() {
		service.mu.Lock()
		service.closed = true
		subscribers := make([]*subscription, 0, len(service.subscribers))
		for _, sub := range service.subscribers {
			subscribers = append(subscribers, sub)
		}
		service.subscribers = make(map[uint64]*subscription)
		service.mu.Unlock()
		close(service.done)
		for _, sub := range subscribers {
			sub.close.Do(func() { close(sub.done) })
		}
	})
}

func (service *Service) signal() {
	select {
	case service.wake <- struct{}{}:
	default:
	}
}

func (service *Service) run() {
	defer close(service.fired)
	for {
		wait, ok := service.nextWait()
		if !ok {
			select {
			case <-service.wake:
				continue
			case <-service.done:
				return
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			service.fireDue()
		case <-service.wake:
			stopTimer(timer)
		case <-service.done:
			stopTimer(timer)
			return
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (service *Service) nextWait() (time.Duration, bool) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.deadlines) == 0 {
		return 0, false
	}
	wait := time.Until(service.deadlines[0].deadline.At)
	if wait < 0 {
		return 0, true
	}
	return wait, true
}

func (service *Service) fireDue() {
	for {
		service.mu.Lock()
		if len(service.deadlines) == 0 || service.deadlines[0].deadline.At.After(time.Now()) {
			service.mu.Unlock()
			return
		}
		entry := heap.Pop(&service.deadlines).(*timerEntry)
		delete(service.entries, entry.deadline.ID)
		deadline := entry.deadline
		subscribers := make([]*subscription, 0, len(service.subscribers))
		for _, sub := range service.subscribers {
			subscribers = append(subscribers, sub)
		}
		service.mu.Unlock()
		select {
		case service.fired <- deadline:
		default:
		}
		select {
		case <-service.done:
			return
		default:
		}
		for _, sub := range subscribers {
			select {
			case sub.events <- deadline:
			case <-sub.done:
			case <-service.done:
				return
			}
		}
	}
}
