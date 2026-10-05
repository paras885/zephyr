package timer

import (
	"testing"
	"time"
)

func TestServiceFiresEarliestDeadline(t *testing.T) {
	service := NewService(4)
	defer service.Close()
	if err := service.Schedule(Deadline{ID: "later", At: time.Now().Add(80 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err := service.Schedule(Deadline{ID: "earlier", At: time.Now().Add(10 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	select {
	case deadline := <-service.Events():
		if deadline.ID != "earlier" {
			t.Fatalf("deadline ID = %q, want earlier", deadline.ID)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
}

func TestServiceReschedulesExistingDeadline(t *testing.T) {
	service := NewService(2)
	defer service.Close()
	if err := service.Schedule(Deadline{ID: "lease", At: time.Now().Add(200 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err := service.Schedule(Deadline{ID: "lease", At: time.Now().Add(10 * time.Millisecond), Payload: "updated"}); err != nil {
		t.Fatal(err)
	}
	select {
	case deadline := <-service.Events():
		if deadline.ID != "lease" || deadline.Payload != "updated" {
			t.Fatalf("deadline = %#v, want updated lease", deadline)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("rescheduled timer did not fire")
	}
}

func TestServiceCancelPreventsDeadline(t *testing.T) {
	service := NewService(1)
	defer service.Close()
	if err := service.Schedule(Deadline{ID: "cancelled", At: time.Now().Add(10 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if !service.Cancel("cancelled") {
		t.Fatal("Cancel returned false for scheduled deadline")
	}
	select {
	case deadline := <-service.Events():
		t.Fatalf("cancelled deadline fired: %#v", deadline)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestServiceCloseStopsScheduling(t *testing.T) {
	service := NewService(1)
	service.Close()
	if err := service.Schedule(Deadline{ID: "closed", At: time.Now()}); err == nil {
		t.Fatal("Schedule accepted a closed timer service")
	}
	if _, open := <-service.Events(); open {
		t.Fatal("Events channel remained open after Close")
	}
}

func TestServiceBroadcastsDeadlineToIndependentSubscribers(t *testing.T) {
	service := NewService(1)
	defer service.Close()
	first, closeFirst := service.Subscribe(1)
	defer closeFirst()
	second, closeSecond := service.Subscribe(1)
	defer closeSecond()
	deadline := Deadline{ID: "shared-deadline", At: time.Now().Add(10 * time.Millisecond), Payload: "broadcast"}
	if err := service.Schedule(deadline); err != nil {
		t.Fatal(err)
	}
	for label, events := range map[string]<-chan Deadline{"first": first, "second": second} {
		select {
		case received := <-events:
			if received.ID != deadline.ID || received.Payload != deadline.Payload {
				t.Errorf("%s subscriber received %#v, want %#v", label, received, deadline)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s timer subscriber did not receive deadline", label)
		}
	}
}
