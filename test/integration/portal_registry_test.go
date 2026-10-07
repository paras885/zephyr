package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestPostgresSharedRegistryAndSessionSerialization(t *testing.T) {
	ctx, executions, db := newAtomicPostgres(t)
	other, err := store.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}
	source := `type Input { value: string } type Output { status: string }
workflow Registered(input: Input) -> Output { return Output { status: input.value }; }`
	definition, err := compiler.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	definition.Version = 2
	record := store.RegisteredWorkflow{Definition: definition, Source: source}
	if err := executions.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := other.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, gateway := newAtomicGateway(t, other)
	run, err := gateway.StartWorkflow(ctx, "Registered", 2, map[string]any{"value": "shared"})
	if err != nil || run.Result["status"] != "shared" {
		t.Fatalf("other replica did not execute registered definition: %+v %v", run, err)
	}
	if err := executions.CreateSession(ctx, "shared-session", identity.Session{Data: "encrypted", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance := executions
			if i%2 == 0 {
				instance = other
			}
			if err := instance.UpdateSession(ctx, "shared-session", func(session *identity.Session) error {
				session.Data += "!"
				return nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := other.UpdateSession(ctx, "shared-session", func(session *identity.Session) error {
		if session.Data != "encrypted!!!!!!!!!!" {
			t.Errorf("lost serialized session update: %s", session.Data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := executions.DeleteSession(context.Background(), "shared-session"); err != nil {
		t.Fatal(err)
	}
	if err := other.UpdateSession(ctx, "shared-session", func(*identity.Session) error { return nil }); !errors.Is(err, identity.ErrSessionNotFound) {
		t.Fatalf("revocation not visible to other replica: %v", err)
	}
}
