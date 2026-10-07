package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
)

func TestSQLiteRegistryAndSessionsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "registry.db")
	first, err := OpenSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	source := `type Input { value: string } type Output { status: string }
workflow Live(input: Input) -> Output { return Output { status: input.value }; }`
	definition, err := compiler.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	record := RegisteredWorkflow{Definition: definition, Source: source}
	if err := first.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := first.PutWorkflow(ctx, record); err != nil {
		t.Fatal("identical registration should be idempotent:", err)
	}
	changed := record
	changed.Source += "\n"
	if err := first.PutWorkflow(ctx, changed); !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("changed source error = %v", err)
	}
	if err := first.CreateSession(ctx, "session", identity.Session{Data: "encrypted-token", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	records, err := second.ListDefinitions(ctx)
	if err != nil || len(records) != 1 || records[0].Source != source {
		t.Fatalf("records = %+v, %v", records, err)
	}
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := second.UpdateSession(ctx, "session", func(session *identity.Session) error {
				session.Data += "!"
				return nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := second.UpdateSession(ctx, "session", func(session *identity.Session) error {
		if session.Data != "encrypted-token!!!!!" {
			t.Errorf("lost session update: %s", session.Data)
		}
		return errors.New("rollback")
	}); err == nil {
		t.Fatal("expected callback error")
	}
	if err := second.DeleteSession(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	if err := second.UpdateSession(ctx, "session", func(*identity.Session) error { return nil }); !errors.Is(err, identity.ErrSessionNotFound) {
		t.Fatalf("deleted session = %v", err)
	}
}
