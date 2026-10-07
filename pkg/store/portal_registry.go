package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
)

type RegisteredWorkflow struct {
	Definition domain.WorkflowDef
	Source     string
}

type WorkflowRegistry interface {
	PutWorkflow(context.Context, RegisteredWorkflow) error
	ListDefinitions(context.Context) ([]RegisteredWorkflow, error)
	GetWorkflow(context.Context, string, int) (RegisteredWorkflow, error)
}

func applyPortalRegistryMigration(ctx context.Context, tx *sql.Tx, migration []byte) error {
	var applied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 7)`).Scan(&applied); err != nil {
		return fmt.Errorf("check portal/registry migration: %w", err)
	}
	if !applied {
		if _, err := tx.ExecContext(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply portal/registry migration: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (7)`); err != nil {
			return fmt.Errorf("record portal/registry migration: %w", err)
		}
	}
	return nil
}

type sharedCatalog struct {
	db       *sql.DB
	postgres bool
}

func (catalog sharedCatalog) bind(query string) string {
	if catalog.postgres {
		for index := 1; strings.Contains(query, "?"); index++ {
			query = strings.Replace(query, "?", fmt.Sprintf("$%d", index), 1)
		}
	}
	return query
}

func (catalog sharedCatalog) put(ctx context.Context, record RegisteredWorkflow) error {
	if err := record.Definition.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(record.Definition)
	if err != nil {
		return err
	}
	tx, err := catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, catalog.bind(`INSERT INTO workflow_definitions (workflow_name, version, definition, source)
		VALUES (?, ?, ?, NULLIF(?, '')) ON CONFLICT (workflow_name, version) DO NOTHING`), record.Definition.Name, record.Definition.Version, string(body), record.Source); err != nil {
		return fmt.Errorf("register workflow definition: %w", err)
	}
	query := catalog.bind(`SELECT definition, source FROM workflow_definitions WHERE workflow_name = ? AND version = ?`)
	if catalog.postgres {
		query += ` FOR UPDATE`
	}
	var existing []byte
	var source sql.NullString
	if err := tx.QueryRowContext(ctx, query, record.Definition.Name, record.Definition.Version).Scan(&existing, &source); err != nil {
		return err
	}
	var definition domain.WorkflowDef
	if err := json.Unmarshal(existing, &definition); err != nil {
		return err
	}
	if !reflect.DeepEqual(definition, record.Definition) || (source.Valid && record.Source != "" && source.String != record.Source) {
		return ErrDefinitionConflict
	}
	if !source.Valid && record.Source != "" {
		if _, err := tx.ExecContext(ctx, catalog.bind(`UPDATE workflow_definitions SET source = ? WHERE workflow_name = ? AND version = ?`), record.Source, record.Definition.Name, record.Definition.Version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (catalog sharedCatalog) list(ctx context.Context) ([]RegisteredWorkflow, error) {
	rows, err := catalog.db.QueryContext(ctx, `SELECT definition, COALESCE(source, '') FROM workflow_definitions ORDER BY workflow_name, version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]RegisteredWorkflow, 0)
	for rows.Next() {
		var record RegisteredWorkflow
		var body []byte
		if err := rows.Scan(&body, &record.Source); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &record.Definition); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (catalog sharedCatalog) get(ctx context.Context, name string, version int) (RegisteredWorkflow, error) {
	query := `SELECT definition, COALESCE(source, '') FROM workflow_definitions WHERE workflow_name = ?`
	args := []any{name}
	if version != 0 {
		query += ` AND version = ?`
		args = append(args, version)
	}
	query += ` ORDER BY version DESC LIMIT 1`
	var record RegisteredWorkflow
	var body []byte
	if err := catalog.db.QueryRowContext(ctx, catalog.bind(query), args...).Scan(&body, &record.Source); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return record, ErrNotFound
		}
		return record, err
	}
	err := json.Unmarshal(body, &record.Definition)
	return record, err
}

func (catalog sharedCatalog) createSession(ctx context.Context, id string, session identity.Session) error {
	tx, err := catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, catalog.bind(`DELETE FROM portal_sessions WHERE id IN (SELECT id FROM portal_sessions WHERE expires_at <= ? LIMIT 100)`), time.Now().Unix()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, catalog.bind(`INSERT INTO portal_sessions (id, data, expires_at) VALUES (?, ?, ?)`), id, session.Data, session.ExpiresAt.Unix()); err != nil {
		return err
	}
	return tx.Commit()
}

func (catalog sharedCatalog) updateSession(ctx context.Context, id string, operation func(*identity.Session) error) error {
	tx, err := catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := catalog.bind(`SELECT data, expires_at FROM portal_sessions WHERE id = ?`)
	if catalog.postgres {
		query += ` FOR UPDATE`
	}
	var session identity.Session
	var expiry int64
	if err := tx.QueryRowContext(ctx, query, id).Scan(&session.Data, &expiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return identity.ErrSessionNotFound
		}
		return err
	}
	session.ExpiresAt = time.Unix(expiry, 0)
	if !session.ExpiresAt.After(time.Now()) {
		return identity.ErrSessionNotFound
	}
	if err := operation(&session); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, catalog.bind(`UPDATE portal_sessions SET data = ? WHERE id = ?`), session.Data, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (catalog sharedCatalog) deleteSession(ctx context.Context, id string) error {
	_, err := catalog.db.ExecContext(ctx, catalog.bind(`DELETE FROM portal_sessions WHERE id = ?`), id)
	return err
}

func (store *PostgresStore) PutWorkflow(ctx context.Context, record RegisteredWorkflow) error {
	return (sharedCatalog{store.db, true}).put(ctx, record)
}
func (store *SQLiteStore) PutWorkflow(ctx context.Context, record RegisteredWorkflow) error {
	return (sharedCatalog{store.db, false}).put(ctx, record)
}
func (store *PostgresStore) ListDefinitions(ctx context.Context) ([]RegisteredWorkflow, error) {
	return (sharedCatalog{store.db, true}).list(ctx)
}
func (store *SQLiteStore) ListDefinitions(ctx context.Context) ([]RegisteredWorkflow, error) {
	return (sharedCatalog{store.db, false}).list(ctx)
}
func (store *PostgresStore) GetWorkflow(ctx context.Context, name string, version int) (RegisteredWorkflow, error) {
	return (sharedCatalog{store.db, true}).get(ctx, name, version)
}
func (store *SQLiteStore) GetWorkflow(ctx context.Context, name string, version int) (RegisteredWorkflow, error) {
	return (sharedCatalog{store.db, false}).get(ctx, name, version)
}
func (store *PostgresStore) CreateSession(ctx context.Context, id string, session identity.Session) error {
	return (sharedCatalog{store.db, true}).createSession(ctx, id, session)
}
func (store *SQLiteStore) CreateSession(ctx context.Context, id string, session identity.Session) error {
	return (sharedCatalog{store.db, false}).createSession(ctx, id, session)
}
func (store *PostgresStore) UpdateSession(ctx context.Context, id string, operation func(*identity.Session) error) error {
	return (sharedCatalog{store.db, true}).updateSession(ctx, id, operation)
}
func (store *SQLiteStore) UpdateSession(ctx context.Context, id string, operation func(*identity.Session) error) error {
	return (sharedCatalog{store.db, false}).updateSession(ctx, id, operation)
}
func (store *PostgresStore) DeleteSession(ctx context.Context, id string) error {
	return (sharedCatalog{store.db, true}).deleteSession(ctx, id)
}
func (store *SQLiteStore) DeleteSession(ctx context.Context, id string) error {
	return (sharedCatalog{store.db, false}).deleteSession(ctx, id)
}

func (store *MemoryStore) PutWorkflow(ctx context.Context, record RegisteredWorkflow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := record.Definition.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	versions := store.definitions[record.Definition.Name]
	if versions == nil {
		versions = make(map[int]RegisteredWorkflow)
		store.definitions[record.Definition.Name] = versions
	}
	existing, ok := versions[record.Definition.Version]
	if ok && (!reflect.DeepEqual(existing.Definition, record.Definition) || (existing.Source != "" && record.Source != "" && existing.Source != record.Source)) {
		return ErrDefinitionConflict
	}
	if ok && record.Source == "" {
		record.Source = existing.Source
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	var copy RegisteredWorkflow
	if err := json.Unmarshal(body, &copy); err != nil {
		return err
	}
	versions[record.Definition.Version] = copy
	return nil
}

func (store *MemoryStore) ListDefinitions(ctx context.Context) ([]RegisteredWorkflow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	records := make([]RegisteredWorkflow, 0)
	for _, versions := range store.definitions {
		for _, record := range versions {
			body, err := json.Marshal(record)
			if err != nil {
				return nil, err
			}
			var copy RegisteredWorkflow
			if err := json.Unmarshal(body, &copy); err != nil {
				return nil, err
			}
			records = append(records, copy)
		}
	}
	return records, nil
}

func (store *MemoryStore) GetWorkflow(ctx context.Context, name string, version int) (RegisteredWorkflow, error) {
	if err := ctx.Err(); err != nil {
		return RegisteredWorkflow{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	versions := store.definitions[name]
	if version == 0 {
		for candidate := range versions {
			if candidate > version {
				version = candidate
			}
		}
	}
	record, ok := versions[version]
	if !ok {
		return RegisteredWorkflow{}, ErrNotFound
	}
	body, err := json.Marshal(record)
	if err != nil {
		return RegisteredWorkflow{}, err
	}
	var copy RegisteredWorkflow
	err = json.Unmarshal(body, &copy)
	return copy, err
}
