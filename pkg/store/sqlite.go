package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	_ "modernc.org/sqlite"
)

//go:embed migrations/0001_sqlite.sql
var sqliteMigration embed.FS

//go:embed migrations/0002_sqlite_outbox.sql
var sqliteOutboxMigration []byte

//go:embed migrations/0003_sqlite_idempotency.sql
var sqliteIdempotencyMigration []byte

//go:embed migrations/0004_sqlite_task_publication.sql
var sqliteTaskPublicationMigration []byte

//go:embed migrations/0005_sqlite_task_leases.sql
var sqliteTaskLeaseMigration []byte

//go:embed migrations/0006_sqlite_retention.sql
var sqliteRetentionMigration []byte

type SQLiteStore struct {
	db             *sql.DB
	workflowLockMu sync.Mutex
	workflowLocks  map[string]*sync.Mutex
}

func NewSQLiteStore(db *sql.DB) (*SQLiteStore, error) {
	if db == nil {
		return nil, fmt.Errorf("SQLite database is required")
	}
	db.SetMaxOpenConns(1)
	return &SQLiteStore{db: db, workflowLocks: make(map[string]*sync.Mutex)}, nil
}

func OpenSQLiteStore(ctx context.Context, dataSource string) (*SQLiteStore, error) {
	if dataSource == "" {
		return nil, fmt.Errorf("SQLite data source is required")
	}
	db, err := sql.Open("sqlite", dataSource)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to SQLite: %w", err)
	}
	store := &SQLiteStore{db: db, workflowLocks: make(map[string]*sync.Mutex)}
	if err := store.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (store *SQLiteStore) WithWorkflowLock(ctx context.Context, workflowID string, operation func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if workflowID == "" || operation == nil {
		return fmt.Errorf("workflow ID and lock operation are required")
	}
	store.workflowLockMu.Lock()
	lock := store.workflowLocks[workflowID]
	if lock == nil {
		lock = &sync.Mutex{}
		store.workflowLocks[workflowID] = lock
	}
	store.workflowLockMu.Unlock()
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return operation()
}

func (store *SQLiteStore) Close() error { return store.db.Close() }

func (store *SQLiteStore) Migrate(ctx context.Context) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite migration: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create SQLite migration table: %w", err)
	}
	var applied bool
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite migration version: %w", err)
	}
	if !applied {
		migration, err := sqliteMigration.ReadFile("migrations/0001_sqlite.sql")
		if err != nil {
			return fmt.Errorf("read SQLite migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply SQLite migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite migration: %w", err)
		}
	}
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 2)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite outbox migration version: %w", err)
	}
	if !applied {
		if _, err := transaction.ExecContext(ctx, string(sqliteOutboxMigration)); err != nil {
			return fmt.Errorf("apply SQLite outbox migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (2) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite outbox migration: %w", err)
		}
	}
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 3)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite idempotency migration version: %w", err)
	}
	if !applied {
		if _, err := transaction.ExecContext(ctx, string(sqliteIdempotencyMigration)); err != nil {
			return fmt.Errorf("apply SQLite idempotency migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (3) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite idempotency migration: %w", err)
		}
	}
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 4)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite task publication migration version: %w", err)
	}
	if !applied {
		if _, err := transaction.ExecContext(ctx, string(sqliteTaskPublicationMigration)); err != nil {
			return fmt.Errorf("apply SQLite task publication migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (4) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite task publication migration: %w", err)
		}
	}
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 5)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite task lease migration version: %w", err)
	}
	if !applied {
		if _, err := transaction.ExecContext(ctx, string(sqliteTaskLeaseMigration)); err != nil {
			return fmt.Errorf("apply SQLite task lease migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (5) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite task lease migration: %w", err)
		}
	}
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 6)`).Scan(&applied); err != nil {
		return fmt.Errorf("check SQLite retention migration version: %w", err)
	}
	if !applied {
		if _, err := transaction.ExecContext(ctx, string(sqliteRetentionMigration)); err != nil {
			return fmt.Errorf("apply SQLite retention migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (6) ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("record SQLite retention migration: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite migration: %w", err)
	}
	return nil
}

func (store *SQLiteStore) Create(instance *domain.WorkflowInstance) error {
	return store.CreateContext(context.Background(), instance)
}

func (store *SQLiteStore) CreateContext(ctx context.Context, instance *domain.WorkflowInstance) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite workflow creation: %w", err)
	}
	defer transaction.Rollback()
	if err := createSQLiteExecution(ctx, transaction, instance); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite workflow creation: %w", err)
	}
	return nil
}

func (store *SQLiteStore) CreateIdempotent(ctx context.Context, instance *domain.WorkflowInstance, key, requestHash string) (bool, string, error) {
	if key == "" || requestHash == "" {
		return false, "", fmt.Errorf("idempotency key and request hash are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin idempotent SQLite workflow creation: %w", err)
	}
	defer transaction.Rollback()
	if err := createSQLiteExecution(ctx, transaction, instance); err != nil {
		return false, "", err
	}
	result, err := transaction.ExecContext(ctx, `INSERT INTO workflow_idempotency_keys
		(workflow_name, definition_version, idempotency_key, request_hash, workflow_id)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (workflow_name, definition_version, idempotency_key) DO NOTHING`,
		instance.Definition.Name, instance.Definition.Version, key, requestHash, instance.ID)
	if err != nil {
		return false, "", fmt.Errorf("claim SQLite workflow idempotency key: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, "", fmt.Errorf("check SQLite workflow idempotency claim: %w", err)
	}
	if inserted == 0 {
		var existingHash, existingID string
		if err := transaction.QueryRowContext(ctx, `SELECT request_hash, workflow_id FROM workflow_idempotency_keys
			WHERE workflow_name = ? AND definition_version = ? AND idempotency_key = ?`,
			instance.Definition.Name, instance.Definition.Version, key).Scan(&existingHash, &existingID); err != nil {
			return false, "", fmt.Errorf("read SQLite workflow idempotency claim: %w", err)
		}
		if existingHash != requestHash {
			return false, "", ErrIdempotencyConflict
		}
		return false, existingID, nil
	}
	if err := transaction.Commit(); err != nil {
		return false, "", fmt.Errorf("commit idempotent SQLite workflow creation: %w", err)
	}
	return true, instance.ID, nil
}

func createSQLiteExecution(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance) error {
	if instance == nil {
		return fmt.Errorf("workflow instance is required")
	}
	if instance.ID == "" {
		return fmt.Errorf("workflow ID is required")
	}
	if err := instance.Definition.Validate(); err != nil {
		return fmt.Errorf("validate workflow definition: %w", err)
	}
	definition, err := json.Marshal(instance.Definition)
	if err != nil {
		return fmt.Errorf("encode workflow definition: %w", err)
	}
	snapshot, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("encode workflow snapshot: %w", err)
	}
	workflowContext, err := json.Marshal(instance.Context)
	if err != nil {
		return fmt.Errorf("encode workflow context: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO workflow_definitions (workflow_name, version, definition)
		VALUES (?, ?, ?) ON CONFLICT (workflow_name, version) DO NOTHING`, instance.Definition.Name, instance.Definition.Version, string(definition)); err != nil {
		return fmt.Errorf("insert SQLite workflow definition: %w", err)
	}
	var existingDefinition string
	if err := transaction.QueryRowContext(ctx, `SELECT definition FROM workflow_definitions WHERE workflow_name = ? AND version = ?`, instance.Definition.Name, instance.Definition.Version).Scan(&existingDefinition); err != nil {
		return fmt.Errorf("read SQLite workflow definition: %w", err)
	}
	var storedDefinition domain.WorkflowDef
	if err := json.Unmarshal([]byte(existingDefinition), &storedDefinition); err != nil {
		return fmt.Errorf("decode SQLite workflow definition: %w", err)
	}
	if !reflect.DeepEqual(storedDefinition, instance.Definition) {
		return ErrDefinitionConflict
	}
	result, err := transaction.ExecContext(ctx, `INSERT INTO workflow_executions
		(id, workflow_name, definition_version, status, context, snapshot)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
		instance.ID, instance.Definition.Name, instance.Definition.Version, instance.Status, string(workflowContext), string(snapshot))
	if err != nil {
		return fmt.Errorf("insert SQLite workflow execution: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check SQLite workflow insertion: %w", err)
	}
	if inserted == 0 {
		return ErrAlreadyExists
	}
	for _, event := range instance.Events {
		if err := insertSQLiteWorkflowEvent(ctx, transaction, event); err != nil {
			return err
		}
	}
	if err := syncSQLiteTasks(ctx, transaction, instance); err != nil {
		return err
	}
	return nil
}

func (store *SQLiteStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	return store.GetContext(context.Background(), workflowID)
}

func (store *SQLiteStore) GetContext(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	var snapshot string
	if err := store.db.QueryRowContext(ctx, `SELECT snapshot FROM workflow_executions WHERE id = ?`, workflowID).Scan(&snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return nil, fmt.Errorf("read SQLite workflow execution: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal([]byte(snapshot), &instance); err != nil {
		return nil, fmt.Errorf("decode SQLite workflow snapshot: %w", err)
	}
	return &instance, nil
}

func (store *SQLiteStore) ListExecutions(ctx context.Context, filter ExecutionFilter) ([]*domain.WorkflowInstance, int, error) {
	if filter.Limit < 1 || filter.Offset < 0 {
		return nil, 0, fmt.Errorf("execution limit must be positive and offset cannot be negative")
	}
	var total int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_executions
		WHERE (? = '' OR workflow_name = ?) AND (? = '' OR status = ?)`, filter.WorkflowName, filter.WorkflowName, string(filter.Status), string(filter.Status)).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count SQLite workflow executions: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT snapshot FROM workflow_executions
		WHERE (? = '' OR workflow_name = ?) AND (? = '' OR status = ?)
		ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?`, filter.WorkflowName, filter.WorkflowName, string(filter.Status), string(filter.Status), filter.Limit, filter.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list SQLite workflow executions: %w", err)
	}
	defer rows.Close()
	instances := make([]*domain.WorkflowInstance, 0)
	for rows.Next() {
		var snapshot string
		if err := rows.Scan(&snapshot); err != nil {
			return nil, 0, fmt.Errorf("scan SQLite workflow execution: %w", err)
		}
		var instance domain.WorkflowInstance
		if err := json.Unmarshal([]byte(snapshot), &instance); err != nil {
			return nil, 0, fmt.Errorf("decode SQLite workflow execution: %w", err)
		}
		instances = append(instances, &instance)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read SQLite workflow executions: %w", err)
	}
	return instances, total, nil
}

func (store *SQLiteStore) CountExecutions(ctx context.Context) (map[domain.WorkflowStatus]int, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM workflow_executions GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count SQLite executions by status: %w", err)
	}
	defer rows.Close()
	counts := make(map[domain.WorkflowStatus]int)
	for rows.Next() {
		var status domain.WorkflowStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan SQLite execution count: %w", err)
		}
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read SQLite execution counts: %w", err)
	}
	return counts, nil
}

func (store *SQLiteStore) Append(workflowID string, event domain.Event) error {
	return store.AppendContext(context.Background(), workflowID, event)
}

func (store *SQLiteStore) AppendContext(ctx context.Context, workflowID string, event domain.Event) error {
	return store.AppendManyContext(ctx, workflowID, []domain.Event{event})
}

func (store *SQLiteStore) AppendMany(workflowID string, events []domain.Event) error {
	return store.AppendManyContext(context.Background(), workflowID, events)
}

func (store *SQLiteStore) AppendManyContext(ctx context.Context, workflowID string, events []domain.Event) error {
	if workflowID == "" || len(events) == 0 {
		return fmt.Errorf("workflow ID and at least one workflow event are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite workflow append: %w", err)
	}
	defer transaction.Rollback()
	if err := appendSQLiteEvents(ctx, transaction, workflowID, events); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite workflow append batch: %w", err)
	}
	return nil
}

type sqliteMutationStore struct {
	ctx         context.Context
	transaction *sql.Tx
}

func (mutation sqliteMutationStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	var snapshot string
	if err := mutation.transaction.QueryRowContext(mutation.ctx, `SELECT snapshot FROM workflow_executions WHERE id = ?`, workflowID).Scan(&snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return nil, fmt.Errorf("read SQLite workflow snapshot in lease transaction: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal([]byte(snapshot), &instance); err != nil {
		return nil, fmt.Errorf("decode SQLite workflow snapshot in lease transaction: %w", err)
	}
	return &instance, nil
}

func (mutation sqliteMutationStore) Append(workflowID string, event domain.Event) error {
	return mutation.AppendMany(workflowID, []domain.Event{event})
}

func (mutation sqliteMutationStore) AppendMany(workflowID string, events []domain.Event) error {
	return appendSQLiteEvents(mutation.ctx, mutation.transaction, workflowID, events)
}

func appendSQLiteEvents(ctx context.Context, transaction *sql.Tx, workflowID string, events []domain.Event) error {
	if workflowID == "" || len(events) == 0 {
		return fmt.Errorf("workflow ID and at least one workflow event are required")
	}
	var snapshot string
	var version int64
	if err := transaction.QueryRowContext(ctx, `SELECT snapshot, version FROM workflow_executions WHERE id = ?`, workflowID).Scan(&snapshot, &version); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return fmt.Errorf("read SQLite workflow snapshot for append: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal([]byte(snapshot), &instance); err != nil {
		return fmt.Errorf("decode SQLite workflow snapshot for append: %w", err)
	}
	firstNewEvent := len(instance.Events)
	for _, event := range events {
		if err := instance.Append(event); err != nil {
			return err
		}
	}
	snapshotBytes, err := json.Marshal(&instance)
	if err != nil {
		return fmt.Errorf("encode SQLite workflow snapshot: %w", err)
	}
	contextBytes, err := json.Marshal(instance.Context)
	if err != nil {
		return fmt.Errorf("encode SQLite workflow context: %w", err)
	}
	result, err := transaction.ExecContext(ctx, `UPDATE workflow_executions
		SET status = ?, context = ?, snapshot = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND version = ?`, string(instance.Status), string(contextBytes), string(snapshotBytes), workflowID, version)
	if err != nil {
		return fmt.Errorf("update SQLite workflow snapshot: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check SQLite workflow snapshot update: %w", err)
	}
	if updated == 0 {
		return ErrConflict
	}
	for _, event := range instance.Events[firstNewEvent:] {
		if err := insertSQLiteWorkflowEvent(ctx, transaction, event); err != nil {
			return err
		}
	}
	if err := syncSQLiteTasks(ctx, transaction, &instance); err != nil {
		return err
	}
	return nil
}

func (store *SQLiteStore) Claim(ctx context.Context, candidate lease.Lease, duration time.Duration) (lease.Lease, error) {
	if err := validateLeaseCandidate(candidate, duration); err != nil {
		return lease.Lease{}, err
	}
	workItem, err := json.Marshal(candidate.Item)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("encode SQLite lease work item: %w", err)
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("begin SQLite task lease claim: %w", err)
	}
	defer transaction.Rollback()
	expiresAt := time.Now().Add(duration)
	result, err := transaction.ExecContext(ctx, `INSERT INTO task_leases
		(lease_id, delivery_id, workflow_id, task_id, node_id, work_item, expires_at_ns, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`, candidate.ID, candidate.DeliveryID, candidate.Item.WorkflowID,
		candidate.Item.TaskID, candidate.Item.NodeID, string(workItem), expiresAt.UnixNano())
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "constraint failed") {
			return lease.Lease{}, lease.ErrLeaseConflict
		}
		return lease.Lease{}, fmt.Errorf("claim SQLite task lease: %w", err)
	}
	token, err := result.LastInsertId()
	if err != nil {
		return lease.Lease{}, fmt.Errorf("read SQLite task lease fencing token: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return lease.Lease{}, fmt.Errorf("commit SQLite task lease claim: %w", err)
	}
	candidate.Token = uint64(token)
	candidate.Item.LeaseToken = candidate.Token
	candidate.ExpiresAt = time.Unix(0, expiresAt.UnixNano()).UTC()
	candidate.State = "ACTIVE"
	return candidate, nil
}

func (store *SQLiteStore) Renew(ctx context.Context, leaseID string, token uint64, duration time.Duration) (lease.Lease, error) {
	if leaseID == "" || token == 0 || token > uint64(1<<63-1) || duration <= 0 {
		return lease.Lease{}, fmt.Errorf("lease ID, token, and positive duration are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("begin SQLite task lease renewal: %w", err)
	}
	defer transaction.Rollback()
	if err := lockSQLiteLeaseWrite(ctx, transaction, leaseID); err != nil {
		return lease.Lease{}, err
	}
	current, err := scanSQLiteLease(transaction.QueryRowContext(ctx, `SELECT lease_id, delivery_id, workflow_id, task_id, node_id, work_item, fencing_token, expires_at_ns, state FROM task_leases WHERE lease_id = ?`, leaseID))
	if err != nil {
		return lease.Lease{}, classifySQLiteLeaseError(transaction, ctx, leaseID, token, err)
	}
	if current.State != "ACTIVE" {
		if current.Token != token {
			return lease.Lease{}, lease.ErrStaleToken
		}
		return lease.Lease{}, lease.ErrLeaseExpired
	}
	if err := validateSQLiteLease(current, token, time.Now()); err != nil {
		return lease.Lease{}, err
	}
	expiresAt := time.Now().Add(duration)
	if _, err := transaction.ExecContext(ctx, `UPDATE task_leases SET expires_at_ns = ?, updated_at = CURRENT_TIMESTAMP WHERE lease_id = ? AND state = 'ACTIVE'`, expiresAt.UnixNano(), leaseID); err != nil {
		return lease.Lease{}, fmt.Errorf("renew SQLite task lease: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return lease.Lease{}, fmt.Errorf("commit SQLite task lease renewal: %w", err)
	}
	current.ExpiresAt = expiresAt.UTC()
	return current, nil
}

func (store *SQLiteStore) CompleteWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, error) {
	transaction, err := store.beginSQLiteLeaseMutation(ctx, leaseID, token)
	if err != nil {
		return lease.Lease{}, err
	}
	defer transaction.tx.Rollback()
	if transaction.lease.State == "COMPLETED" {
		if operation != nil {
			if err := operation(transaction.lease, transaction.mutation); err != nil {
				return lease.Lease{}, err
			}
		}
		return transaction.lease, nil
	}
	if !transaction.lease.ExpiresAt.After(time.Now()) {
		return lease.Lease{}, lease.ErrLeaseExpired
	}
	if operation != nil {
		if err := operation(transaction.lease, transaction.mutation); err != nil {
			return lease.Lease{}, err
		}
	}
	if _, err := transaction.tx.ExecContext(ctx, `UPDATE task_leases SET state = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE lease_id = ? AND state = 'ACTIVE'`, leaseID); err != nil {
		return lease.Lease{}, fmt.Errorf("complete SQLite task lease: %w", err)
	}
	if err := transaction.tx.Commit(); err != nil {
		return lease.Lease{}, fmt.Errorf("commit SQLite task completion: %w", err)
	}
	transaction.lease.State = "COMPLETED"
	return transaction.lease, nil
}

func (store *SQLiteStore) ExpireWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, bool, error) {
	transaction, err := store.beginSQLiteLeaseMutation(ctx, leaseID, token)
	if err != nil {
		return lease.Lease{}, false, err
	}
	defer transaction.tx.Rollback()
	if transaction.lease.State == "COMPLETED" {
		return transaction.lease, false, nil
	}
	if transaction.lease.ExpiresAt.After(time.Now()) {
		return transaction.lease, false, nil
	}
	if operation != nil {
		if err := operation(transaction.lease, transaction.mutation); err != nil {
			return lease.Lease{}, false, err
		}
	}
	if _, err := transaction.tx.ExecContext(ctx, `UPDATE task_leases SET state = 'EXPIRED', updated_at = CURRENT_TIMESTAMP WHERE lease_id = ? AND state = 'ACTIVE'`, leaseID); err != nil {
		return lease.Lease{}, false, fmt.Errorf("expire SQLite task lease: %w", err)
	}
	if err := transaction.tx.Commit(); err != nil {
		return lease.Lease{}, false, fmt.Errorf("commit SQLite task lease expiry: %w", err)
	}
	transaction.lease.State = "EXPIRED"
	return transaction.lease, true, nil
}

func (store *SQLiteStore) ListExpired(ctx context.Context, limit int) ([]lease.Lease, error) {
	if limit < 1 {
		return nil, fmt.Errorf("expired lease limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT lease_id, delivery_id, workflow_id, task_id, node_id, work_item, fencing_token, expires_at_ns, state
		FROM task_leases WHERE state = 'ACTIVE' AND expires_at_ns <= ? ORDER BY expires_at_ns, lease_id LIMIT ?`, time.Now().UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("list expired SQLite task leases: %w", err)
	}
	defer rows.Close()
	var expired []lease.Lease
	for rows.Next() {
		active, err := scanSQLiteLease(rows)
		if err != nil {
			return nil, err
		}
		expired = append(expired, active)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read expired SQLite task leases: %w", err)
	}
	return expired, nil
}

type sqliteLeaseMutation struct {
	tx       *sql.Tx
	lease    lease.Lease
	mutation sqliteMutationStore
}

func (store *SQLiteStore) beginSQLiteLeaseMutation(ctx context.Context, leaseID string, token uint64) (sqliteLeaseMutation, error) {
	if leaseID == "" || token == 0 || token > uint64(1<<63-1) {
		return sqliteLeaseMutation{}, fmt.Errorf("valid lease ID and token are required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sqliteLeaseMutation{}, fmt.Errorf("begin SQLite task lease mutation: %w", err)
	}
	if err := lockSQLiteLeaseWrite(ctx, tx, leaseID); err != nil {
		_ = tx.Rollback()
		return sqliteLeaseMutation{}, err
	}
	active, err := scanSQLiteLease(tx.QueryRowContext(ctx, `SELECT lease_id, delivery_id, workflow_id, task_id, node_id, work_item, fencing_token, expires_at_ns, state FROM task_leases WHERE lease_id = ?`, leaseID))
	if err != nil {
		classified := classifySQLiteLeaseError(tx, ctx, leaseID, token, err)
		_ = tx.Rollback()
		return sqliteLeaseMutation{}, classified
	}
	if active.Token != token {
		_ = tx.Rollback()
		return sqliteLeaseMutation{}, lease.ErrStaleToken
	}
	if active.State != "ACTIVE" && active.State != "COMPLETED" {
		_ = tx.Rollback()
		return sqliteLeaseMutation{}, lease.ErrLeaseExpired
	}
	return sqliteLeaseMutation{tx: tx, lease: active, mutation: sqliteMutationStore{ctx: ctx, transaction: tx}}, nil
}

func lockSQLiteLeaseWrite(ctx context.Context, tx *sql.Tx, leaseID string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE task_leases SET updated_at = updated_at WHERE lease_id = ?`, leaseID); err != nil {
		return fmt.Errorf("lock SQLite task lease row: %w", err)
	}
	return nil
}

type sqliteLeaseRow interface {
	Scan(...any) error
}

func scanSQLiteLease(row sqliteLeaseRow) (lease.Lease, error) {
	var active lease.Lease
	var workflowID, taskID, nodeID string
	var item []byte
	var token, expiry int64
	err := row.Scan(&active.ID, &active.DeliveryID, &workflowID, &taskID, &nodeID, &item, &token, &expiry, &active.State)
	if err != nil {
		return lease.Lease{}, err
	}
	if token <= 0 {
		return lease.Lease{}, fmt.Errorf("stored SQLite lease token is invalid")
	}
	if err := json.Unmarshal(item, &active.Item); err != nil {
		return lease.Lease{}, fmt.Errorf("decode SQLite lease work item: %w", err)
	}
	if active.Item.WorkflowID != workflowID || active.Item.TaskID != taskID || active.Item.NodeID != nodeID {
		return lease.Lease{}, fmt.Errorf("stored SQLite lease identity does not match its work item")
	}
	active.Token = uint64(token)
	active.Item.LeaseToken = active.Token
	active.ExpiresAt = time.Unix(0, expiry).UTC()
	return active, nil
}

func validateSQLiteLease(active lease.Lease, token uint64, now time.Time) error {
	if active.Token != token {
		return lease.ErrStaleToken
	}
	if active.State == "COMPLETED" {
		return nil
	}
	if active.State != "ACTIVE" || !active.ExpiresAt.After(now) {
		return lease.ErrLeaseExpired
	}
	return nil
}

type sqliteLeaseQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func classifySQLiteLeaseError(queryer sqliteLeaseQueryer, ctx context.Context, leaseID string, token uint64, cause error) error {
	if cause != sql.ErrNoRows {
		return fmt.Errorf("read SQLite task lease: %w", cause)
	}
	var state string
	var storedToken int64
	if err := queryer.QueryRowContext(ctx, `SELECT fencing_token, state FROM task_leases WHERE lease_id = ?`, leaseID).Scan(&storedToken, &state); err != nil {
		return lease.ErrLeaseNotFound
	}
	if uint64(storedToken) != token {
		return lease.ErrStaleToken
	}
	if state != "ACTIVE" {
		if state == "COMPLETED" {
			return lease.ErrLeaseNotFound
		}
		return lease.ErrLeaseExpired
	}
	return lease.ErrLeaseNotFound
}

func (store *SQLiteStore) ListEvents(ctx context.Context, workflowID string, afterSequence uint64, limit int) ([]domain.Event, error) {
	if workflowID == "" || limit < 1 {
		return nil, fmt.Errorf("workflow ID and positive event limit are required")
	}
	if afterSequence > uint64(1<<63-1) {
		return nil, fmt.Errorf("event cursor exceeds the database sequence range")
	}
	var exists bool
	if err := store.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM workflow_executions WHERE id = ?)`, workflowID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check SQLite workflow for event history: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT sequence, event_type, COALESCE(task_id, ''), COALESCE(node_id, ''), payload, occurred_at
		FROM workflow_events WHERE workflow_id = ? AND sequence > ? ORDER BY sequence LIMIT ?`, workflowID, int64(afterSequence), limit)
	if err != nil {
		return nil, fmt.Errorf("list SQLite workflow events: %w", err)
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		var sequence int64
		var event domain.Event
		var payload string
		var occurredAt string
		if err := rows.Scan(&sequence, &event.Type, &event.TaskID, &event.NodeID, &payload, &occurredAt); err != nil {
			return nil, fmt.Errorf("scan SQLite workflow event: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &event.Payload); err != nil {
			return nil, fmt.Errorf("decode SQLite workflow event payload: %w", err)
		}
		event.OccurredAt, err = parseSQLiteTimestamp(occurredAt)
		if err != nil {
			return nil, fmt.Errorf("parse SQLite workflow event timestamp: %w", err)
		}
		event.WorkflowID = workflowID
		event.Sequence = uint64(sequence)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read SQLite workflow events: %w", err)
	}
	return events, nil
}

func parseSQLiteTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", value)
}

func insertSQLiteWorkflowEvent(ctx context.Context, transaction *sql.Tx, event domain.Event) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("encode SQLite workflow event payload: %w", err)
	}
	_, err = transaction.ExecContext(ctx, `INSERT INTO workflow_events
		(workflow_id, sequence, event_type, task_id, node_id, payload, occurred_at)
		VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)`,
		event.WorkflowID, int64(event.Sequence), string(event.Type), event.TaskID, event.NodeID, string(payload), event.OccurredAt)
	if err != nil {
		return fmt.Errorf("append SQLite workflow event: %w", err)
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode SQLite workflow event for outbox: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO workflow_event_outbox (workflow_id, sequence, event, created_at, available_at)
		VALUES (?, ?, ?, ?, ?)`, event.WorkflowID, int64(event.Sequence), string(eventJSON), time.Now().UnixNano(), time.Now().UnixNano()); err != nil {
		return fmt.Errorf("enqueue SQLite workflow event: %w", err)
	}
	return insertTaskPublication(ctx, transaction, event, `INSERT INTO task_publication_outbox (task_id, work_item, created_at, available_at) VALUES (?, ?, ?, ?)`, time.Now().UnixNano(), time.Now().UnixNano())
}

func syncSQLiteTasks(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance) error {
	for _, task := range instance.Tasks {
		result, err := json.Marshal(task.Result)
		if err != nil {
			return fmt.Errorf("encode SQLite task result: %w", err)
		}
		var completedAt any
		if !task.CompletedAt.IsZero() {
			completedAt = task.CompletedAt
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO task_executions
			(workflow_id, node_id, task_id, task_name, status, result, error, lease_token, completed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (workflow_id, node_id) DO UPDATE SET
			task_id = excluded.task_id, task_name = excluded.task_name, status = excluded.status,
			result = excluded.result, error = excluded.error, lease_token = excluded.lease_token,
			completed_at = excluded.completed_at`,
			instance.ID, task.NodeID, task.ID, task.TaskName, string(task.Status), string(result), task.Error, int64(task.LeaseToken), completedAt); err != nil {
			return fmt.Errorf("sync SQLite task execution %q: %w", task.ID, err)
		}
	}
	return nil
}

var _ ExecutionStore = (*SQLiteStore)(nil)
