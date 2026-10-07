package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
)

//go:embed migrations/0001_initial.sql
var postgresMigration embed.FS

//go:embed migrations/0002_outbox.sql
var postgresOutboxMigration []byte

//go:embed migrations/0003_idempotency.sql
var postgresIdempotencyMigration []byte

//go:embed migrations/0004_task_publication.sql
var postgresTaskPublicationMigration []byte

//go:embed migrations/0005_shared_task_leases.sql
var postgresLeaseMigration []byte

//go:embed migrations/0006_retention.sql
var postgresRetentionMigration []byte

//go:embed migrations/0007_portal_registry.sql
var postgresPortalRegistryMigration []byte

const postgresMigrationVersion = 1

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	if db == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	return &PostgresStore{db: db}, nil
}

func OpenPostgresStore(ctx context.Context, dataSource string) (*PostgresStore, error) {
	if dataSource == "" {
		return nil, fmt.Errorf("PostgreSQL data source is required")
	}
	db, err := sql.Open("pgx", dataSource)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return &PostgresStore{db: db}, nil
}

func (store *PostgresStore) Close() error { return store.db.Close() }

func (store *PostgresStore) WithWorkflowLock(ctx context.Context, workflowID string, operation func() error) error {
	if workflowID == "" || operation == nil {
		return fmt.Errorf("workflow ID and lock operation are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL workflow decision lock: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `SELECT pg_advisory_xact_lock(731020, hashtext($1))`, workflowID); err != nil {
		return fmt.Errorf("acquire PostgreSQL workflow decision lock: %w", err)
	}
	if err := operation(); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("release PostgreSQL workflow decision lock: %w", err)
	}
	return nil
}

func (store *PostgresStore) Ping(ctx context.Context) error {
	return store.db.PingContext(ctx)
}

func (store *PostgresStore) Migrate(ctx context.Context) error {
	return MigratePostgres(ctx, store.db)
}

func MigratePostgres(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("PostgreSQL database is required")
	}
	transaction, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL migration: %w", err)
	}
	defer transaction.Rollback()
	if err := ensurePostgresMigrationsTable(ctx, transaction); err != nil {
		return err
	}
	if err := migratePostgresInitial(ctx, transaction); err != nil {
		return err
	}
	if err := applyPostgresVersionedMigration(ctx, transaction, 2, postgresOutboxMigration, "outbox"); err != nil {
		return err
	}
	if err := applyPostgresVersionedMigration(ctx, transaction, 3, postgresIdempotencyMigration, "idempotency"); err != nil {
		return err
	}
	if err := applyPostgresVersionedMigration(ctx, transaction, 4, postgresTaskPublicationMigration, "task publication"); err != nil {
		return err
	}
	if err := applyPostgresVersionedMigration(ctx, transaction, 5, postgresLeaseMigration, "lease"); err != nil {
		return err
	}
	if err := applyPostgresVersionedMigration(ctx, transaction, 6, postgresRetentionMigration, "retention"); err != nil {
		return err
	}
	if err := applyPortalRegistryMigration(ctx, transaction, postgresPortalRegistryMigration); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL migration: %w", err)
	}
	return nil
}

// ensurePostgresMigrationsTable creates the schema_migrations tracking table if it doesn't exist yet.
func ensurePostgresMigrationsTable(ctx context.Context, transaction *sql.Tx) error {
	if _, err := transaction.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	return nil
}

// migratePostgresInitial applies the version 1 (initial schema) migration, which is read from an
// embedded file rather than an in-memory byte slice like the later versioned migrations.
func migratePostgresInitial(ctx context.Context, transaction *sql.Tx) error {
	var applied bool
	if err := transaction.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, postgresMigrationVersion).Scan(&applied); err != nil {
		return fmt.Errorf("check PostgreSQL migration version: %w", err)
	}
	if applied {
		return nil
	}
	migration, err := postgresMigration.ReadFile("migrations/0001_initial.sql")
	if err != nil {
		return fmt.Errorf("read PostgreSQL migration: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, string(migration)); err != nil {
		return fmt.Errorf("apply PostgreSQL migration: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, postgresMigrationVersion); err != nil {
		return fmt.Errorf("record PostgreSQL migration: %w", err)
	}
	return nil
}

// applyPostgresVersionedMigration applies a single numbered migration step if it hasn't been
// recorded in schema_migrations yet, wrapping errors with the given human-readable label.
func applyPostgresVersionedMigration(ctx context.Context, transaction *sql.Tx, version int, migration []byte, label string) error {
	var applied bool
	checkQuery := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = %d)`, version)
	if err := transaction.QueryRowContext(ctx, checkQuery).Scan(&applied); err != nil {
		return fmt.Errorf("check PostgreSQL %s migration version: %w", label, err)
	}
	if applied {
		return nil
	}
	if _, err := transaction.ExecContext(ctx, string(migration)); err != nil {
		return fmt.Errorf("apply PostgreSQL %s migration: %w", label, err)
	}
	insertQuery := fmt.Sprintf(`INSERT INTO schema_migrations (version) VALUES (%d) ON CONFLICT DO NOTHING`, version)
	if _, err := transaction.ExecContext(ctx, insertQuery); err != nil {
		return fmt.Errorf("record PostgreSQL %s migration: %w", label, err)
	}
	return nil
}

func (store *PostgresStore) Create(instance *domain.WorkflowInstance) error {
	return store.CreateContext(context.Background(), instance)
}

func (store *PostgresStore) CreateContext(ctx context.Context, instance *domain.WorkflowInstance) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin workflow creation: %w", err)
	}
	defer transaction.Rollback()
	if err := createPostgresExecution(ctx, transaction, instance); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit workflow creation: %w", err)
	}
	return nil
}

func (store *PostgresStore) CreateIdempotent(ctx context.Context, instance *domain.WorkflowInstance, key, requestHash string) (bool, string, error) {
	if key == "" || requestHash == "" {
		return false, "", fmt.Errorf("idempotency key and request hash are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin idempotent workflow creation: %w", err)
	}
	defer transaction.Rollback()
	if err := createPostgresExecution(ctx, transaction, instance); err != nil {
		return false, "", err
	}
	result, err := transaction.ExecContext(ctx, `INSERT INTO workflow_idempotency_keys
		(workflow_name, definition_version, idempotency_key, request_hash, workflow_id)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (workflow_name, definition_version, idempotency_key) DO NOTHING`,
		instance.Definition.Name, instance.Definition.Version, key, requestHash, instance.ID)
	if err != nil {
		return false, "", fmt.Errorf("claim workflow idempotency key: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, "", fmt.Errorf("check workflow idempotency claim: %w", err)
	}
	if inserted == 0 {
		var existingHash, existingID string
		if err := transaction.QueryRowContext(ctx, `SELECT request_hash, workflow_id FROM workflow_idempotency_keys
			WHERE workflow_name = $1 AND definition_version = $2 AND idempotency_key = $3`,
			instance.Definition.Name, instance.Definition.Version, key).Scan(&existingHash, &existingID); err != nil {
			return false, "", fmt.Errorf("read workflow idempotency claim: %w", err)
		}
		if existingHash != requestHash {
			return false, "", ErrIdempotencyConflict
		}
		return false, existingID, nil
	}
	if err := transaction.Commit(); err != nil {
		return false, "", fmt.Errorf("commit idempotent workflow creation: %w", err)
	}
	return true, instance.ID, nil
}

func createPostgresExecution(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance) error {
	if err := validatePostgresNewExecution(instance); err != nil {
		return err
	}
	definitionJSON, snapshotJSON, contextJSON, err := marshalPostgresExecutionPayload(instance)
	if err != nil {
		return err
	}
	if err := upsertPostgresWorkflowDefinition(ctx, transaction, instance, definitionJSON); err != nil {
		return err
	}
	inserted, err := insertPostgresWorkflowExecution(ctx, transaction, instance, contextJSON, snapshotJSON)
	if err != nil {
		return err
	}
	if !inserted {
		return ErrAlreadyExists
	}
	for _, event := range instance.Events {
		if err := insertWorkflowEvent(ctx, transaction, event); err != nil {
			return err
		}
	}
	return syncTasks(ctx, transaction, instance)
}

// validatePostgresNewExecution checks the required preconditions for creating a new workflow execution.
func validatePostgresNewExecution(instance *domain.WorkflowInstance) error {
	if instance == nil {
		return fmt.Errorf("workflow instance is required")
	}
	if instance.ID == "" {
		return fmt.Errorf("workflow ID is required")
	}
	if err := instance.Definition.Validate(); err != nil {
		return fmt.Errorf("validate workflow definition: %w", err)
	}
	return nil
}

// marshalPostgresExecutionPayload encodes the definition, snapshot, and context JSON payloads
// needed to persist a new workflow execution.
func marshalPostgresExecutionPayload(instance *domain.WorkflowInstance) (definitionJSON, snapshotJSON, contextJSON []byte, err error) {
	definitionJSON, err = json.Marshal(instance.Definition)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode workflow definition: %w", err)
	}
	snapshotJSON, err = json.Marshal(instance)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode workflow snapshot: %w", err)
	}
	contextJSON, err = json.Marshal(instance.Context)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode workflow context: %w", err)
	}
	return definitionJSON, snapshotJSON, contextJSON, nil
}

// upsertPostgresWorkflowDefinition inserts the workflow definition if absent, then verifies that
// the stored definition matches the one being created, returning ErrDefinitionConflict otherwise.
func upsertPostgresWorkflowDefinition(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance, definitionJSON []byte) error {
	if _, err := transaction.ExecContext(ctx, `INSERT INTO workflow_definitions (workflow_name, version, definition)
		VALUES ($1, $2, $3) ON CONFLICT (workflow_name, version) DO NOTHING`, instance.Definition.Name, instance.Definition.Version, definitionJSON); err != nil {
		return fmt.Errorf("insert workflow definition: %w", err)
	}
	var existingDefinition []byte
	if err := transaction.QueryRowContext(ctx, `SELECT definition FROM workflow_definitions WHERE workflow_name = $1 AND version = $2`, instance.Definition.Name, instance.Definition.Version).Scan(&existingDefinition); err != nil {
		return fmt.Errorf("read workflow definition: %w", err)
	}
	var storedDefinition domain.WorkflowDef
	if err := json.Unmarshal(existingDefinition, &storedDefinition); err != nil {
		return fmt.Errorf("decode stored workflow definition: %w", err)
	}
	if !reflect.DeepEqual(storedDefinition, instance.Definition) {
		return ErrDefinitionConflict
	}
	return nil
}

// insertPostgresWorkflowExecution inserts the workflow execution row, reporting whether a new row
// was actually inserted (false means a row with the same ID already existed).
func insertPostgresWorkflowExecution(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance, contextJSON, snapshotJSON []byte) (bool, error) {
	result, err := transaction.ExecContext(ctx, `INSERT INTO workflow_executions
		(id, workflow_name, definition_version, status, context, snapshot)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`,
		instance.ID, instance.Definition.Name, instance.Definition.Version, instance.Status, contextJSON, snapshotJSON)
	if err != nil {
		return false, fmt.Errorf("insert workflow execution: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check workflow insertion: %w", err)
	}
	return inserted != 0, nil
}

func (store *PostgresStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	return store.GetContext(context.Background(), workflowID)
}

func (store *PostgresStore) GetContext(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	var snapshot []byte
	if err := store.db.QueryRowContext(ctx, `SELECT snapshot FROM workflow_executions WHERE id = $1`, workflowID).Scan(&snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return nil, fmt.Errorf("read workflow execution: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal(snapshot, &instance); err != nil {
		return nil, fmt.Errorf("decode workflow snapshot: %w", err)
	}
	return &instance, nil
}

func (store *PostgresStore) ListExecutions(ctx context.Context, filter ExecutionFilter) ([]*domain.WorkflowInstance, int, error) {
	if filter.Limit < 1 || filter.Offset < 0 {
		return nil, 0, fmt.Errorf("execution limit must be positive and offset cannot be negative")
	}
	var total int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_executions
		WHERE ($1 = '' OR workflow_name = $1) AND ($2 = '' OR status = $2)`, filter.WorkflowName, string(filter.Status)).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count PostgreSQL workflow executions: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT snapshot FROM workflow_executions
		WHERE ($1 = '' OR workflow_name = $1) AND ($2 = '' OR status = $2)
		ORDER BY updated_at DESC, id DESC LIMIT $3 OFFSET $4`, filter.WorkflowName, string(filter.Status), filter.Limit, filter.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list PostgreSQL workflow executions: %w", err)
	}
	defer rows.Close()
	instances := make([]*domain.WorkflowInstance, 0)
	for rows.Next() {
		var snapshot []byte
		if err := rows.Scan(&snapshot); err != nil {
			return nil, 0, fmt.Errorf("scan PostgreSQL workflow execution: %w", err)
		}
		var instance domain.WorkflowInstance
		if err := json.Unmarshal(snapshot, &instance); err != nil {
			return nil, 0, fmt.Errorf("decode PostgreSQL workflow execution: %w", err)
		}
		instances = append(instances, &instance)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read PostgreSQL workflow executions: %w", err)
	}
	return instances, total, nil
}

func (store *PostgresStore) CountExecutions(ctx context.Context) (map[domain.WorkflowStatus]int, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM workflow_executions GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count PostgreSQL executions by status: %w", err)
	}
	defer rows.Close()
	counts := make(map[domain.WorkflowStatus]int)
	for rows.Next() {
		var status domain.WorkflowStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL execution count: %w", err)
		}
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read PostgreSQL execution counts: %w", err)
	}
	return counts, nil
}

func (store *PostgresStore) Append(workflowID string, event domain.Event) error {
	return store.AppendContext(context.Background(), workflowID, event)
}

func (store *PostgresStore) AppendContext(ctx context.Context, workflowID string, event domain.Event) error {
	return store.AppendManyContext(ctx, workflowID, []domain.Event{event})
}

func (store *PostgresStore) AppendMany(workflowID string, events []domain.Event) error {
	return store.AppendManyContext(context.Background(), workflowID, events)
}

func (store *PostgresStore) AppendManyContext(ctx context.Context, workflowID string, events []domain.Event) error {
	if workflowID == "" || len(events) == 0 {
		return fmt.Errorf("workflow ID and at least one workflow event are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin workflow append: %w", err)
	}
	defer transaction.Rollback()
	if err := appendPostgresEvents(ctx, transaction, workflowID, events); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit workflow append batch: %w", err)
	}
	return nil
}

type postgresMutationStore struct {
	ctx         context.Context
	transaction *sql.Tx
}

func (mutation postgresMutationStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	var snapshot []byte
	if err := mutation.transaction.QueryRowContext(mutation.ctx, `SELECT snapshot FROM workflow_executions WHERE id = $1`, workflowID).Scan(&snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return nil, fmt.Errorf("read PostgreSQL workflow snapshot in lease transaction: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal(snapshot, &instance); err != nil {
		return nil, fmt.Errorf("decode PostgreSQL workflow snapshot in lease transaction: %w", err)
	}
	return &instance, nil
}

func (mutation postgresMutationStore) Append(workflowID string, event domain.Event) error {
	return mutation.AppendMany(workflowID, []domain.Event{event})
}

func (mutation postgresMutationStore) AppendMany(workflowID string, events []domain.Event) error {
	if workflowID == "" || len(events) == 0 {
		return fmt.Errorf("workflow ID and at least one workflow event are required")
	}
	return appendPostgresEvents(mutation.ctx, mutation.transaction, workflowID, events)
}

func appendPostgresEvents(ctx context.Context, transaction *sql.Tx, workflowID string, events []domain.Event) error {
	if workflowID == "" || len(events) == 0 {
		return fmt.Errorf("workflow ID and at least one workflow event are required")
	}
	instance, version, err := loadPostgresExecutionForAppend(ctx, transaction, workflowID)
	if err != nil {
		return err
	}
	firstNewEvent := len(instance.Events)
	for _, event := range events {
		if err := instance.Append(event); err != nil {
			return err
		}
	}
	if err := savePostgresExecutionSnapshot(ctx, transaction, workflowID, version, &instance); err != nil {
		return err
	}
	for _, event := range instance.Events[firstNewEvent:] {
		if err := insertWorkflowEvent(ctx, transaction, event); err != nil {
			return err
		}
	}
	return syncTasks(ctx, transaction, &instance)
}

// loadPostgresExecutionForAppend reads and decodes the current snapshot and version of a workflow
// execution so new events can be appended to it.
func loadPostgresExecutionForAppend(ctx context.Context, transaction *sql.Tx, workflowID string) (domain.WorkflowInstance, int64, error) {
	var snapshot []byte
	var version int64
	if err := transaction.QueryRowContext(ctx, `SELECT snapshot, version FROM workflow_executions WHERE id = $1`, workflowID).Scan(&snapshot, &version); err != nil {
		if err == sql.ErrNoRows {
			return domain.WorkflowInstance{}, 0, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
		}
		return domain.WorkflowInstance{}, 0, fmt.Errorf("read workflow snapshot for append: %w", err)
	}
	var instance domain.WorkflowInstance
	if err := json.Unmarshal(snapshot, &instance); err != nil {
		return domain.WorkflowInstance{}, 0, fmt.Errorf("decode workflow snapshot for append: %w", err)
	}
	return instance, version, nil
}

// savePostgresExecutionSnapshot persists the updated snapshot with an optimistic version check,
// returning ErrConflict if the version has moved on since it was loaded.
func savePostgresExecutionSnapshot(ctx context.Context, transaction *sql.Tx, workflowID string, version int64, instance *domain.WorkflowInstance) error {
	snapshot, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("encode workflow snapshot: %w", err)
	}
	contextJSON, err := json.Marshal(instance.Context)
	if err != nil {
		return fmt.Errorf("encode workflow context: %w", err)
	}
	result, err := transaction.ExecContext(ctx, `UPDATE workflow_executions
		SET status = $1, context = $2, snapshot = $3, version = version + 1, updated_at = NOW()
		WHERE id = $4 AND version = $5`, instance.Status, contextJSON, snapshot, workflowID, version)
	if err != nil {
		return fmt.Errorf("update workflow snapshot: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check workflow snapshot update: %w", err)
	}
	if updated == 0 {
		return ErrConflict
	}
	return nil
}

func (store *PostgresStore) Claim(ctx context.Context, candidate lease.Lease, duration time.Duration) (lease.Lease, error) {
	if err := validateLeaseCandidate(candidate, duration); err != nil {
		return lease.Lease{}, err
	}
	workItem, err := json.Marshal(candidate.Item)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("encode lease work item: %w", err)
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("begin PostgreSQL lease claim: %w", err)
	}
	defer transaction.Rollback()
	claimed, err := scanPostgresLease(transaction.QueryRowContext(ctx, `INSERT INTO task_leases
		(lease_id, delivery_id, workflow_id, task_id, node_id, work_item, fencing_token, expires_at, state)
		VALUES ($1, $2, $3, $4, $5, $6, nextval('task_lease_fencing_token_seq'),
			clock_timestamp() + ($7 * INTERVAL '1 second'), 'ACTIVE')
		RETURNING lease_id, delivery_id, work_item, fencing_token, expires_at, state`,
		candidate.ID, candidate.DeliveryID, candidate.Item.WorkflowID, candidate.Item.TaskID,
		candidate.Item.NodeID, workItem, duration.Seconds()))
	if err != nil {
		if postgresUniqueViolation(err) {
			return lease.Lease{}, lease.ErrLeaseConflict
		}
		return lease.Lease{}, fmt.Errorf("claim PostgreSQL task lease: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return lease.Lease{}, fmt.Errorf("commit PostgreSQL lease claim: %w", err)
	}
	claimed.Item.LeaseToken = claimed.Token
	return claimed, nil
}

func (store *PostgresStore) Renew(ctx context.Context, leaseID string, token uint64, duration time.Duration) (lease.Lease, error) {
	if leaseID == "" || token == 0 || duration <= 0 {
		return lease.Lease{}, fmt.Errorf("lease ID, token, and positive duration are required")
	}
	if token > uint64(1<<63-1) {
		return lease.Lease{}, fmt.Errorf("lease token exceeds the PostgreSQL integer range")
	}
	updated, err := scanPostgresLease(store.db.QueryRowContext(ctx, `UPDATE task_leases
		SET expires_at = clock_timestamp() + ($3 * INTERVAL '1 second'), updated_at = clock_timestamp()
		WHERE lease_id = $1 AND fencing_token = $2 AND state = 'ACTIVE' AND expires_at > clock_timestamp()
		RETURNING lease_id, delivery_id, work_item, fencing_token, expires_at, state`, leaseID, int64(token), duration.Seconds()))
	if err == nil {
		updated.Item.LeaseToken = updated.Token
		return updated, nil
	}
	if err != sql.ErrNoRows {
		return lease.Lease{}, fmt.Errorf("renew PostgreSQL task lease: %w", err)
	}
	return lease.Lease{}, store.leaseMutationError(ctx, leaseID, token)
}

func (store *PostgresStore) CompleteWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, error) {
	if leaseID == "" || token == 0 || token > uint64(1<<63-1) {
		return lease.Lease{}, fmt.Errorf("valid lease ID and token are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return lease.Lease{}, fmt.Errorf("begin PostgreSQL lease completion: %w", err)
	}
	defer transaction.Rollback()
	active, due, err := scanPostgresLeaseForUpdate(ctx, transaction, leaseID)
	if err != nil {
		return lease.Lease{}, err
	}
	if err := validateLeaseState(active, token, due); err != nil {
		return lease.Lease{}, err
	}
	mutation := postgresMutationStore{ctx: ctx, transaction: transaction}
	if active.State == "COMPLETED" {
		if operation != nil {
			if err := operation(active, mutation); err != nil {
				return lease.Lease{}, err
			}
		}
		return active, nil
	}
	if operation != nil {
		if err := operation(active, mutation); err != nil {
			return lease.Lease{}, err
		}
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE task_leases SET state = 'COMPLETED', updated_at = clock_timestamp()
		WHERE lease_id = $1 AND fencing_token = $2 AND state = 'ACTIVE'`, leaseID, int64(token)); err != nil {
		return lease.Lease{}, fmt.Errorf("complete PostgreSQL task lease: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return lease.Lease{}, fmt.Errorf("commit PostgreSQL lease completion: %w", err)
	}
	return active, nil
}

func (store *PostgresStore) ExpireWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, bool, error) {
	if leaseID == "" || token == 0 || token > uint64(1<<63-1) {
		return lease.Lease{}, false, fmt.Errorf("valid lease ID and token are required")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return lease.Lease{}, false, fmt.Errorf("begin PostgreSQL lease expiry: %w", err)
	}
	defer transaction.Rollback()
	expiredLease, due, err := scanPostgresLeaseForUpdate(ctx, transaction, leaseID)
	if err != nil {
		return lease.Lease{}, false, err
	}
	if err := validateLeaseState(expiredLease, token, false); err != nil {
		return lease.Lease{}, false, err
	}
	if expiredLease.State == "COMPLETED" {
		return expiredLease, false, nil
	}
	if !due {
		return expiredLease, false, nil
	}
	if operation != nil {
		if err := operation(expiredLease, postgresMutationStore{ctx: ctx, transaction: transaction}); err != nil {
			return lease.Lease{}, false, err
		}
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE task_leases SET state = 'EXPIRED', updated_at = clock_timestamp()
		WHERE lease_id = $1 AND fencing_token = $2 AND state = 'ACTIVE'`, leaseID, int64(token)); err != nil {
		return lease.Lease{}, false, fmt.Errorf("expire PostgreSQL task lease: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return lease.Lease{}, false, fmt.Errorf("commit PostgreSQL lease expiry: %w", err)
	}
	return expiredLease, true, nil
}

func (store *PostgresStore) ListExpired(ctx context.Context, limit int) ([]lease.Lease, error) {
	if limit < 1 {
		return nil, fmt.Errorf("expired lease limit must be positive")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT lease_id, delivery_id, work_item, fencing_token, expires_at, state
		FROM task_leases WHERE state = 'ACTIVE' AND expires_at <= clock_timestamp()
		ORDER BY expires_at, lease_id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired PostgreSQL task leases: %w", err)
	}
	defer rows.Close()
	var expired []lease.Lease
	for rows.Next() {
		activeLease, err := scanPostgresLease(rows)
		if err != nil {
			return nil, fmt.Errorf("scan expired PostgreSQL task lease: %w", err)
		}
		expired = append(expired, activeLease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read expired PostgreSQL task leases: %w", err)
	}
	return expired, nil
}

func validateLeaseCandidate(candidate lease.Lease, duration time.Duration) error {
	if candidate.ID == "" || candidate.DeliveryID == "" || candidate.Item.WorkflowID == "" || candidate.Item.TaskID == "" || candidate.Item.NodeID == "" {
		return fmt.Errorf("lease, delivery, workflow, task, and node IDs are required")
	}
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	return nil
}

type postgresLeaseRow interface {
	Scan(...any) error
}

func scanPostgresLease(row postgresLeaseRow) (lease.Lease, error) {
	var result lease.Lease
	var item []byte
	var token int64
	var state string
	err := row.Scan(&result.ID, &result.DeliveryID, &item, &token, &result.ExpiresAt, &state)
	if err != nil {
		return lease.Lease{}, err
	}
	if token <= 0 {
		return lease.Lease{}, fmt.Errorf("stored PostgreSQL lease token is invalid")
	}
	if err := json.Unmarshal(item, &result.Item); err != nil {
		return lease.Lease{}, fmt.Errorf("decode PostgreSQL lease work item: %w", err)
	}
	result.Token = uint64(token)
	result.State = state
	return result, nil
}

func scanPostgresLeaseForUpdate(ctx context.Context, transaction *sql.Tx, leaseID string) (lease.Lease, bool, error) {
	var result lease.Lease
	var item []byte
	var token int64
	var state string
	var workflowID, taskID, nodeID string
	err := transaction.QueryRowContext(ctx, `SELECT lease_id, delivery_id, work_item, fencing_token, expires_at, state,
		workflow_id, task_id, node_id FROM task_leases WHERE lease_id = $1 FOR UPDATE`, leaseID).
		Scan(&result.ID, &result.DeliveryID, &item, &token, &result.ExpiresAt, &state, &workflowID, &taskID, &nodeID)
	if err == sql.ErrNoRows {
		return lease.Lease{}, false, lease.ErrLeaseNotFound
	}
	if err != nil {
		return lease.Lease{}, false, fmt.Errorf("read PostgreSQL task lease: %w", err)
	}
	if token <= 0 {
		return lease.Lease{}, false, fmt.Errorf("stored PostgreSQL lease token is invalid")
	}
	if err := json.Unmarshal(item, &result.Item); err != nil {
		return lease.Lease{}, false, fmt.Errorf("decode PostgreSQL lease work item: %w", err)
	}
	if result.Item.WorkflowID != workflowID || result.Item.TaskID != taskID || result.Item.NodeID != nodeID {
		return lease.Lease{}, false, fmt.Errorf("stored PostgreSQL lease identity does not match its work item")
	}
	result.Token = uint64(token)
	result.State = state
	result.Item.LeaseToken = result.Token
	if err := validateLeaseState(result, uint64(token), false); err != nil {
		return lease.Lease{}, false, err
	}
	if state != "ACTIVE" && state != "COMPLETED" {
		return lease.Lease{}, false, lease.ErrLeaseExpired
	}
	var due bool
	if err := transaction.QueryRowContext(ctx, `SELECT expires_at <= clock_timestamp() FROM task_leases WHERE lease_id = $1`, leaseID).Scan(&due); err != nil {
		return lease.Lease{}, false, fmt.Errorf("check PostgreSQL task lease deadline after locking: %w", err)
	}
	if state == "COMPLETED" {
		due = false
	}
	return result, due, nil
}

func validateLeaseState(current lease.Lease, token uint64, due bool) error {
	if current.Token != token {
		return lease.ErrStaleToken
	}
	if due {
		return lease.ErrLeaseExpired
	}
	return nil
}

func (store *PostgresStore) leaseMutationError(ctx context.Context, leaseID string, token uint64) error {
	current, err := scanPostgresLease(store.db.QueryRowContext(ctx, `SELECT lease_id, delivery_id, work_item, fencing_token, expires_at, state
		FROM task_leases WHERE lease_id = $1`, leaseID))
	if err == sql.ErrNoRows {
		return lease.ErrLeaseNotFound
	}
	if err != nil {
		return fmt.Errorf("read PostgreSQL task lease after mutation conflict: %w", err)
	}
	if current.Token != token {
		return lease.ErrStaleToken
	}
	var state string
	var due bool
	if err := store.db.QueryRowContext(ctx, `SELECT state, expires_at <= clock_timestamp() FROM task_leases WHERE lease_id = $1`, leaseID).Scan(&state, &due); err != nil {
		return fmt.Errorf("check PostgreSQL task lease state: %w", err)
	}
	if state != "ACTIVE" || due {
		return lease.ErrLeaseExpired
	}
	return lease.ErrLeaseNotFound
}

func postgresUniqueViolation(err error) bool {
	type sqlStater interface{ SQLState() string }
	var stateError sqlStater
	return errors.As(err, &stateError) && stateError.SQLState() == "23505"
}

func (store *PostgresStore) ListEvents(ctx context.Context, workflowID string, afterSequence uint64, limit int) ([]domain.Event, error) {
	if workflowID == "" || limit < 1 {
		return nil, fmt.Errorf("workflow ID and positive event limit are required")
	}
	if afterSequence > uint64(1<<63-1) {
		return nil, fmt.Errorf("event cursor exceeds the database sequence range")
	}
	var exists bool
	if err := store.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM workflow_executions WHERE id = $1)`, workflowID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check workflow for event history: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT sequence, event_type, COALESCE(task_id, ''), COALESCE(node_id, ''), payload, occurred_at
		FROM workflow_events WHERE workflow_id = $1 AND sequence > $2 ORDER BY sequence LIMIT $3`, workflowID, int64(afterSequence), limit)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL workflow events: %w", err)
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		var sequence int64
		var event domain.Event
		var payload []byte
		if err := rows.Scan(&sequence, &event.Type, &event.TaskID, &event.NodeID, &payload, &event.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL workflow event: %w", err)
		}
		if err := json.Unmarshal(payload, &event.Payload); err != nil {
			return nil, fmt.Errorf("decode PostgreSQL workflow event payload: %w", err)
		}
		event.WorkflowID = workflowID
		event.Sequence = uint64(sequence)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read PostgreSQL workflow events: %w", err)
	}
	return events, nil
}

func insertWorkflowEvent(ctx context.Context, transaction *sql.Tx, event domain.Event) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("encode workflow event payload: %w", err)
	}
	_, err = transaction.ExecContext(ctx, `INSERT INTO workflow_events
		(workflow_id, sequence, event_type, task_id, node_id, payload, occurred_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7)`,
		event.WorkflowID, int64(event.Sequence), event.Type, event.TaskID, event.NodeID, payload, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("append workflow event: %w", err)
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode workflow event for outbox: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO workflow_event_outbox (workflow_id, sequence, event)
		VALUES ($1, $2, $3)`, event.WorkflowID, int64(event.Sequence), eventJSON); err != nil {
		return fmt.Errorf("enqueue workflow event: %w", err)
	}
	return insertTaskPublication(ctx, transaction, event, `INSERT INTO task_publication_outbox (task_id, work_item) VALUES ($1, $2)`)
}

func syncTasks(ctx context.Context, transaction *sql.Tx, instance *domain.WorkflowInstance) error {
	for _, task := range instance.Tasks {
		result, err := json.Marshal(task.Result)
		if err != nil {
			return fmt.Errorf("encode task result: %w", err)
		}
		var completedAt any
		if !task.CompletedAt.IsZero() {
			completedAt = task.CompletedAt
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO task_executions
			(workflow_id, node_id, task_id, task_name, status, result, error, lease_token, completed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, node_id) DO UPDATE SET
			task_id = EXCLUDED.task_id, task_name = EXCLUDED.task_name, status = EXCLUDED.status,
			result = EXCLUDED.result, error = EXCLUDED.error, lease_token = EXCLUDED.lease_token,
			completed_at = EXCLUDED.completed_at`,
			instance.ID, task.NodeID, task.ID, task.TaskName, task.Status, result, task.Error, int64(task.LeaseToken), completedAt); err != nil {
			return fmt.Errorf("sync task execution %q: %w", task.ID, err)
		}
	}
	return nil
}

var _ ExecutionStore = (*PostgresStore)(nil)
