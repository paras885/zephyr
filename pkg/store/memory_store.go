package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
)

type MemoryStore struct {
	mu               sync.RWMutex
	workflowLockMu   sync.Mutex
	workflowLocks    map[string]*sync.Mutex
	instances        map[string]*domain.WorkflowInstance
	idempotency      map[idempotencyIdentity]idempotencyClaim
	taskPublications map[string]*memoryTaskPublication
	leases           map[string]lease.Lease
	fencingToken     uint64
}

type memoryTaskPublication struct {
	record       TaskPublicationRecord
	createdAt    time.Time
	availableAt  time.Time
	claimedUntil time.Time
	publishedAt  time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflowLocks:    make(map[string]*sync.Mutex),
		instances:        make(map[string]*domain.WorkflowInstance),
		idempotency:      make(map[idempotencyIdentity]idempotencyClaim),
		taskPublications: make(map[string]*memoryTaskPublication),
		leases:           make(map[string]lease.Lease),
	}
}

func (store *MemoryStore) WithWorkflowLock(ctx context.Context, workflowID string, operation func() error) error {
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

func (store *MemoryStore) Create(instance *domain.WorkflowInstance) error {
	if instance == nil {
		return fmt.Errorf("workflow instance is required")
	}
	publications, err := prepareMemoryTaskPublications(instance.Events)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.instances[instance.ID]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, instance.ID)
	}
	if err := store.addTaskPublications(publications); err != nil {
		return err
	}
	store.instances[instance.ID] = cloneInstance(instance)
	return nil
}

func (store *MemoryStore) CreateIdempotent(ctx context.Context, instance *domain.WorkflowInstance, key, requestHash string) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	if instance == nil || key == "" || requestHash == "" {
		return false, "", fmt.Errorf("workflow instance, idempotency key, and request hash are required")
	}
	if err := instance.Definition.Validate(); err != nil {
		return false, "", err
	}
	publications, err := prepareMemoryTaskPublications(instance.Events)
	if err != nil {
		return false, "", err
	}
	identity := idempotencyIdentity{workflowName: instance.Definition.Name, version: instance.Definition.Version, key: key}
	store.mu.Lock()
	defer store.mu.Unlock()
	if claim, exists := store.idempotency[identity]; exists {
		if claim.requestHash != requestHash {
			return false, "", ErrIdempotencyConflict
		}
		return false, claim.workflowID, nil
	}
	if _, exists := store.instances[instance.ID]; exists {
		return false, "", fmt.Errorf("%w: %s", ErrAlreadyExists, instance.ID)
	}
	if err := store.addTaskPublications(publications); err != nil {
		return false, "", err
	}
	store.instances[instance.ID] = cloneInstance(instance)
	store.idempotency[identity] = idempotencyClaim{requestHash: requestHash, workflowID: instance.ID}
	return true, instance.ID, nil
}

func prepareMemoryTaskPublications(events []domain.Event) (map[string]*memoryTaskPublication, error) {
	publications := make(map[string]*memoryTaskPublication)
	now := time.Now().UTC()
	for _, event := range events {
		record, scheduled, err := taskPublicationFromEvent(event)
		if err != nil {
			return nil, err
		}
		if !scheduled {
			continue
		}
		if _, exists := publications[record.Item.TaskID]; exists {
			return nil, fmt.Errorf("task publication already exists for task %q", record.Item.TaskID)
		}
		publications[record.Item.TaskID] = &memoryTaskPublication{
			record: record, createdAt: now, availableAt: now,
		}
	}
	return publications, nil
}

func (store *MemoryStore) addTaskPublications(publications map[string]*memoryTaskPublication) error {
	for taskID := range publications {
		if _, exists := store.taskPublications[taskID]; exists {
			return fmt.Errorf("task publication already exists for task %q", taskID)
		}
	}
	for taskID, publication := range publications {
		store.taskPublications[taskID] = publication
	}
	return nil
}

func (store *MemoryStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	instance, ok := store.instances[workflowID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	return cloneInstance(instance), nil
}

func (store *MemoryStore) Append(workflowID string, event domain.Event) error {
	return store.AppendMany(workflowID, []domain.Event{event})
}

func (store *MemoryStore) AppendMany(workflowID string, events []domain.Event) error {
	if len(events) == 0 {
		return fmt.Errorf("at least one workflow event is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	instance, ok := store.instances[workflowID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	publications, err := prepareMemoryTaskPublications(events)
	if err != nil {
		return err
	}
	updated := cloneInstance(instance)
	for _, event := range events {
		if err := updated.Append(event); err != nil {
			return err
		}
	}
	if err := store.addTaskPublications(publications); err != nil {
		return err
	}
	store.instances[workflowID] = updated
	return nil
}

func (store *MemoryStore) Claim(ctx context.Context, candidate lease.Lease, duration time.Duration) (lease.Lease, error) {
	if err := ctx.Err(); err != nil {
		return lease.Lease{}, err
	}
	if err := validateLeaseCandidate(candidate, duration); err != nil {
		return lease.Lease{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, current := range store.leases {
		if current.Item.WorkflowID == candidate.Item.WorkflowID && current.Item.TaskID == candidate.Item.TaskID && current.State != "COMPLETED" {
			return lease.Lease{}, lease.ErrLeaseConflict
		}
	}
	store.fencingToken++
	candidate.Token = store.fencingToken
	candidate.Item.LeaseToken = candidate.Token
	candidate.ExpiresAt = time.Now().Add(duration).UTC()
	candidate.State = "ACTIVE"
	store.leases[candidate.ID] = candidate
	return candidate, nil
}

func (store *MemoryStore) Renew(ctx context.Context, leaseID string, token uint64, duration time.Duration) (lease.Lease, error) {
	if err := ctx.Err(); err != nil {
		return lease.Lease{}, err
	}
	if leaseID == "" || token == 0 || duration <= 0 {
		return lease.Lease{}, fmt.Errorf("lease ID, token, and positive duration are required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.leases[leaseID]
	if !exists {
		return lease.Lease{}, lease.ErrLeaseNotFound
	}
	if current.Token != token {
		return lease.Lease{}, lease.ErrStaleToken
	}
	if current.State != "ACTIVE" || !current.ExpiresAt.After(time.Now()) {
		return lease.Lease{}, lease.ErrLeaseExpired
	}
	current.ExpiresAt = time.Now().Add(duration).UTC()
	store.leases[leaseID] = current
	return current, nil
}

func (store *MemoryStore) CompleteWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, error) {
	if err := ctx.Err(); err != nil {
		return lease.Lease{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.leases[leaseID]
	if !exists {
		return lease.Lease{}, lease.ErrLeaseNotFound
	}
	if current.Token != token {
		return lease.Lease{}, lease.ErrStaleToken
	}
	if current.State == "COMPLETED" {
		mutation, err := store.newMemoryMutation(current.Item.WorkflowID)
		if err != nil {
			return lease.Lease{}, err
		}
		if operation != nil {
			if err := operation(current, mutation); err != nil {
				return lease.Lease{}, err
			}
		}
		return current, nil
	}
	if current.State != "ACTIVE" || !current.ExpiresAt.After(time.Now()) {
		return lease.Lease{}, lease.ErrLeaseExpired
	}
	mutation, err := store.newMemoryMutation(current.Item.WorkflowID)
	if err != nil {
		return lease.Lease{}, err
	}
	if operation != nil {
		if err := operation(current, mutation); err != nil {
			return lease.Lease{}, err
		}
	}
	if err := store.commitMemoryMutation(mutation); err != nil {
		return lease.Lease{}, err
	}
	current.State = "COMPLETED"
	store.leases[leaseID] = current
	return current, nil
}

func (store *MemoryStore) ExpireWith(ctx context.Context, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) (lease.Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return lease.Lease{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.leases[leaseID]
	if !exists {
		return lease.Lease{}, false, lease.ErrLeaseNotFound
	}
	if current.Token != token {
		return lease.Lease{}, false, lease.ErrStaleToken
	}
	if current.State != "ACTIVE" {
		return lease.Lease{}, false, lease.ErrLeaseExpired
	}
	if current.ExpiresAt.After(time.Now()) {
		return current, false, nil
	}
	mutation, err := store.newMemoryMutation(current.Item.WorkflowID)
	if err != nil {
		return lease.Lease{}, false, err
	}
	if operation != nil {
		if err := operation(current, mutation); err != nil {
			return lease.Lease{}, false, err
		}
	}
	if err := store.commitMemoryMutation(mutation); err != nil {
		return lease.Lease{}, false, err
	}
	current.State = "EXPIRED"
	store.leases[leaseID] = current
	return current, true, nil
}

func (store *MemoryStore) ListExpired(ctx context.Context, limit int) ([]lease.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, fmt.Errorf("expired lease limit must be positive")
	}
	now := time.Now()
	store.mu.RLock()
	defer store.mu.RUnlock()
	expired := make([]lease.Lease, 0)
	for _, current := range store.leases {
		if current.State == "ACTIVE" && !current.ExpiresAt.After(now) {
			expired = append(expired, current)
		}
	}
	sort.Slice(expired, func(left, right int) bool {
		if expired[left].ExpiresAt.Equal(expired[right].ExpiresAt) {
			return expired[left].ID < expired[right].ID
		}
		return expired[left].ExpiresAt.Before(expired[right].ExpiresAt)
	})
	if len(expired) > limit {
		expired = expired[:limit]
	}
	return expired, nil
}

type memoryMutationStore struct {
	instance     *domain.WorkflowInstance
	publications map[string]*memoryTaskPublication
}

func (store *MemoryStore) newMemoryMutation(workflowID string) (*memoryMutationStore, error) {
	instance, exists := store.instances[workflowID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	return &memoryMutationStore{instance: cloneInstance(instance), publications: make(map[string]*memoryTaskPublication)}, nil
}

func (mutation *memoryMutationStore) Get(workflowID string) (*domain.WorkflowInstance, error) {
	if mutation.instance == nil || mutation.instance.ID != workflowID {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	return cloneInstance(mutation.instance), nil
}

func (mutation *memoryMutationStore) Append(workflowID string, event domain.Event) error {
	return mutation.AppendMany(workflowID, []domain.Event{event})
}

func (mutation *memoryMutationStore) AppendMany(workflowID string, events []domain.Event) error {
	if mutation.instance == nil || mutation.instance.ID != workflowID {
		return fmt.Errorf("%w: %s", ErrNotFound, workflowID)
	}
	if len(events) == 0 {
		return fmt.Errorf("at least one workflow event is required")
	}
	publications, err := prepareMemoryTaskPublications(events)
	if err != nil {
		return err
	}
	updated := cloneInstance(mutation.instance)
	for _, event := range events {
		if err := updated.Append(event); err != nil {
			return err
		}
	}
	for taskID, publication := range publications {
		if _, exists := mutation.publications[taskID]; exists {
			return fmt.Errorf("task publication already exists for task %q", taskID)
		}
		mutation.publications[taskID] = publication
	}
	mutation.instance = updated
	return nil
}

func (store *MemoryStore) commitMemoryMutation(mutation *memoryMutationStore) error {
	for taskID := range mutation.publications {
		if _, exists := store.taskPublications[taskID]; exists {
			return fmt.Errorf("task publication already exists for task %q", taskID)
		}
	}
	for taskID, publication := range mutation.publications {
		store.taskPublications[taskID] = publication
	}
	store.instances[mutation.instance.ID] = mutation.instance
	return nil
}

func validateMemoryLease(current lease.Lease, token uint64, now time.Time) error {
	if current.Token != token {
		return lease.ErrStaleToken
	}
	if current.State == "COMPLETED" {
		return nil
	}
	if current.State != "ACTIVE" || !current.ExpiresAt.After(now) {
		return lease.ErrLeaseExpired
	}
	return nil
}

func (store *MemoryStore) ClaimTaskPublications(ctx context.Context, limit int, leaseDuration time.Duration) ([]TaskPublicationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || leaseDuration <= 0 {
		return nil, fmt.Errorf("task publication claim limit and lease duration must be positive")
	}
	now := time.Now().UTC()
	claimToken := domain.NewID("task-publication-claim")
	store.mu.Lock()
	defer store.mu.Unlock()
	pending := make([]*memoryTaskPublication, 0)
	for _, publication := range store.taskPublications {
		if publication.publishedAt.IsZero() && !publication.availableAt.After(now) && !publication.claimedUntil.After(now) {
			pending = append(pending, publication)
		}
	}
	sort.Slice(pending, func(left, right int) bool {
		if pending[left].createdAt.Equal(pending[right].createdAt) {
			return pending[left].record.Item.TaskID < pending[right].record.Item.TaskID
		}
		return pending[left].createdAt.Before(pending[right].createdAt)
	})
	if len(pending) > limit {
		pending = pending[:limit]
	}
	records := make([]TaskPublicationRecord, 0, len(pending))
	for _, publication := range pending {
		publication.claimedUntil = now.Add(leaseDuration)
		publication.record.ClaimToken = claimToken
		record := publication.record
		record.ClaimToken = claimToken
		record.Item.Payload = copyMap(record.Item.Payload)
		records = append(records, record)
	}
	return records, nil
}

func (store *MemoryStore) MarkTaskPublished(ctx context.Context, record TaskPublicationRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	publication, exists := store.taskPublications[record.Item.TaskID]
	if !exists || !publication.publishedAt.IsZero() || publication.record.ClaimToken != record.ClaimToken {
		return ErrConflict
	}
	publication.publishedAt = time.Now().UTC()
	publication.claimedUntil = time.Time{}
	publication.record.ClaimToken = ""
	return nil
}

func (store *MemoryStore) RetryTaskPublication(ctx context.Context, record TaskPublicationRecord, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("task publication retry delay cannot be negative")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	publication, exists := store.taskPublications[record.Item.TaskID]
	if !exists || !publication.publishedAt.IsZero() || publication.record.ClaimToken != record.ClaimToken {
		return ErrConflict
	}
	publication.availableAt = time.Now().UTC().Add(delay)
	publication.claimedUntil = time.Time{}
	publication.record.ClaimToken = ""
	return nil
}

func (store *MemoryStore) ListExecutions(ctx context.Context, filter ExecutionFilter) ([]*domain.WorkflowInstance, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if filter.Limit < 1 || filter.Offset < 0 {
		return nil, 0, fmt.Errorf("execution limit must be positive and offset cannot be negative")
	}
	store.mu.RLock()
	instances := make([]*domain.WorkflowInstance, 0, len(store.instances))
	for _, instance := range store.instances {
		if filter.WorkflowName != "" && instance.Definition.Name != filter.WorkflowName {
			continue
		}
		if filter.Status != "" && instance.Status != filter.Status {
			continue
		}
		instances = append(instances, cloneInstance(instance))
	}
	store.mu.RUnlock()
	sort.Slice(instances, func(left, right int) bool {
		leftUpdated := executionUpdatedAt(instances[left])
		rightUpdated := executionUpdatedAt(instances[right])
		if leftUpdated.Equal(rightUpdated) {
			return instances[left].ID > instances[right].ID
		}
		return leftUpdated.After(rightUpdated)
	})
	total := len(instances)
	if filter.Offset >= total {
		return []*domain.WorkflowInstance{}, total, nil
	}
	end := filter.Offset + filter.Limit
	if end > total {
		end = total
	}
	return instances[filter.Offset:end], total, nil
}

func (store *MemoryStore) CountExecutions(ctx context.Context) (map[domain.WorkflowStatus]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	counts := make(map[domain.WorkflowStatus]int)
	store.mu.RLock()
	defer store.mu.RUnlock()
	for _, instance := range store.instances {
		counts[instance.Status]++
	}
	return counts, nil
}

func executionUpdatedAt(instance *domain.WorkflowInstance) time.Time {
	if len(instance.Events) == 0 {
		return time.Time{}
	}
	return instance.Events[len(instance.Events)-1].OccurredAt
}

func cloneInstance(source *domain.WorkflowInstance) *domain.WorkflowInstance {
	clone := *source
	clone.Context = copyMap(source.Context)
	clone.Result = copyMap(source.Result)
	clone.Tasks = make(map[string]domain.TaskInstance, len(source.Tasks))
	for key, task := range source.Tasks {
		task.Result = copyMap(task.Result)
		task.Input = copyMap(task.Input)
		clone.Tasks[key] = task
	}
	clone.Events = make([]domain.Event, len(source.Events))
	for index, event := range source.Events {
		event.Payload = copyMap(event.Payload)
		clone.Events[index] = event
	}
	clone.Definition.Nodes = make(map[string]domain.NodeDefinition, len(source.Definition.Nodes))
	for key, node := range source.Definition.Nodes {
		clone.Definition.Nodes[key] = cloneNodeDefinition(node)
	}
	clone.RuntimeNodes = make(map[string]domain.NodeDefinition, len(source.RuntimeNodes))
	for key, node := range source.RuntimeNodes {
		clone.RuntimeNodes[key] = cloneNodeDefinition(node)
	}
	clone.FanOuts = make(map[string]domain.FanOutExecution, len(source.FanOuts))
	for key, state := range source.FanOuts {
		state.Items = append([]domain.FanOutItemExecution(nil), state.Items...)
		for index := range state.Items {
			state.Items[index].NodeIDs = append([]string(nil), state.Items[index].NodeIDs...)
			state.Items[index].Leaves = append([]string(nil), state.Items[index].Leaves...)
		}
		clone.FanOuts[key] = state
	}
	return &clone
}

func copyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = cloneValue(value)
	}
	return copy
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return copyMap(typed)
	case []any:
		copy := make([]any, len(typed))
		for index, item := range typed {
			copy[index] = cloneValue(item)
		}
		return copy
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}

func cloneNodeDefinition(node domain.NodeDefinition) domain.NodeDefinition {
	clone := node
	clone.DependsOn = append([]string(nil), node.DependsOn...)
	clone.Next = append([]string(nil), node.Next...)
	clone.Branches = append([]string(nil), node.Branches...)
	clone.ThenNodes = append([]string(nil), node.ThenNodes...)
	clone.ElseNodes = append([]string(nil), node.ElseNodes...)
	clone.FanOutLeaves = append([]string(nil), node.FanOutLeaves...)
	clone.FanOutAncestors = append([]domain.FanOutItemRef(nil), node.FanOutAncestors...)
	clone.RuntimeScope = copyMap(node.RuntimeScope)
	clone.FanOutTemplates = make([]domain.NodeDefinition, len(node.FanOutTemplates))
	for index, template := range node.FanOutTemplates {
		clone.FanOutTemplates[index] = cloneNodeDefinition(template)
	}
	if node.Task != nil {
		task := *node.Task
		clone.Task = &task
	}
	if node.Compensation != nil {
		compensation := *node.Compensation
		clone.Compensation = &compensation
	}
	clone.Input = cloneExpression(node.Input)
	clone.CompensationInput = cloneExpression(node.CompensationInput)
	clone.Condition = cloneExpression(node.Condition)
	return clone
}

func cloneExpression(expression *domain.Expression) *domain.Expression {
	if expression == nil {
		return nil
	}
	clone := *expression
	clone.Value = cloneValue(expression.Value)
	clone.Object = cloneExpression(expression.Object)
	clone.Left = cloneExpression(expression.Left)
	clone.Right = cloneExpression(expression.Right)
	clone.Fields = append([]domain.ExpressionField(nil), expression.Fields...)
	for index := range clone.Fields {
		clone.Fields[index].Value = *cloneExpression(&expression.Fields[index].Value)
	}
	return &clone
}
