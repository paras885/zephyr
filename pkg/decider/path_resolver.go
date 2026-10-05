package decider

import (
	"sort"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func readyNodes(instance *domain.WorkflowInstance) []string {
	ready := make([]string, 0)
	reserved := make(map[string]map[int]bool)
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type != domain.NodeTask {
			continue
		}
		if _, scheduled := instance.Tasks[nodeID]; scheduled {
			continue
		}
		if nodeSkipped(instance, nodeID) {
			continue
		}
		if fanOutItemFailedForNode(instance, node) {
			continue
		}
		if !dependenciesResolved(instance, node.DependsOn) {
			continue
		}
		if canScheduleFanOutNode(instance, node, reserved) {
			ready = append(ready, nodeID)
		}
	}
	sort.Strings(ready)
	return ready
}

func readyDelayNodes(instance *domain.WorkflowInstance) []string {
	ready := make([]string, 0)
	reserved := make(map[string]map[int]bool)
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type != domain.NodeDelay || delayScheduled(instance, nodeID) {
			continue
		}
		if fanOutItemFailedForNode(instance, node) {
			continue
		}
		if dependenciesResolved(instance, node.DependsOn) && canScheduleFanOutNode(instance, node, reserved) {
			ready = append(ready, nodeID)
		}
	}
	sort.Strings(ready)
	return ready
}

func dependenciesResolved(instance *domain.WorkflowInstance, dependencies []string) bool {
	for _, dependency := range dependencies {
		if !nodeResolved(instance, dependency, map[string]bool{}) {
			return false
		}
	}
	return true
}

func nodeResolved(instance *domain.WorkflowInstance, nodeID string, visiting map[string]bool) bool {
	if visiting[nodeID] {
		return false
	}
	visiting[nodeID] = true
	node, ok := lookupNode(instance, nodeID)
	if !ok {
		return false
	}
	if nodeSkipped(instance, nodeID) {
		return true
	}
	if node.Type == domain.NodeSwitch {
		return switchWasEvaluated(instance, nodeID)
	}
	if node.Type == domain.NodeFail {
		return failureWasRequested(instance, nodeID) || failNodeTriggeredInFanOut(instance, nodeID)
	}
	if node.Type == domain.NodeReturn {
		return instance.Status == domain.WorkflowCompleted
	}
	if fanOutItemFailedForNode(instance, node) {
		return true
	}
	if node.Type == domain.NodeFanOut || node.JoinFanOutID != "" {
		fanOutID := nodeID
		if node.JoinFanOutID != "" {
			fanOutID = node.JoinFanOutID
		}
		return fanOutComplete(instance, fanOutID)
	}
	if node.Type == domain.NodeTask {
		task, exists := instance.Tasks[nodeID]
		return exists && task.Status == domain.TaskCompleted
	}
	if node.Type == domain.NodeDelay {
		return delayCompleted(instance, nodeID)
	}
	return dependenciesResolvedWithVisit(instance, node.DependsOn, visiting)
}

func dependenciesResolvedWithVisit(instance *domain.WorkflowInstance, dependencies []string, visiting map[string]bool) bool {
	for _, dependency := range dependencies {
		if !nodeResolved(instance, dependency, visiting) {
			return false
		}
	}
	return true
}

func allTasksComplete(instance *domain.WorkflowInstance) bool {
	for nodeID, node := range workflowNodes(instance) {
		if node.Type != domain.NodeTask {
			continue
		}
		if nodeSkipped(instance, nodeID) {
			continue
		}
		task, ok := instance.Tasks[nodeID]
		if !ok || task.Status != domain.TaskCompleted {
			return false
		}
	}
	return true
}

func allNodesComplete(instance *domain.WorkflowInstance) bool {
	if !allTasksComplete(instance) {
		return false
	}
	for nodeID, node := range workflowNodes(instance) {
		if node.Type == domain.NodeDelay && !delayCompleted(instance, nodeID) && !nodeSkipped(instance, nodeID) {
			return false
		}
		if node.Type == domain.NodeSwitch && !switchWasEvaluated(instance, nodeID) && !nodeSkipped(instance, nodeID) {
			return false
		}
		if node.Type == domain.NodeFail && !failureWasRequested(instance, nodeID) && !nodeSkipped(instance, nodeID) {
			return false
		}
		if node.Type == domain.NodeReturn && instance.Status != domain.WorkflowCompleted && !nodeSkipped(instance, nodeID) {
			return false
		}
		if node.Type == domain.NodeFanOut && !nodeSkipped(instance, nodeID) && !fanOutComplete(instance, nodeID) {
			return false
		}
	}
	return true
}

func fanOutFailureReady(instance *domain.WorkflowInstance) (bool, string) {
	stopped := make([]string, 0)
	for fanOutID, state := range instance.FanOuts {
		if state.Stopped {
			stopped = append(stopped, fanOutID)
		}
	}
	if len(stopped) == 0 {
		return false, ""
	}
	sort.Strings(stopped)
	for _, fanOutID := range stopped {
		if activeFanOutItems(instance, fanOutID) > 0 {
			return false, ""
		}
	}
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady || task.Status == domain.TaskRunning || task.Status == domain.TaskRetrying {
			return false, ""
		}
	}
	return true, stopped[0]
}

func workflowFailureRequested(instance *domain.WorkflowInstance) bool {
	for _, event := range instance.Events {
		if event.Type == domain.EventWorkflowFailureRequested {
			return true
		}
	}
	return false
}

func hasActiveTaskWork(instance *domain.WorkflowInstance) bool {
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady || task.Status == domain.TaskRunning || task.Status == domain.TaskRetrying {
			return true
		}
	}
	return false
}

func workflowNodes(instance *domain.WorkflowInstance) map[string]domain.NodeDefinition {
	nodes := make(map[string]domain.NodeDefinition, len(instance.Definition.Nodes)+len(instance.RuntimeNodes))
	for id, node := range instance.Definition.Nodes {
		nodes[id] = node
	}
	for id, node := range instance.RuntimeNodes {
		nodes[id] = node
	}
	return nodes
}

func lookupNode(instance *domain.WorkflowInstance, nodeID string) (domain.NodeDefinition, bool) {
	if node, ok := instance.RuntimeNodes[nodeID]; ok {
		return node, true
	}
	node, ok := instance.Definition.Nodes[nodeID]
	return node, ok
}

func readyFanOutNodes(instance *domain.WorkflowInstance) []string {
	var ready []string
	reserved := make(map[string]map[int]bool)
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type != domain.NodeFanOut || instance.FanOuts[nodeID].Expanded || nodeSkipped(instance, nodeID) || fanOutItemFailedForNode(instance, node) {
			continue
		}
		if dependenciesResolved(instance, node.DependsOn) && canScheduleFanOutNode(instance, node, reserved) {
			ready = append(ready, nodeID)
		}
	}
	sort.Strings(ready)
	return ready
}

func canScheduleFanOutNode(instance *domain.WorkflowInstance, node domain.NodeDefinition, reserved map[string]map[int]bool) bool {
	refs := append([]domain.FanOutItemRef(nil), node.FanOutAncestors...)
	if node.FanOutID != "" {
		refs = append(refs, domain.FanOutItemRef{FanOutID: node.FanOutID, Index: node.FanOutIndex})
	}
	seen := make(map[domain.FanOutItemRef]bool, len(refs))
	for _, ref := range refs {
		if seen[ref] {
			continue
		}
		seen[ref] = true
		state := instance.FanOuts[ref.FanOutID]
		if state.Stopped && !fanOutItemStarted(instance, ref.FanOutID, ref.Index) {
			return false
		}
		if fanOutItemStarted(instance, ref.FanOutID, ref.Index) {
			continue
		}
		if reserved[ref.FanOutID] != nil && reserved[ref.FanOutID][ref.Index] {
			continue
		}
		fanOut := lookupNodeOrZero(instance, ref.FanOutID)
		if activeFanOutItems(instance, ref.FanOutID)+len(reserved[ref.FanOutID]) >= effectiveFanOutConcurrency(fanOut) {
			return false
		}
		if reserved[ref.FanOutID] == nil {
			reserved[ref.FanOutID] = make(map[int]bool)
		}
		reserved[ref.FanOutID][ref.Index] = true
	}
	return true
}

func effectiveFanOutConcurrency(node domain.NodeDefinition) int {
	if node.FanOutConcurrency < 1 {
		return 8
	}
	return node.FanOutConcurrency
}

func activeFanOutItems(instance *domain.WorkflowInstance, fanOutID string) int {
	state := instance.FanOuts[fanOutID]
	active := 0
	for _, item := range state.Items {
		if !item.Started {
			continue
		}
		if item.Failed {
			if fanOutItemHasActiveTasks(instance, fanOutID, item.Index) {
				active++
			}
			continue
		}
		if !fanOutItemComplete(instance, fanOutID, item) {
			active++
		}
	}
	return active
}

func fanOutItemHasActiveTasks(instance *domain.WorkflowInstance, fanOutID string, itemIndex int) bool {
	for _, task := range instance.Tasks {
		if task.Status != domain.TaskReady && task.Status != domain.TaskRunning && task.Status != domain.TaskRetrying {
			continue
		}
		if taskBelongsToFanOutItem(task, fanOutID, itemIndex) {
			return true
		}
	}
	return false
}

func taskBelongsToFanOutItem(task domain.TaskInstance, fanOutID string, itemIndex int) bool {
	if task.FanOutID == fanOutID && task.FanOutIndex == itemIndex {
		return true
	}
	for _, ancestor := range task.FanOutAncestors {
		if ancestor.FanOutID == fanOutID && ancestor.Index == itemIndex {
			return true
		}
	}
	return false
}

func fanOutItemFailedForNode(instance *domain.WorkflowInstance, node domain.NodeDefinition) bool {
	refs := append([]domain.FanOutItemRef(nil), node.FanOutAncestors...)
	if node.FanOutID != "" {
		refs = append(refs, domain.FanOutItemRef{FanOutID: node.FanOutID, Index: node.FanOutIndex})
	}
	for _, ref := range refs {
		for _, item := range instance.FanOuts[ref.FanOutID].Items {
			if item.Index == ref.Index && item.Failed {
				return true
			}
		}
	}
	return false
}

func failNodeTriggeredInFanOut(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.Type == domain.EventFanOutItemFailed && event.Payload["failed_node_id"] == nodeID {
			return true
		}
	}
	return false
}

func fanOutItemStarted(instance *domain.WorkflowInstance, fanOutID string, itemIndex int) bool {
	for _, item := range instance.FanOuts[fanOutID].Items {
		if item.Index == itemIndex {
			return item.Started
		}
	}
	return false
}

func fanOutItemComplete(instance *domain.WorkflowInstance, fanOutID string, item domain.FanOutItemExecution) bool {
	for _, leaf := range item.Leaves {
		if !nodeResolved(instance, leaf, map[string]bool{}) {
			return false
		}
	}
	return true
}

func fanOutComplete(instance *domain.WorkflowInstance, fanOutID string) bool {
	state, ok := instance.FanOuts[fanOutID]
	if !ok || !state.Expanded {
		return false
	}
	for _, item := range state.Items {
		if item.Failed || !fanOutItemComplete(instance, fanOutID, item) {
			return false
		}
	}
	return true
}

func lookupNodeOrZero(instance *domain.WorkflowInstance, nodeID string) domain.NodeDefinition {
	node, _ := lookupNode(instance, nodeID)
	return node
}

func sortedNodeIDs(nodes map[string]domain.NodeDefinition) []string {
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	return nodeIDs
}

func readySwitch(instance *domain.WorkflowInstance) string {
	nodeIDs := make([]string, 0)
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type == domain.NodeSwitch && !switchWasEvaluated(instance, nodeID) && !nodeSkipped(instance, nodeID) && dependenciesResolved(instance, node.DependsOn) {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Strings(nodeIDs)
	if len(nodeIDs) == 0 {
		return ""
	}
	return nodeIDs[0]
}

func readyFailNode(instance *domain.WorkflowInstance) string {
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type == domain.NodeFail && !failureWasRequested(instance, nodeID) && !nodeSkipped(instance, nodeID) && dependenciesResolved(instance, node.DependsOn) {
			return nodeID
		}
	}
	return ""
}

func readyReturnNode(instance *domain.WorkflowInstance) string {
	if instance.FailureReason != "" {
		return ""
	}
	nodes := workflowNodes(instance)
	for _, nodeID := range sortedNodeIDs(nodes) {
		node := nodes[nodeID]
		if node.Type == domain.NodeReturn && !nodeSkipped(instance, nodeID) && dependenciesResolved(instance, node.DependsOn) {
			return nodeID
		}
	}
	return ""
}

func switchWasEvaluated(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.Type == domain.EventSwitchEvaluated && event.NodeID == nodeID {
			return true
		}
	}
	return false
}

func failureWasRequested(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.Type == domain.EventWorkflowFailureRequested && event.NodeID == nodeID {
			return true
		}
	}
	return false
}

func nodeSkipped(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.Type == domain.EventNodeSkipped && event.NodeID == nodeID {
			return true
		}
	}
	return false
}

func delayScheduled(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.NodeID == nodeID && (event.Type == domain.EventDelayScheduled || event.Type == domain.EventDelayCompleted) {
			return true
		}
	}
	return false
}

func delayCompleted(instance *domain.WorkflowInstance, nodeID string) bool {
	for _, event := range instance.Events {
		if event.NodeID == nodeID && event.Type == domain.EventDelayCompleted {
			return true
		}
	}
	return false
}

func compensationNodes(instance *domain.WorkflowInstance) []string {
	completed := make([]string, 0)
	for index := len(instance.Events) - 1; index >= 0; index-- {
		event := instance.Events[index]
		if event.Type != domain.EventTaskCompleted {
			continue
		}
		if node, ok := lookupNode(instance, event.NodeID); ok && node.Compensation != nil {
			completed = append(completed, event.NodeID)
		}
	}
	return completed
}
