package decider

import (
	"fmt"
	"reflect"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func (decider *Decider) expandFanOut(instance *domain.WorkflowInstance, fanOutID string) error {
	fanOutNode, ok := lookupNode(instance, fanOutID)
	if !ok || fanOutNode.Type != domain.NodeFanOut {
		return fmt.Errorf("fan-out node %q not found", fanOutID)
	}
	value, err := evaluateExpression(fanOutNode.FanOutCollection, instance, fanOutNode.RuntimeScope)
	if err != nil {
		return decider.failNode(instance, fanOutID, fmt.Errorf("evaluate fan-out collection for %q: %w", fanOutID, err))
	}
	items, err := collectionItems(value)
	if err != nil {
		return decider.failNode(instance, fanOutID, fmt.Errorf("fan-out collection for %q: %w", fanOutID, err))
	}
	runtimeNodes := make(map[string]domain.NodeDefinition)
	itemStates := make([]domain.FanOutItemExecution, 0, len(items))
	for itemIndex, itemValue := range items {
		cloned, leaves, nodeIDs, err := cloneFanOutItem(fanOutNode, fanOutID, itemIndex, itemValue)
		if err != nil {
			return decider.failNode(instance, fanOutID, fmt.Errorf("expand fan-out %q item %d: %w", fanOutID, itemIndex, err))
		}
		for nodeID, node := range cloned {
			runtimeNodes[nodeID] = node
		}
		itemStates = append(itemStates, domain.FanOutItemExecution{Index: itemIndex, NodeIDs: nodeIDs, Leaves: leaves})
	}
	payload := map[string]any{"runtime_nodes": runtimeNodes, "items": itemStates, "concurrency": effectiveFanOutConcurrency(fanOutNode)}
	if fanOutNode.FanOutID != "" {
		payload["parent_fan_out_id"] = fanOutNode.FanOutID
		payload["parent_fan_out_index"] = fanOutNode.FanOutIndex
	}
	payload["parent_fan_out_ancestors"] = fanOutNode.FanOutAncestors
	return decider.store.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID,
		NodeID:     fanOutID,
		Type:       domain.EventFanOutExpanded,
		Payload:    payload,
	})
}

func cloneFanOutItem(fanOutNode domain.NodeDefinition, fanOutID string, itemIndex int, itemValue any) (map[string]domain.NodeDefinition, []string, []string, error) {
	templateNodes := make(map[string]domain.NodeDefinition, len(fanOutNode.FanOutTemplates))
	for _, template := range fanOutNode.FanOutTemplates {
		templateNodes[template.ID] = template
	}
	idMap := make(map[string]string, len(templateNodes))
	for templateID := range templateNodes {
		idMap[templateID] = fmt.Sprintf("%s:%d:%s", fanOutID, itemIndex, templateID)
	}
	scope := cloneScope(fanOutNode.RuntimeScope)
	scope[fanOutNode.FanOutVariable] = itemValue
	ancestors := append([]domain.FanOutItemRef(nil), fanOutNode.FanOutAncestors...)
	if fanOutNode.FanOutID != "" {
		ancestors = append(ancestors, domain.FanOutItemRef{FanOutID: fanOutNode.FanOutID, Index: fanOutNode.FanOutIndex})
	}
	ancestors = append(ancestors, domain.FanOutItemRef{FanOutID: fanOutID, Index: itemIndex})
	clonedNodes := make(map[string]domain.NodeDefinition, len(templateNodes))
	for templateID, template := range templateNodes {
		clone := template
		clone.ID = idMap[templateID]
		clone.DependsOn = remapNodeIDs(template.DependsOn, idMap)
		clone.ThenNodes = remapNodeIDs(template.ThenNodes, idMap)
		clone.ElseNodes = remapNodeIDs(template.ElseNodes, idMap)
		if template.JoinFanOutID != "" {
			clone.JoinFanOutID = idMap[template.JoinFanOutID]
		}
		clone.RuntimeScope = cloneScope(scope)
		clone.FanOutAncestors = append([]domain.FanOutItemRef(nil), ancestors...)
		clone.FanOutID = fanOutID
		clone.FanOutIndex = itemIndex
		clone.ParentFanOutID = fanOutNode.FanOutID
		clone.ParentFanOutIndex = fanOutNode.FanOutIndex
		clonedNodes[clone.ID] = clone
	}
	leaves := remapNodeIDs(fanOutNode.FanOutLeaves, idMap)
	nodeIDs := make([]string, 0, len(clonedNodes))
	for nodeID := range clonedNodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return clonedNodes, leaves, nodeIDs, nil
}

func remapNodeIDs(nodeIDs []string, idMap map[string]string) []string {
	remapped := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if mapped, ok := idMap[nodeID]; ok {
			remapped = append(remapped, mapped)
		} else {
			remapped = append(remapped, nodeID)
		}
	}
	return remapped
}

func cloneScope(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source)+1)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func collectionItems(value any) ([]any, error) {
	if value == nil {
		return []any{}, nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Array && reflected.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected an array or slice, got %T", value)
	}
	items := make([]any, reflected.Len())
	for index := range reflected.Len() {
		items[index] = reflected.Index(index).Interface()
	}
	return items, nil
}
