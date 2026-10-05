package domain

import (
	"fmt"
	"time"
)

type TaskDefinition struct {
	Name         string
	Retries      int
	Backoff      time.Duration
	Compensation *TaskDefinition
}

type NodeDefinition struct {
	ID                string
	Type              NodeType
	Task              *TaskDefinition
	DependsOn         []string
	Next              []string
	Branches          []string
	Delay             time.Duration
	Compensation      *TaskDefinition
	Input             *Expression
	CompensationInput *Expression
	Condition         *Expression
	ThenNodes         []string
	ElseNodes         []string
	FanOutVariable    string
	FanOutCollection  *Expression
	FanOutConcurrency int
	FanOutTemplates   []NodeDefinition
	FanOutLeaves      []string
	FanOutID          string
	FanOutIndex       int
	TemplateID        string
	RuntimeScope      map[string]any
	JoinFanOutID      string
	ParentFanOutID    string
	ParentFanOutIndex int
	FanOutAncestors   []FanOutItemRef
	FailureMessage    *Expression
	ReturnValue       *Expression
}

type FanOutItemRef struct {
	FanOutID string
	Index    int
}

type WorkflowDef struct {
	Name       string
	Version    int
	OutputType string
	Start      []string
	Nodes      map[string]NodeDefinition
}

func (definition WorkflowDef) Validate() error {
	if definition.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if definition.Version < 1 {
		return fmt.Errorf("workflow version must be positive")
	}
	if len(definition.Nodes) == 0 {
		return fmt.Errorf("workflow must define at least one node")
	}
	for nodeID, node := range definition.Nodes {
		if node.ID != "" && node.ID != nodeID {
			return fmt.Errorf("node key %q does not match node ID %q", nodeID, node.ID)
		}
		if node.Type == NodeTask && node.Task == nil {
			return fmt.Errorf("task node %q requires a task definition", nodeID)
		}
		if node.Type == NodeDelay && node.Delay <= 0 {
			return fmt.Errorf("delay node %q requires a positive duration", nodeID)
		}
		for _, dependency := range node.DependsOn {
			if _, ok := definition.Nodes[dependency]; !ok {
				return fmt.Errorf("node %q depends on unknown node %q", nodeID, dependency)
			}
		}
	}
	return nil
}
