package ast

import "time"

type File struct {
	Namespace string
	Types     []TypeDecl
	Tasks     []TaskDecl
	Workflows []WorkflowDecl
}

type TypeDecl struct {
	Name   string
	Fields []FieldDecl
}

type FieldDecl struct {
	Name string
	Type string
}

type TaskDecl struct {
	Name    string
	Input   string
	Output  string
	Timeout time.Duration
	Retries int
	Backoff time.Duration
}

type WorkflowDecl struct {
	Name   string
	Input  string
	Output string
	Body   []Node
}

type Node interface{ node() }

type StepNode struct {
	Name         string
	Call         CallExpr
	Compensation *CallExpr
}

func (StepNode) node() {}

type DelayNode struct {
	Name     string
	Duration time.Duration
}

func (DelayNode) node() {}

type IfNode struct {
	Condition Expression
	Then      []Node
	Else      []Node
}

func (IfNode) node() {}

type ForkNode struct {
	Branches [][]Node
}

func (ForkNode) node() {}

type FanOutNode struct {
	Variable    string
	Collection  Expression
	Concurrency int
	Body        []Node
}

func (FanOutNode) node() {}

type ReturnNode struct {
	Type   string
	Fields []Assignment
}

func (ReturnNode) node() {}

type FailNode struct {
	Message Expression
}

func (FailNode) node() {}

type CallExpr struct {
	Name   string
	Fields []Assignment
}

type Assignment struct {
	Name  string
	Value Expression
}

type Expression interface{ expression() }

type IdentifierExpr struct{ Name string }

func (IdentifierExpr) expression() {}

type StringExpr struct{ Value string }

func (StringExpr) expression() {}

type NumberExpr struct{ Value string }

func (NumberExpr) expression() {}

type DurationExpr struct{ Value time.Duration }

func (DurationExpr) expression() {}

type BoolExpr struct{ Value bool }

func (BoolExpr) expression() {}

type MemberExpr struct {
	Object Expression
	Name   string
}

func (MemberExpr) expression() {}

type ObjectExpr struct{ Fields []Assignment }

func (ObjectExpr) expression() {}

type BinaryExpr struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (BinaryExpr) expression() {}

type UnaryExpr struct {
	Operator string
	Operand  Expression
}

func (UnaryExpr) expression() {}
