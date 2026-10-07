package compiler

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/ast"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/parser"
)

func Compile(source string) (domain.WorkflowDef, error) {
	file, err := parser.Parse(source)
	if err != nil {
		return domain.WorkflowDef{}, err
	}
	return CompileFile(file)
}

func CompileFile(file *ast.File) (domain.WorkflowDef, error) {
	if file == nil {
		return domain.WorkflowDef{}, fmt.Errorf("DSL file is required")
	}
	if len(file.Workflows) != 1 {
		return domain.WorkflowDef{}, fmt.Errorf("exactly one workflow is required per compiled definition")
	}
	tasks := make(map[string]ast.TaskDecl, len(file.Tasks))
	for _, task := range file.Tasks {
		if _, exists := tasks[task.Name]; exists {
			return domain.WorkflowDef{}, fmt.Errorf("duplicate task %q", task.Name)
		}
		tasks[task.Name] = task
	}
	if err := validateTypeSystem(file, tasks); err != nil {
		return domain.WorkflowDef{}, err
	}
	workflow := file.Workflows[0]
	if sequenceCanSucceedWithoutReturn(workflow.Body) {
		return domain.WorkflowDef{}, fmt.Errorf("workflow %q has a successful path without return %s", workflow.Name, workflow.Output)
	}
	outputType := workflow.Output
	definition := domain.WorkflowDef{Name: workflow.Name, Version: 1, OutputType: outputType, Nodes: make(map[string]domain.NodeDefinition)}
	builder := graphBuilder{definition: &definition, tasks: tasks, outputType: outputType}
	leaves, err := builder.compileNodes(workflow.Body, nil)
	if err != nil {
		return domain.WorkflowDef{}, err
	}
	if len(leaves) == 0 {
		return domain.WorkflowDef{}, fmt.Errorf("workflow %q has no executable nodes", workflow.Name)
	}
	definition.Start = startNodes(definition.Nodes)
	if err := definition.Validate(); err != nil {
		return domain.WorkflowDef{}, err
	}
	if err := validateAcyclic(definition.Nodes); err != nil {
		return domain.WorkflowDef{}, err
	}
	return definition, nil
}

var primitiveTypes = map[string]bool{
	"string": true, "bool": true, "int": true, "float": true, "duration": true,
}

func validateTypeSystem(file *ast.File, tasks map[string]ast.TaskDecl) error {
	types := make(map[string]map[string]string, len(file.Types))
	for _, declaration := range file.Types {
		if _, exists := types[declaration.Name]; exists {
			return fmt.Errorf("duplicate type %q", declaration.Name)
		}
		fields := make(map[string]string, len(declaration.Fields))
		for _, field := range declaration.Fields {
			if _, exists := fields[field.Name]; exists {
				return fmt.Errorf("type %q has duplicate field %q", declaration.Name, field.Name)
			}
			if !validType(field.Type, types) && !knownDeclaredType(field.Type, file.Types) {
				return fmt.Errorf("type %q field %q uses unknown type %q", declaration.Name, field.Name, field.Type)
			}
			fields[field.Name] = field.Type
		}
		types[declaration.Name] = fields
	}
	for _, declaration := range file.Types {
		for _, field := range declaration.Fields {
			if !validType(field.Type, types) {
				return fmt.Errorf("type %q field %q uses unknown type %q", declaration.Name, field.Name, field.Type)
			}
		}
	}
	for _, task := range tasks {
		_, inputType, ok := strings.Cut(task.Input, ":")
		if !ok || !validType(inputType, types) {
			return fmt.Errorf("task %q uses unknown input type %q", task.Name, inputType)
		}
		if !validType(task.Output, types) {
			return fmt.Errorf("task %q uses unknown output type %q", task.Name, task.Output)
		}
	}
	for _, workflow := range file.Workflows {
		_, inputType, ok := strings.Cut(workflow.Input, ":")
		if !ok || !validType(inputType, types) {
			return fmt.Errorf("workflow %q uses unknown input type %q", workflow.Name, inputType)
		}
		if !validType(workflow.Output, types) {
			return fmt.Errorf("workflow %q uses unknown output type %q", workflow.Name, workflow.Output)
		}
		_, inputType, _ = strings.Cut(workflow.Input, ":")
		if err := validateNodes(workflow.Body, tasks, types, workflow.Output, inputType, make(map[string]string), true); err != nil {
			return fmt.Errorf("workflow %q: %w", workflow.Name, err)
		}
	}
	return nil
}

func validateNodes(nodes []ast.Node, tasks map[string]ast.TaskDecl, types map[string]map[string]string, outputType, inputType string, stepOutputs map[string]string, returnAllowed bool) error {
	for _, node := range nodes {
		switch typed := node.(type) {
		case ast.StepNode:
			if err := validateStepNode(typed, tasks, types, stepOutputs); err != nil {
				return err
			}
		case ast.IfNode:
			if err := validateIfNode(typed, tasks, types, outputType, inputType, stepOutputs, returnAllowed); err != nil {
				return err
			}
		case ast.ForkNode:
			if err := validateForkNode(typed, tasks, types, outputType, inputType, stepOutputs); err != nil {
				return err
			}
		case ast.FanOutNode:
			if err := validateFanOutNode(typed, tasks, types, outputType, inputType, stepOutputs); err != nil {
				return err
			}
		case ast.ReturnNode:
			if err := validateReturnNode(typed, types, outputType, inputType, stepOutputs, returnAllowed); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateStepNode(step ast.StepNode, tasks map[string]ast.TaskDecl, types map[string]map[string]string, stepOutputs map[string]string) error {
	if err := validateCall(step.Call, tasks, types); err != nil {
		return err
	}
	if task, ok := tasks[step.Call.Name]; ok {
		stepOutputs[step.Name] = task.Output
	}
	if step.Compensation != nil {
		if err := validateCall(*step.Compensation, tasks, types); err != nil {
			return err
		}
	}
	return nil
}

func validateIfNode(ifNode ast.IfNode, tasks map[string]ast.TaskDecl, types map[string]map[string]string, outputType, inputType string, stepOutputs map[string]string, returnAllowed bool) error {
	thenOutputs := copyStepOutputs(stepOutputs)
	elseOutputs := copyStepOutputs(stepOutputs)
	if err := validateNodes(ifNode.Then, tasks, types, outputType, inputType, thenOutputs, returnAllowed); err != nil {
		return err
	}
	if err := validateNodes(ifNode.Else, tasks, types, outputType, inputType, elseOutputs, returnAllowed); err != nil {
		return err
	}
	thenFallsThrough := sequenceCanSucceedWithoutReturn(ifNode.Then)
	elseFallsThrough := len(ifNode.Else) == 0 || sequenceCanSucceedWithoutReturn(ifNode.Else)
	switch {
	case thenFallsThrough && !elseFallsThrough:
		replaceStepOutputs(stepOutputs, thenOutputs)
	case elseFallsThrough && !thenFallsThrough:
		replaceStepOutputs(stepOutputs, elseOutputs)
	default:
		for name, typeName := range thenOutputs {
			if elseOutputs[name] != typeName {
				delete(thenOutputs, name)
			}
		}
		replaceStepOutputs(stepOutputs, thenOutputs)
	}
	return nil
}

func validateForkNode(forkNode ast.ForkNode, tasks map[string]ast.TaskDecl, types map[string]map[string]string, outputType, inputType string, stepOutputs map[string]string) error {
	mergedOutputs := copyStepOutputs(stepOutputs)
	for _, branch := range forkNode.Branches {
		branchOutputs := copyStepOutputs(stepOutputs)
		if err := validateNodes(branch, tasks, types, outputType, inputType, branchOutputs, false); err != nil {
			return err
		}
		for name, typeName := range branchOutputs {
			mergedOutputs[name] = typeName
		}
	}
	replaceStepOutputs(stepOutputs, mergedOutputs)
	return nil
}

func validateFanOutNode(fanOut ast.FanOutNode, tasks map[string]ast.TaskDecl, types map[string]map[string]string, outputType, inputType string, stepOutputs map[string]string) error {
	bodyOutputs := copyStepOutputs(stepOutputs)
	return validateNodes(fanOut.Body, tasks, types, outputType, inputType, bodyOutputs, false)
}

func validateReturnNode(returnNode ast.ReturnNode, types map[string]map[string]string, outputType, inputType string, stepOutputs map[string]string, returnAllowed bool) error {
	if !returnAllowed {
		return fmt.Errorf("workflow return cannot appear inside a fork or fan_out block")
	}
	if returnNode.Type != outputType {
		return fmt.Errorf("workflow return type %q does not match declared output %q", returnNode.Type, outputType)
	}
	outputFields := types[outputType]
	seen := make(map[string]bool, len(returnNode.Fields))
	for _, field := range returnNode.Fields {
		if field.Name == "" || field.Value == nil {
			return fmt.Errorf("return field %q is incomplete", field.Name)
		}
		if seen[field.Name] {
			return fmt.Errorf("workflow return has duplicate field %q", field.Name)
		}
		if _, exists := outputFields[field.Name]; !exists {
			return fmt.Errorf("workflow output %q has no field %q", outputType, field.Name)
		}
		if err := validateReturnValueType(field.Value, outputFields[field.Name], types, inputType, stepOutputs); err != nil {
			return fmt.Errorf("workflow return field %q: %w", field.Name, err)
		}
		seen[field.Name] = true
	}
	for fieldName := range outputFields {
		if !seen[fieldName] {
			return fmt.Errorf("workflow return is missing output field %q", fieldName)
		}
	}
	return nil
}

func copyStepOutputs(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for name, typeName := range source {
		clone[name] = typeName
	}
	return clone
}

func replaceStepOutputs(target, source map[string]string) {
	for name := range target {
		delete(target, name)
	}
	for name, typeName := range source {
		target[name] = typeName
	}
}

func validateReturnValueType(expression ast.Expression, expected string, types map[string]map[string]string, inputType string, stepOutputs map[string]string) error {
	if object, ok := expression.(ast.ObjectExpr); ok {
		fields, declared := types[expected]
		if !declared {
			return fmt.Errorf("type %q is not an object type", expected)
		}
		seen := make(map[string]bool, len(object.Fields))
		for _, field := range object.Fields {
			fieldType, exists := fields[field.Name]
			if !exists {
				return fmt.Errorf("output type %q has no field %q", expected, field.Name)
			}
			if seen[field.Name] {
				return fmt.Errorf("object has duplicate field %q", field.Name)
			}
			seen[field.Name] = true
			if err := validateReturnValueType(field.Value, fieldType, types, inputType, stepOutputs); err != nil {
				return fmt.Errorf("field %q: %w", field.Name, err)
			}
		}
		for fieldName := range fields {
			if !seen[fieldName] {
				return fmt.Errorf("object is missing field %q", fieldName)
			}
		}
		return nil
	}
	actual, err := inferReturnValueType(expression, types, inputType, stepOutputs)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("expression has type %q, want %q", actual, expected)
	}
	return nil
}

func inferReturnValueType(expression ast.Expression, types map[string]map[string]string, inputType string, stepOutputs map[string]string) (string, error) {
	switch typed := expression.(type) {
	case ast.StringExpr:
		return "string", nil
	case ast.BoolExpr:
		return "bool", nil
	case ast.DurationExpr:
		return "duration", nil
	case ast.NumberExpr:
		if strings.ContainsAny(typed.Value, ".eE") {
			return "float", nil
		}
		return "int", nil
	case ast.IdentifierExpr:
		if typed.Name == "input" {
			return inputType, nil
		}
		return "", fmt.Errorf("unresolved return identifier %q", typed.Name)
	case ast.UnaryExpr:
		if typed.Operator != "!" {
			return "", fmt.Errorf("unsupported unary return operator %q", typed.Operator)
		}
		operandType, err := inferReturnValueType(typed.Operand, types, inputType, stepOutputs)
		if err != nil {
			return "", err
		}
		if operandType != "bool" {
			return "", fmt.Errorf("operator ! requires bool, got %q", operandType)
		}
		return "bool", nil
	case ast.MemberExpr:
		return inferMemberReturnType(typed, types, inputType, stepOutputs)
	case ast.BinaryExpr:
		return inferBinaryReturnType(typed, types, inputType, stepOutputs)
	default:
		return "", fmt.Errorf("unsupported return expression %T", expression)
	}
}

func inferMemberReturnType(member ast.MemberExpr, types map[string]map[string]string, inputType string, stepOutputs map[string]string) (string, error) {
	path, ok := expressionPath(member)
	if !ok || len(path) < 2 {
		return "", fmt.Errorf("unsupported return member expression")
	}
	currentType := ""
	fields := path[1:]
	if path[0] == "input" {
		currentType = inputType
	} else if outputType, exists := stepOutputs[path[0]]; exists && path[1] == "result" {
		currentType = outputType
		fields = path[2:]
	} else {
		return "", fmt.Errorf("unresolved return path %q", strings.Join(path, "."))
	}
	for _, fieldName := range fields {
		fieldType, exists := types[currentType][fieldName]
		if !exists {
			return "", fmt.Errorf("type %q has no field %q in return path %q", currentType, fieldName, strings.Join(path, "."))
		}
		currentType = fieldType
	}
	return currentType, nil
}

func inferBinaryReturnType(binary ast.BinaryExpr, types map[string]map[string]string, inputType string, stepOutputs map[string]string) (string, error) {
	leftType, err := inferReturnValueType(binary.Left, types, inputType, stepOutputs)
	if err != nil {
		return "", err
	}
	rightType, err := inferReturnValueType(binary.Right, types, inputType, stepOutputs)
	if err != nil {
		return "", err
	}
	switch binary.Operator {
	case "&&", "||":
		if leftType != "bool" || rightType != "bool" {
			return "", fmt.Errorf("operator %s requires bool operands, got %q and %q", binary.Operator, leftType, rightType)
		}
	case "==", "!=":
		return "bool", nil
	case ">", ">=", "<", "<=":
		if !orderedReturnTypesCompatible(leftType, rightType) {
			return "", fmt.Errorf("operator %s cannot compare %q and %q", binary.Operator, leftType, rightType)
		}
	default:
		return "", fmt.Errorf("unsupported binary return operator %q", binary.Operator)
	}
	return "bool", nil
}

func orderedReturnTypesCompatible(left, right string) bool {
	if left == "string" || right == "string" {
		return left == "string" && right == "string"
	}
	return numericReturnType(left) && numericReturnType(right)
}

func numericReturnType(typeName string) bool {
	switch typeName {
	case "int", "float", "duration":
		return true
	default:
		return false
	}
}

func expressionPath(expression ast.Expression) ([]string, bool) {
	switch typed := expression.(type) {
	case ast.IdentifierExpr:
		return []string{typed.Name}, true
	case ast.MemberExpr:
		path, ok := expressionPath(typed.Object)
		if !ok {
			return nil, false
		}
		return append(path, typed.Name), true
	default:
		return nil, false
	}
}

func validateCall(call ast.CallExpr, tasks map[string]ast.TaskDecl, types map[string]map[string]string) error {
	task, ok := tasks[call.Name]
	if !ok {
		return fmt.Errorf("workflow references unknown task %q", call.Name)
	}
	_, inputType, _ := strings.Cut(task.Input, ":")
	fields := types[inputType]
	for _, assignment := range call.Fields {
		if _, ok := fields[assignment.Name]; !ok {
			return fmt.Errorf("task %q input has no field %q", call.Name, assignment.Name)
		}
	}
	return nil
}

func validType(typeName string, types map[string]map[string]string) bool {
	if primitiveTypes[typeName] {
		return true
	}
	if strings.HasPrefix(typeName, "list[") && strings.HasSuffix(typeName, "]") {
		return validType(typeName[5:len(typeName)-1], types)
	}
	_, ok := types[typeName]
	return ok
}

func knownDeclaredType(typeName string, declarations []ast.TypeDecl) bool {
	if primitiveTypes[typeName] {
		return true
	}
	for _, declaration := range declarations {
		if declaration.Name == typeName {
			return true
		}
	}
	return strings.HasPrefix(typeName, "list[")
}

type graphBuilder struct {
	definition *domain.WorkflowDef
	tasks      map[string]ast.TaskDecl
	outputType string
	sequence   int
}

func (builder *graphBuilder) compileNodes(nodes []ast.Node, dependencies []string) ([]string, error) {
	leaves := append([]string(nil), dependencies...)
	for _, node := range nodes {
		var err error
		switch typed := node.(type) {
		case ast.StepNode:
			leaves, err = builder.compileStep(typed, leaves)
		case ast.DelayNode:
			leaves, err = builder.compileDelay(typed, leaves)
		case ast.ForkNode:
			leaves, err = builder.compileFork(typed, leaves)
		case ast.FanOutNode:
			leaves, err = builder.compileFanOut(typed, leaves)
		case ast.IfNode:
			leaves, err = builder.compileIf(typed, leaves)
		case ast.FailNode:
			leaves, err = builder.compileFail(typed, leaves)
		case ast.ReturnNode:
			leaves, err = builder.compileReturn(typed, leaves)
		default:
			err = fmt.Errorf("unsupported AST node %T", node)
		}
		if err != nil {
			return nil, err
		}
	}
	return leaves, nil
}

func (builder *graphBuilder) compileReturn(returnNode ast.ReturnNode, dependencies []string) ([]string, error) {
	if returnNode.Type != builder.outputType {
		return nil, fmt.Errorf("workflow return type %q does not match declared output %q", returnNode.Type, builder.outputType)
	}
	value, err := expressionFromAssignments(returnNode.Fields)
	if err != nil {
		return nil, err
	}
	nodeID := builder.generatedID("return")
	if err := builder.addNode(domain.NodeDefinition{ID: nodeID, Type: domain.NodeReturn, DependsOn: copyStrings(dependencies), ReturnValue: value}); err != nil {
		return nil, err
	}
	return []string{nodeID}, nil
}

func sequenceCanSucceedWithoutReturn(nodes []ast.Node) bool {
	canFallThrough := true
	for _, node := range nodes {
		if !canFallThrough {
			return false
		}
		switch typed := node.(type) {
		case ast.ReturnNode, ast.FailNode:
			canFallThrough = false
		case ast.IfNode:
			thenFallsThrough := sequenceCanSucceedWithoutReturn(typed.Then)
			elseFallsThrough := len(typed.Else) == 0 || sequenceCanSucceedWithoutReturn(typed.Else)
			canFallThrough = thenFallsThrough || elseFallsThrough
		}
	}
	return canFallThrough
}

func (builder *graphBuilder) compileFail(failNode ast.FailNode, dependencies []string) ([]string, error) {
	message, err := expressionFromAST(failNode.Message)
	if err != nil {
		return nil, err
	}
	nodeID := builder.generatedID("fail")
	if err := builder.addNode(domain.NodeDefinition{ID: nodeID, Type: domain.NodeFail, DependsOn: copyStrings(dependencies), FailureMessage: message}); err != nil {
		return nil, err
	}
	return []string{nodeID}, nil
}

func (builder *graphBuilder) compileStep(step ast.StepNode, dependencies []string) ([]string, error) {
	task, ok := builder.tasks[step.Call.Name]
	if !ok {
		return nil, fmt.Errorf("workflow references unknown task %q", step.Call.Name)
	}
	node := domain.NodeDefinition{ID: step.Name, Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: task.Name, Retries: task.Retries, Backoff: task.Backoff}, DependsOn: copyStrings(dependencies)}
	input, err := expressionFromAssignments(step.Call.Fields)
	if err != nil {
		return nil, err
	}
	node.Input = input
	if step.Compensation != nil {
		compensation, ok := builder.tasks[step.Compensation.Name]
		if !ok {
			return nil, fmt.Errorf("workflow references unknown compensation task %q", step.Compensation.Name)
		}
		node.Compensation = &domain.TaskDefinition{Name: compensation.Name, Retries: compensation.Retries, Backoff: compensation.Backoff}
		node.CompensationInput, err = expressionFromAssignments(step.Compensation.Fields)
		if err != nil {
			return nil, err
		}
	}
	if err := builder.addNode(node); err != nil {
		return nil, err
	}
	return []string{step.Name}, nil
}

func (builder *graphBuilder) compileDelay(delay ast.DelayNode, dependencies []string) ([]string, error) {
	if err := builder.addNode(domain.NodeDefinition{ID: delay.Name, Type: domain.NodeDelay, Delay: delay.Duration, DependsOn: copyStrings(dependencies)}); err != nil {
		return nil, err
	}
	return []string{delay.Name}, nil
}

func (builder *graphBuilder) compileFork(fork ast.ForkNode, dependencies []string) ([]string, error) {
	forkID := builder.generatedID("fork")
	if err := builder.addNode(domain.NodeDefinition{ID: forkID, Type: domain.NodeFork, DependsOn: copyStrings(dependencies)}); err != nil {
		return nil, err
	}
	leaves := make([]string, 0, len(fork.Branches))
	for _, branch := range fork.Branches {
		branchLeaves, err := builder.compileNodes(branch, []string{forkID})
		if err != nil {
			return nil, err
		}
		leaves = append(leaves, branchLeaves...)
	}
	joinID := builder.generatedID("join")
	if err := builder.addNode(domain.NodeDefinition{ID: joinID, Type: domain.NodeJoin, DependsOn: leaves}); err != nil {
		return nil, err
	}
	return []string{joinID}, nil
}

func (builder *graphBuilder) compileFanOut(fanOut ast.FanOutNode, dependencies []string) ([]string, error) {
	collection, err := expressionFromAST(fanOut.Collection)
	if err != nil {
		return nil, err
	}
	fanOutID := builder.generatedID("fan_out")
	mainDefinition := builder.definition
	templateDefinition := &domain.WorkflowDef{Nodes: make(map[string]domain.NodeDefinition)}
	builder.definition = templateDefinition
	leaves, err := builder.compileNodes(fanOut.Body, nil)
	builder.definition = mainDefinition
	if err != nil {
		return nil, err
	}
	templates := make([]domain.NodeDefinition, 0, len(templateDefinition.Nodes))
	for _, nodeID := range sortedNodeIDs(templateDefinition.Nodes) {
		templates = append(templates, templateDefinition.Nodes[nodeID])
	}
	if err := builder.addNode(domain.NodeDefinition{
		ID: fanOutID, Type: domain.NodeFanOut, DependsOn: copyStrings(dependencies),
		FanOutVariable: fanOut.Variable, FanOutCollection: collection,
		FanOutConcurrency: fanOut.Concurrency, FanOutTemplates: templates, FanOutLeaves: leaves,
	}); err != nil {
		return nil, err
	}
	joinID := builder.generatedID("fan_out_join")
	if err := builder.addNode(domain.NodeDefinition{ID: joinID, Type: domain.NodeJoin, DependsOn: []string{fanOutID}, FanOutID: fanOutID}); err != nil {
		return nil, err
	}
	return []string{joinID}, nil
}

func sortedNodeIDs(nodes map[string]domain.NodeDefinition) []string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (builder *graphBuilder) compileIf(ifNode ast.IfNode, dependencies []string) ([]string, error) {
	switchID := builder.generatedID("switch")
	condition, err := expressionFromAST(ifNode.Condition)
	if err != nil {
		return nil, err
	}
	if err := builder.addNode(domain.NodeDefinition{ID: switchID, Type: domain.NodeSwitch, DependsOn: copyStrings(dependencies), Condition: condition}); err != nil {
		return nil, err
	}
	baseNodes := copyNodes(builder.definition.Nodes)
	thenLeaves, err := builder.compileNodes(ifNode.Then, []string{switchID})
	if err != nil {
		return nil, err
	}
	thenNodes := addedNodeIDs(baseNodes, builder.definition.Nodes)
	baseNodes = copyNodes(builder.definition.Nodes)
	elseLeaves, err := builder.compileNodes(ifNode.Else, []string{switchID})
	if err != nil {
		return nil, err
	}
	elseNodes := addedNodeIDs(baseNodes, builder.definition.Nodes)
	switchNode := builder.definition.Nodes[switchID]
	switchNode.ThenNodes = thenNodes
	switchNode.ElseNodes = elseNodes
	builder.definition.Nodes[switchID] = switchNode
	leaves := append(thenLeaves, elseLeaves...)
	if len(leaves) == 0 {
		return []string{switchID}, nil
	}
	joinID := builder.generatedID("switch_join")
	if err := builder.addNode(domain.NodeDefinition{ID: joinID, Type: domain.NodeJoin, DependsOn: leaves}); err != nil {
		return nil, err
	}
	return []string{joinID}, nil
}

func expressionFromAssignments(assignments []ast.Assignment) (*domain.Expression, error) {
	fields := make([]domain.ExpressionField, 0, len(assignments))
	for _, assignment := range assignments {
		value, err := expressionFromAST(assignment.Value)
		if err != nil {
			return nil, err
		}
		fields = append(fields, domain.ExpressionField{Name: assignment.Name, Value: *value})
	}
	return &domain.Expression{Kind: domain.ExpressionObject, Fields: fields}, nil
}

func expressionFromAST(expression ast.Expression) (*domain.Expression, error) {
	switch typed := expression.(type) {
	case ast.IdentifierExpr:
		return &domain.Expression{Kind: domain.ExpressionIdentifier, Name: typed.Name}, nil
	case ast.StringExpr:
		return &domain.Expression{Kind: domain.ExpressionString, Value: typed.Value}, nil
	case ast.NumberExpr:
		return &domain.Expression{Kind: domain.ExpressionNumber, Value: typed.Value}, nil
	case ast.BoolExpr:
		return &domain.Expression{Kind: domain.ExpressionBoolean, Value: typed.Value}, nil
	case ast.MemberExpr:
		object, err := expressionFromAST(typed.Object)
		if err != nil {
			return nil, err
		}
		return &domain.Expression{Kind: domain.ExpressionMember, Object: object, Name: typed.Name}, nil
	case ast.ObjectExpr:
		return expressionFromAssignments(typed.Fields)
	case ast.BinaryExpr:
		left, err := expressionFromAST(typed.Left)
		if err != nil {
			return nil, err
		}
		right, err := expressionFromAST(typed.Right)
		if err != nil {
			return nil, err
		}
		return &domain.Expression{Kind: domain.ExpressionBinary, Operator: typed.Operator, Left: left, Right: right}, nil
	case ast.UnaryExpr:
		operand, err := expressionFromAST(typed.Operand)
		if err != nil {
			return nil, err
		}
		return &domain.Expression{Kind: domain.ExpressionUnary, Operator: typed.Operator, Right: operand}, nil
	default:
		return nil, fmt.Errorf("unsupported expression %T", expression)
	}
}

func copyNodes(nodes map[string]domain.NodeDefinition) map[string]struct{} {
	copy := make(map[string]struct{}, len(nodes))
	for nodeID := range nodes {
		copy[nodeID] = struct{}{}
	}
	return copy
}

func addedNodeIDs(before map[string]struct{}, after map[string]domain.NodeDefinition) []string {
	added := make([]string, 0)
	for nodeID := range after {
		if _, existed := before[nodeID]; !existed {
			added = append(added, nodeID)
		}
	}
	sort.Strings(added)
	return added
}

func (builder *graphBuilder) addNode(node domain.NodeDefinition) error {
	if _, exists := builder.definition.Nodes[node.ID]; exists {
		return fmt.Errorf("duplicate workflow node %q", node.ID)
	}
	builder.definition.Nodes[node.ID] = node
	return nil
}

func (builder *graphBuilder) generatedID(prefix string) string {
	builder.sequence++
	return fmt.Sprintf("%s_%d", prefix, builder.sequence)
}

func startNodes(nodes map[string]domain.NodeDefinition) []string {
	starts := make([]string, 0)
	for nodeID := range nodes {
		if len(nodes[nodeID].DependsOn) == 0 {
			starts = append(starts, nodeID)
		}
	}
	sort.Strings(starts)
	return starts
}

func validateAcyclic(nodes map[string]domain.NodeDefinition) error {
	visiting := make(map[string]bool)
	visited := make(map[string]bool)
	var visit func(string) error
	visit = func(nodeID string) error {
		if visiting[nodeID] {
			return fmt.Errorf("workflow graph contains a cycle at node %q", nodeID)
		}
		if visited[nodeID] {
			return nil
		}
		visiting[nodeID] = true
		for _, dependency := range nodes[nodeID].DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		delete(visiting, nodeID)
		visited[nodeID] = true
		return nil
	}
	for nodeID := range nodes {
		if err := visit(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func copyStrings(values []string) []string {
	return append([]string(nil), values...)
}
