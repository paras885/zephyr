package decider

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func evaluateExpression(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	if expression == nil {
		return nil, fmt.Errorf("expression is required")
	}
	switch expression.Kind {
	case domain.ExpressionIdentifier:
		return evaluateIdentifier(expression, instance, scope)
	case domain.ExpressionString, domain.ExpressionBoolean:
		return expression.Value, nil
	case domain.ExpressionNumber:
		return evaluateNumber(expression)
	case domain.ExpressionMember:
		return evaluateMember(expression, instance, scope)
	case domain.ExpressionObject:
		return evaluateObject(expression, instance, scope)
	case domain.ExpressionUnary:
		return evaluateUnary(expression, instance, scope)
	case domain.ExpressionBinary:
		return evaluateBinary(expression, instance, scope)
	default:
		return nil, fmt.Errorf("unsupported expression kind %q", expression.Kind)
	}
}

func evaluateIdentifier(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	if value, ok := scope[expression.Name]; ok {
		return value, nil
	}
	if expression.Name == "input" {
		return instance.Context, nil
	}
	if task, ok := instance.Tasks[expression.Name]; ok {
		return map[string]any{"result": task.Result, "input": task.Input, "status": string(task.Status)}, nil
	}
	return nil, fmt.Errorf("expression references unavailable value %q", expression.Name)
}

func evaluateNumber(expression *domain.Expression) (any, error) {
	text, ok := expression.Value.(string)
	if !ok {
		return nil, fmt.Errorf("number expression has invalid value %T", expression.Value)
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, fmt.Errorf("parse number %q: %w", text, err)
	}
	return value, nil
}

func evaluateMember(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	object, err := evaluateExpression(expression.Object, instance, scope)
	if err != nil {
		return nil, err
	}
	fields, ok := object.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("cannot read member %q from %T", expression.Name, object)
	}
	value, ok := fields[expression.Name]
	if !ok {
		return nil, fmt.Errorf("value has no member %q", expression.Name)
	}
	return value, nil
}

func evaluateObject(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	object := make(map[string]any, len(expression.Fields))
	for _, field := range expression.Fields {
		value, err := evaluateExpression(&field.Value, instance, scope)
		if err != nil {
			return nil, fmt.Errorf("evaluate field %q: %w", field.Name, err)
		}
		object[field.Name] = value
	}
	return object, nil
}

func evaluateUnary(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	if expression.Operator != "!" || expression.Right == nil {
		return nil, fmt.Errorf("unsupported unary expression %q", expression.Operator)
	}
	value, err := evaluateExpression(expression.Right, instance, scope)
	if err != nil {
		return nil, err
	}
	boolean, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("operator ! requires a boolean, got %T", value)
	}
	return !boolean, nil
}

func evaluateBinary(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	left, err := evaluateExpression(expression.Left, instance, scope)
	if err != nil {
		return nil, err
	}
	if expression.Operator == "&&" || expression.Operator == "||" {
		return evaluateLogicalBinary(expression, instance, scope, left)
	}
	right, err := evaluateExpression(expression.Right, instance, scope)
	if err != nil {
		return nil, err
	}
	switch expression.Operator {
	case "==":
		return equalValues(left, right), nil
	case "!=":
		equal, err := evaluateBinary(&domain.Expression{Kind: domain.ExpressionBinary, Operator: "==", Left: expression.Left, Right: expression.Right}, instance, scope)
		if err != nil {
			return nil, err
		}
		return !equal.(bool), nil
	case ">", ">=", "<", "<=":
		return evaluateComparison(expression.Operator, left, right)
	default:
		return nil, fmt.Errorf("unsupported binary operator %q", expression.Operator)
	}
}

// evaluateLogicalBinary handles short-circuit evaluation for && and ||. The
// right operand is only evaluated when short-circuiting does not apply.
func evaluateLogicalBinary(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any, left any) (any, error) {
	leftBoolean, ok := left.(bool)
	if !ok {
		return nil, fmt.Errorf("operator %s requires boolean operands", expression.Operator)
	}
	if expression.Operator == "&&" && !leftBoolean {
		return false, nil
	}
	if expression.Operator == "||" && leftBoolean {
		return true, nil
	}
	right, err := evaluateExpression(expression.Right, instance, scope)
	if err != nil {
		return nil, err
	}
	rightBoolean, ok := right.(bool)
	if !ok {
		return nil, fmt.Errorf("operator %s requires boolean operands", expression.Operator)
	}
	return rightBoolean, nil
}

func equalValues(left, right any) bool {
	if leftNumber, ok := numeric(left); ok {
		if rightNumber, rightOK := numeric(right); rightOK {
			return leftNumber == rightNumber
		}
	}
	return reflect.DeepEqual(left, right)
}

func evaluateComparison(operator string, left, right any) (any, error) {
	if leftNumber, ok := numeric(left); ok {
		rightNumber, rightOK := numeric(right)
		if !rightOK {
			return nil, fmt.Errorf("operator %s compares incompatible values", operator)
		}
		return compareNumbers(operator, leftNumber, rightNumber), nil
	}
	leftString, leftOK := left.(string)
	rightString, rightOK := right.(string)
	if !leftOK || !rightOK {
		return nil, fmt.Errorf("operator %s compares incompatible values %T and %T", operator, left, right)
	}
	return compareStrings(operator, leftString, rightString), nil
}

func compareNumbers(operator string, left, right float64) bool {
	switch operator {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	default:
		return left <= right
	}
}

func compareStrings(operator, left, right string) bool {
	switch operator {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	default:
		return left <= right
	}
}

func numeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
