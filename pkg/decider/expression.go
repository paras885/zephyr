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
	case domain.ExpressionString, domain.ExpressionBoolean:
		return expression.Value, nil
	case domain.ExpressionNumber:
		text, ok := expression.Value.(string)
		if !ok {
			return nil, fmt.Errorf("number expression has invalid value %T", expression.Value)
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("parse number %q: %w", text, err)
		}
		return value, nil
	case domain.ExpressionMember:
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
	case domain.ExpressionObject:
		object := make(map[string]any, len(expression.Fields))
		for _, field := range expression.Fields {
			value, err := evaluateExpression(&field.Value, instance, scope)
			if err != nil {
				return nil, fmt.Errorf("evaluate field %q: %w", field.Name, err)
			}
			object[field.Name] = value
		}
		return object, nil
	case domain.ExpressionUnary:
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
	case domain.ExpressionBinary:
		return evaluateBinary(expression, instance, scope)
	default:
		return nil, fmt.Errorf("unsupported expression kind %q", expression.Kind)
	}
}

func evaluateBinary(expression *domain.Expression, instance *domain.WorkflowInstance, scope map[string]any) (any, error) {
	left, err := evaluateExpression(expression.Left, instance, scope)
	if err != nil {
		return nil, err
	}
	if expression.Operator == "&&" || expression.Operator == "||" {
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
	right, err := evaluateExpression(expression.Right, instance, scope)
	if err != nil {
		return nil, err
	}
	switch expression.Operator {
	case "==":
		if leftNumber, ok := numeric(left); ok {
			if rightNumber, rightOK := numeric(right); rightOK {
				return leftNumber == rightNumber, nil
			}
		}
		return reflect.DeepEqual(left, right), nil
	case "!=":
		equal, err := evaluateBinary(&domain.Expression{Kind: domain.ExpressionBinary, Operator: "==", Left: expression.Left, Right: expression.Right}, instance, scope)
		if err != nil {
			return nil, err
		}
		return !equal.(bool), nil
	case ">", ">=", "<", "<=":
		if leftNumber, ok := numeric(left); ok {
			rightNumber, rightOK := numeric(right)
			if !rightOK {
				return nil, fmt.Errorf("operator %s compares incompatible values", expression.Operator)
			}
			switch expression.Operator {
			case ">":
				return leftNumber > rightNumber, nil
			case ">=":
				return leftNumber >= rightNumber, nil
			case "<":
				return leftNumber < rightNumber, nil
			default:
				return leftNumber <= rightNumber, nil
			}
		}
		leftString, leftOK := left.(string)
		rightString, rightOK := right.(string)
		if !leftOK || !rightOK {
			return nil, fmt.Errorf("operator %s compares incompatible values %T and %T", expression.Operator, left, right)
		}
		switch expression.Operator {
		case ">":
			return leftString > rightString, nil
		case ">=":
			return leftString >= rightString, nil
		case "<":
			return leftString < rightString, nil
		default:
			return leftString <= rightString, nil
		}
	default:
		return nil, fmt.Errorf("unsupported binary operator %q", expression.Operator)
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
