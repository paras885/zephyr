package parser

import (
	"fmt"
	"strconv"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/ast"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/lexer"
)

type Parser struct {
	tokens []lexer.Token
	index  int
}

func Parse(source string) (*ast.File, error) {
	tokens, err := lexer.New(source).All()
	if err != nil {
		return nil, err
	}
	return New(tokens).File()
}

func New(tokens []lexer.Token) *Parser { return &Parser{tokens: tokens} }

func (parser *Parser) File() (*ast.File, error) {
	file := &ast.File{}
	if parser.match(lexer.Namespace) {
		name, err := parser.parseQualifiedName()
		if err != nil {
			return nil, err
		}
		file.Namespace = name
		if err := parser.expect(lexer.Semicolon); err != nil {
			return nil, err
		}
	}
	for !parser.at(lexer.EOF) {
		switch {
		case parser.match(lexer.Type):
			declaration, err := parser.parseType()
			if err != nil {
				return nil, err
			}
			file.Types = append(file.Types, declaration)
		case parser.match(lexer.Task):
			declaration, err := parser.parseTask()
			if err != nil {
				return nil, err
			}
			file.Tasks = append(file.Tasks, declaration)
		case parser.match(lexer.Workflow):
			declaration, err := parser.parseWorkflow()
			if err != nil {
				return nil, err
			}
			file.Workflows = append(file.Workflows, declaration)
		default:
			return nil, parser.errorf("expected type, task, or workflow declaration")
		}
	}
	return file, nil
}

func (parser *Parser) parseType() (ast.TypeDecl, error) {
	name, err := parser.identifier("type name")
	if err != nil {
		return ast.TypeDecl{}, err
	}
	if err := parser.expect(lexer.LeftBrace); err != nil {
		return ast.TypeDecl{}, err
	}
	declaration := ast.TypeDecl{Name: name}
	for !parser.match(lexer.RightBrace) {
		fieldName, err := parser.identifier("field name")
		if err != nil {
			return ast.TypeDecl{}, err
		}
		if err := parser.expect(lexer.Colon); err != nil {
			return ast.TypeDecl{}, err
		}
		fieldType, err := parser.typeName("field type")
		if err != nil {
			return ast.TypeDecl{}, err
		}
		declaration.Fields = append(declaration.Fields, ast.FieldDecl{Name: fieldName, Type: fieldType})
		parser.match(lexer.Semicolon)
		parser.match(lexer.Comma)
	}
	return declaration, nil
}

func (parser *Parser) parseTask() (ast.TaskDecl, error) {
	name, err := parser.identifier("task name")
	if err != nil {
		return ast.TaskDecl{}, err
	}
	input, err := parser.parseParameter()
	if err != nil {
		return ast.TaskDecl{}, err
	}
	if err := parser.expect(lexer.Arrow); err != nil {
		return ast.TaskDecl{}, err
	}
	output, err := parser.typeName("task output type")
	if err != nil {
		return ast.TaskDecl{}, err
	}
	if err := parser.expect(lexer.LeftBrace); err != nil {
		return ast.TaskDecl{}, err
	}
	declaration := ast.TaskDecl{Name: name, Input: input, Output: output}
	for !parser.match(lexer.RightBrace) {
		if err := parser.parseTaskPolicy(&declaration); err != nil {
			return ast.TaskDecl{}, err
		}
		parser.match(lexer.Semicolon)
	}
	return declaration, nil
}

// parseTaskPolicy parses one timeout or retries policy entry inside a task body.
func (parser *Parser) parseTaskPolicy(declaration *ast.TaskDecl) error {
	switch {
	case parser.match(lexer.Timeout):
		duration, err := parser.duration()
		if err != nil {
			return err
		}
		declaration.Timeout = duration
	case parser.match(lexer.Retries):
		retries, err := parser.number("retry count")
		if err != nil {
			return err
		}
		declaration.Retries = retries
		if err := parser.expect(lexer.With); err != nil {
			return err
		}
		if err := parser.expect(lexer.Backoff); err != nil {
			return err
		}
		backoff, err := parser.duration()
		if err != nil {
			return err
		}
		declaration.Backoff = backoff
	default:
		return parser.errorf("expected timeout or retries policy")
	}
	return nil
}

func (parser *Parser) parseWorkflow() (ast.WorkflowDecl, error) {
	name, err := parser.identifier("workflow name")
	if err != nil {
		return ast.WorkflowDecl{}, err
	}
	input, err := parser.parseParameter()
	if err != nil {
		return ast.WorkflowDecl{}, err
	}
	if err := parser.expect(lexer.Arrow); err != nil {
		return ast.WorkflowDecl{}, err
	}
	output, err := parser.typeName("workflow output type")
	if err != nil {
		return ast.WorkflowDecl{}, err
	}
	body, err := parser.parseBlock()
	if err != nil {
		return ast.WorkflowDecl{}, err
	}
	return ast.WorkflowDecl{Name: name, Input: input, Output: output, Body: body}, nil
}

func (parser *Parser) parseParameter() (string, error) {
	if err := parser.expect(lexer.LeftParen); err != nil {
		return "", err
	}
	name, err := parser.identifier("parameter name")
	if err != nil {
		return "", err
	}
	if err := parser.expect(lexer.Colon); err != nil {
		return "", err
	}
	typeName, err := parser.typeName("parameter type")
	if err != nil {
		return "", err
	}
	if err := parser.expect(lexer.RightParen); err != nil {
		return "", err
	}
	return name + ":" + typeName, nil
}

func (parser *Parser) typeName(description string) (string, error) {
	name, err := parser.identifier(description)
	if err != nil {
		return "", err
	}
	if parser.match(lexer.LeftBracket) {
		item, err := parser.typeName("collection item type")
		if err != nil {
			return "", err
		}
		if err := parser.expect(lexer.RightBracket); err != nil {
			return "", err
		}
		return name + "[" + item + "]", nil
	}
	return name, nil
}

func (parser *Parser) parseBlock() ([]ast.Node, error) {
	if err := parser.expect(lexer.LeftBrace); err != nil {
		return nil, err
	}
	nodes := make([]ast.Node, 0)
	for !parser.match(lexer.RightBrace) {
		if parser.at(lexer.EOF) {
			return nil, parser.errorf("unterminated block")
		}
		node, err := parser.parseNode()
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (parser *Parser) parseNode() (ast.Node, error) {
	switch {
	case parser.match(lexer.Step):
		return parser.parseStep()
	case parser.match(lexer.Delay):
		return parser.parseDelay()
	case parser.match(lexer.If):
		return parser.parseIf()
	case parser.match(lexer.Fork):
		return parser.parseFork()
	case parser.match(lexer.FanOut):
		return parser.parseFanOut()
	case parser.match(lexer.Return):
		return parser.parseReturn()
	case parser.match(lexer.Fail):
		return parser.parseFail()
	default:
		return nil, parser.errorf("expected workflow node")
	}
}

func (parser *Parser) parseStep() (ast.StepNode, error) {
	name, err := parser.identifier("step name")
	if err != nil {
		return ast.StepNode{}, err
	}
	if err := parser.expect(lexer.Assign); err != nil {
		return ast.StepNode{}, err
	}
	call, err := parser.parseCall()
	if err != nil {
		return ast.StepNode{}, err
	}
	var compensation *ast.CallExpr
	if parser.match(lexer.Compensate) {
		if err := parser.expect(lexer.With); err != nil {
			return ast.StepNode{}, err
		}
		value, err := parser.parseCall()
		if err != nil {
			return ast.StepNode{}, err
		}
		compensation = &value
	}
	if err := parser.expect(lexer.Semicolon); err != nil {
		return ast.StepNode{}, err
	}
	return ast.StepNode{Name: name, Call: call, Compensation: compensation}, nil
}

func (parser *Parser) parseDelay() (ast.DelayNode, error) {
	name, err := parser.identifier("delay name")
	if err != nil {
		return ast.DelayNode{}, err
	}
	if err := parser.expect(lexer.Assign); err != nil {
		return ast.DelayNode{}, err
	}
	duration, err := parser.duration()
	if err != nil {
		return ast.DelayNode{}, err
	}
	if err := parser.expect(lexer.Semicolon); err != nil {
		return ast.DelayNode{}, err
	}
	return ast.DelayNode{Name: name, Duration: duration}, nil
}

func (parser *Parser) parseIf() (ast.IfNode, error) {
	if err := parser.expect(lexer.LeftParen); err != nil {
		return ast.IfNode{}, err
	}
	condition, err := parser.parseExpression()
	if err != nil {
		return ast.IfNode{}, err
	}
	if err := parser.expect(lexer.RightParen); err != nil {
		return ast.IfNode{}, err
	}
	then, err := parser.parseBlock()
	if err != nil {
		return ast.IfNode{}, err
	}
	var otherwise []ast.Node
	if parser.match(lexer.Else) {
		otherwise, err = parser.parseBlock()
		if err != nil {
			return ast.IfNode{}, err
		}
	}
	return ast.IfNode{Condition: condition, Then: then, Else: otherwise}, nil
}

func (parser *Parser) parseFork() (ast.ForkNode, error) {
	if err := parser.expect(lexer.LeftBrace); err != nil {
		return ast.ForkNode{}, err
	}
	fork := ast.ForkNode{}
	for !parser.match(lexer.RightBrace) {
		if err := parser.expect(lexer.Branch); err != nil {
			return ast.ForkNode{}, err
		}
		branch, err := parser.parseBlock()
		if err != nil {
			return ast.ForkNode{}, err
		}
		fork.Branches = append(fork.Branches, branch)
	}
	return fork, nil
}

func (parser *Parser) parseFanOut() (ast.FanOutNode, error) {
	variable, err := parser.identifier("fan-out variable")
	if err != nil {
		return ast.FanOutNode{}, err
	}
	if err := parser.expect(lexer.In); err != nil {
		return ast.FanOutNode{}, err
	}
	collection, err := parser.parseExpression()
	if err != nil {
		return ast.FanOutNode{}, err
	}
	concurrency := 8
	if parser.match(lexer.Concurrency) {
		value := parser.advance()
		if value.Kind != lexer.Number {
			return ast.FanOutNode{}, parser.errorAt(value, "expected positive fan-out concurrency")
		}
		concurrency, err = strconv.Atoi(value.Lexeme)
		if err != nil || concurrency < 1 {
			return ast.FanOutNode{}, parser.errorAt(value, "fan-out concurrency must be a positive integer")
		}
	}
	body, err := parser.parseBlock()
	if err != nil {
		return ast.FanOutNode{}, err
	}
	return ast.FanOutNode{Variable: variable, Collection: collection, Concurrency: concurrency, Body: body}, nil
}

func (parser *Parser) parseReturn() (ast.ReturnNode, error) {
	typeName, err := parser.identifier("return type")
	if err != nil {
		return ast.ReturnNode{}, err
	}
	fields, err := parser.parseObject()
	if err != nil {
		return ast.ReturnNode{}, err
	}
	if err := parser.expect(lexer.Semicolon); err != nil {
		return ast.ReturnNode{}, err
	}
	return ast.ReturnNode{Type: typeName, Fields: fields}, nil
}

func (parser *Parser) parseFail() (ast.FailNode, error) {
	if err := parser.expect(lexer.LeftParen); err != nil {
		return ast.FailNode{}, err
	}
	message, err := parser.parseExpression()
	if err != nil {
		return ast.FailNode{}, err
	}
	if err := parser.expect(lexer.RightParen); err != nil {
		return ast.FailNode{}, err
	}
	if err := parser.expect(lexer.Semicolon); err != nil {
		return ast.FailNode{}, err
	}
	return ast.FailNode{Message: message}, nil
}

func (parser *Parser) parseCall() (ast.CallExpr, error) {
	name, err := parser.identifier("call name")
	if err != nil {
		return ast.CallExpr{}, err
	}
	if err := parser.expect(lexer.LeftParen); err != nil {
		return ast.CallExpr{}, err
	}
	call := ast.CallExpr{Name: name}
	if !parser.match(lexer.RightParen) {
		object, err := parser.parseObject()
		if err != nil {
			return ast.CallExpr{}, err
		}
		call.Fields = object
		if err := parser.expect(lexer.RightParen); err != nil {
			return ast.CallExpr{}, err
		}
	}
	return call, nil
}

func (parser *Parser) parseObject() ([]ast.Assignment, error) {
	if err := parser.expect(lexer.LeftBrace); err != nil {
		return nil, err
	}
	fields := make([]ast.Assignment, 0)
	for !parser.match(lexer.RightBrace) {
		name, err := parser.identifier("object field")
		if err != nil {
			return nil, err
		}
		if err := parser.expect(lexer.Colon); err != nil {
			return nil, err
		}
		value, err := parser.parseExpression()
		if err != nil {
			return nil, err
		}
		fields = append(fields, ast.Assignment{Name: name, Value: value})
		if !parser.match(lexer.Comma) {
			if !parser.at(lexer.RightBrace) {
				return nil, parser.errorf("expected comma or closing brace")
			}
		}
	}
	return fields, nil
}

func (parser *Parser) parseExpression() (ast.Expression, error) {
	return parser.parseOr()
}

func (parser *Parser) parseOr() (ast.Expression, error) {
	left, err := parser.parseAnd()
	if err != nil {
		return nil, err
	}
	for parser.at(lexer.Or) {
		operator := parser.advance().Lexeme
		right, err := parser.parseAnd()
		if err != nil {
			return nil, err
		}
		left = ast.BinaryExpr{Left: left, Operator: operator, Right: right}
	}
	return left, nil
}

func (parser *Parser) parseAnd() (ast.Expression, error) {
	left, err := parser.parseComparison()
	if err != nil {
		return nil, err
	}
	for parser.at(lexer.And) {
		operator := parser.advance().Lexeme
		right, err := parser.parseComparison()
		if err != nil {
			return nil, err
		}
		left = ast.BinaryExpr{Left: left, Operator: operator, Right: right}
	}
	return left, nil
}

func (parser *Parser) parseComparison() (ast.Expression, error) {
	left, err := parser.parseUnary()
	if err != nil {
		return nil, err
	}
	if parser.at(lexer.Greater) || parser.at(lexer.Less) || parser.at(lexer.Equal) || parser.at(lexer.NotEqual) || parser.at(lexer.GreaterEqual) || parser.at(lexer.LessEqual) {
		operator := parser.advance().Lexeme
		right, err := parser.parseUnary()
		if err != nil {
			return nil, err
		}
		return ast.BinaryExpr{Left: left, Operator: operator, Right: right}, nil
	}
	return left, nil
}

func (parser *Parser) parseUnary() (ast.Expression, error) {
	if parser.match(lexer.Bang) {
		operand, err := parser.parseUnary()
		if err != nil {
			return nil, err
		}
		return ast.UnaryExpr{Operator: "!", Operand: operand}, nil
	}
	return parser.parsePrimary()
}

func (parser *Parser) parsePrimary() (ast.Expression, error) {
	var expression ast.Expression
	token := parser.advance()
	switch token.Kind {
	case lexer.Identifier:
		expression = ast.IdentifierExpr{Name: token.Lexeme}
	case lexer.String:
		expression = ast.StringExpr{Value: token.Lexeme}
	case lexer.Number:
		expression = ast.NumberExpr{Value: token.Lexeme}
	case lexer.Duration:
		duration, err := time.ParseDuration(token.Lexeme)
		if err != nil {
			return nil, parser.errorAt(token, "invalid duration")
		}
		expression = ast.DurationExpr{Value: duration}
	case lexer.True:
		expression = ast.BoolExpr{Value: true}
	case lexer.False:
		expression = ast.BoolExpr{Value: false}
	default:
		return nil, parser.errorAt(token, "expected expression")
	}
	for parser.match(lexer.Dot) {
		name, err := parser.identifier("member name")
		if err != nil {
			return nil, err
		}
		expression = ast.MemberExpr{Object: expression, Name: name}
	}
	return expression, nil
}

func (parser *Parser) parseQualifiedName() (string, error) {
	name, err := parser.identifier("qualified name")
	if err != nil {
		return "", err
	}
	for parser.match(lexer.Dot) {
		part, err := parser.identifier("qualified name segment")
		if err != nil {
			return "", err
		}
		name += "." + part
	}
	return name, nil
}

func (parser *Parser) identifier(description string) (string, error) {
	token := parser.advance()
	if token.Kind != lexer.Identifier {
		return "", parser.errorAt(token, "expected "+description)
	}
	return token.Lexeme, nil
}

func (parser *Parser) duration() (time.Duration, error) {
	token := parser.advance()
	if token.Kind != lexer.Duration {
		return 0, parser.errorAt(token, "expected duration")
	}
	value, err := time.ParseDuration(token.Lexeme)
	if err != nil {
		return 0, parser.errorAt(token, "invalid duration")
	}
	return value, nil
}

func (parser *Parser) number(description string) (int, error) {
	token := parser.advance()
	if token.Kind != lexer.Number {
		return 0, parser.errorAt(token, "expected "+description)
	}
	value, err := strconv.Atoi(token.Lexeme)
	if err != nil {
		return 0, parser.errorAt(token, "invalid number")
	}
	return value, nil
}

func (parser *Parser) expect(kind lexer.Kind) error {
	if parser.at(kind) {
		parser.index++
		return nil
	}
	return parser.errorf("expected %s", kind)
}

func (parser *Parser) match(kind lexer.Kind) bool {
	if parser.at(kind) {
		parser.index++
		return true
	}
	return false
}

func (parser *Parser) at(kind lexer.Kind) bool {
	return parser.index < len(parser.tokens) && parser.tokens[parser.index].Kind == kind
}

func (parser *Parser) advance() lexer.Token {
	if parser.index >= len(parser.tokens) {
		return lexer.Token{Kind: lexer.EOF}
	}
	token := parser.tokens[parser.index]
	parser.index++
	return token
}

func (parser *Parser) errorf(format string, args ...any) error {
	if parser.index >= len(parser.tokens) {
		return fmt.Errorf(format+" at end of input", args...)
	}
	return parser.errorAt(parser.tokens[parser.index], format, args...)
}

func (parser *Parser) errorAt(token lexer.Token, message string, args ...any) error {
	return fmt.Errorf(message+" at %d:%d", append(args, token.Line, token.Column)...)
}
