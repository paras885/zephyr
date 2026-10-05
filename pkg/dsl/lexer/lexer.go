package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type Kind string

const (
	EOF          Kind = "EOF"
	Identifier   Kind = "IDENTIFIER"
	String       Kind = "STRING"
	Number       Kind = "NUMBER"
	Duration     Kind = "DURATION"
	Namespace    Kind = "namespace"
	Type         Kind = "type"
	Task         Kind = "task"
	Workflow     Kind = "workflow"
	Timeout      Kind = "timeout"
	Retries      Kind = "retries"
	With         Kind = "with"
	Backoff      Kind = "backoff"
	Step         Kind = "step"
	Compensate   Kind = "compensate"
	Delay        Kind = "delay"
	If           Kind = "if"
	Else         Kind = "else"
	Fork         Kind = "fork"
	Branch       Kind = "branch"
	FanOut       Kind = "fan_out"
	Concurrency  Kind = "concurrency"
	In           Kind = "in"
	Return       Kind = "return"
	Fail         Kind = "fail"
	True         Kind = "true"
	False        Kind = "false"
	Arrow        Kind = "->"
	Equal        Kind = "=="
	NotEqual     Kind = "!="
	Greater      Kind = ">"
	Less         Kind = "<"
	GreaterEqual Kind = ">="
	LessEqual    Kind = "<="
	And          Kind = "&&"
	Or           Kind = "||"
	Bang         Kind = "!"
	Assign       Kind = "="
	LeftBrace    Kind = "{"
	RightBrace   Kind = "}"
	LeftParen    Kind = "("
	RightParen   Kind = ")"
	LeftBracket  Kind = "["
	RightBracket Kind = "]"
	Colon        Kind = ":"
	Comma        Kind = ","
	Dot          Kind = "."
	Semicolon    Kind = ";"
)

type Token struct {
	Kind   Kind
	Lexeme string
	Line   int
	Column int
}

func (token Token) String() string {
	return fmt.Sprintf("%s(%q) at %d:%d", token.Kind, token.Lexeme, token.Line, token.Column)
}

var keywords = map[string]Kind{
	"namespace": Namespace, "type": Type, "task": Task, "workflow": Workflow,
	"timeout": Timeout, "retries": Retries, "with": With, "backoff": Backoff,
	"step": Step, "compensate": Compensate, "delay": Delay, "if": If, "else": Else,
	"fork": Fork, "branch": Branch, "fan_out": FanOut, "concurrency": Concurrency, "in": In,
	"return": Return, "fail": Fail, "true": True, "false": False,
}

type Lexer struct {
	source []rune
	index  int
	line   int
	column int
}

func New(source string) *Lexer {
	return &Lexer{source: []rune(source), line: 1, column: 1}
}

func (lexer *Lexer) All() ([]Token, error) {
	tokens := make([]Token, 0)
	for {
		token, err := lexer.Next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if token.Kind == EOF {
			return tokens, nil
		}
	}
}

func (lexer *Lexer) Next() (Token, error) {
	for lexer.index < len(lexer.source) {
		if unicode.IsSpace(lexer.current()) {
			lexer.advance()
			continue
		}
		if lexer.current() == '/' && lexer.peek() == '/' {
			lexer.skipComment()
			continue
		}
		break
	}
	line, column := lexer.line, lexer.column
	if lexer.index >= len(lexer.source) {
		return Token{Kind: EOF, Line: line, Column: column}, nil
	}
	character := lexer.current()
	if unicode.IsLetter(character) || character == '_' {
		return lexer.identifier(line, column), nil
	}
	if unicode.IsDigit(character) {
		return lexer.number(line, column), nil
	}
	if character == '"' {
		return lexer.string(line, column)
	}
	if character == '-' && lexer.peek() == '>' {
		lexer.advance()
		lexer.advance()
		return Token{Kind: Arrow, Lexeme: "->", Line: line, Column: column}, nil
	}
	if character == '=' && lexer.peek() == '=' {
		lexer.advance()
		lexer.advance()
		return Token{Kind: Equal, Lexeme: "==", Line: line, Column: column}, nil
	}
	if character == '!' && lexer.peek() == '=' {
		lexer.advance()
		lexer.advance()
		return Token{Kind: NotEqual, Lexeme: "!=", Line: line, Column: column}, nil
	}
	if (character == '&' && lexer.peek() == '&') || (character == '|' && lexer.peek() == '|') {
		operator := string(character) + string(lexer.peek())
		lexer.advance()
		lexer.advance()
		kind := And
		if character == '|' {
			kind = Or
		}
		return Token{Kind: kind, Lexeme: operator, Line: line, Column: column}, nil
	}
	if character == '!' {
		lexer.advance()
		return Token{Kind: Bang, Lexeme: "!", Line: line, Column: column}, nil
	}
	if (character == '>' || character == '<') && lexer.peek() == '=' {
		lexer.advance()
		lexer.advance()
		kind := GreaterEqual
		if character == '<' {
			kind = LessEqual
		}
		return Token{Kind: kind, Lexeme: string(character) + "=", Line: line, Column: column}, nil
	}
	if kind, ok := punctuation(character); ok {
		lexer.advance()
		return Token{Kind: kind, Lexeme: string(character), Line: line, Column: column}, nil
	}
	return Token{}, fmt.Errorf("unexpected character %q at %d:%d", character, line, column)
}

func (lexer *Lexer) identifier(line, column int) Token {
	start := lexer.index
	for lexer.index < len(lexer.source) {
		character := lexer.current()
		if !unicode.IsLetter(character) && !unicode.IsDigit(character) && character != '_' {
			break
		}
		lexer.advance()
	}
	lexeme := string(lexer.source[start:lexer.index])
	kind := Identifier
	if keyword, ok := keywords[lexeme]; ok {
		kind = keyword
	}
	return Token{Kind: kind, Lexeme: lexeme, Line: line, Column: column}
}

func (lexer *Lexer) number(line, column int) Token {
	start := lexer.index
	for lexer.index < len(lexer.source) && (unicode.IsDigit(lexer.current()) || lexer.current() == '.') {
		lexer.advance()
	}
	kind := Number
	for lexer.index < len(lexer.source) && unicode.IsLetter(lexer.current()) {
		kind = Duration
		lexer.advance()
	}
	return Token{Kind: kind, Lexeme: string(lexer.source[start:lexer.index]), Line: line, Column: column}
}

func (lexer *Lexer) string(line, column int) (Token, error) {
	lexer.advance()
	var value strings.Builder
	for lexer.index < len(lexer.source) {
		character := lexer.current()
		if character == '"' {
			lexer.advance()
			return Token{Kind: String, Lexeme: value.String(), Line: line, Column: column}, nil
		}
		if character == '\\' {
			lexer.advance()
			if lexer.index >= len(lexer.source) {
				break
			}
			escaped := lexer.current()
			decoded, err := strconv.Unquote(`"` + `\` + string(escaped) + `"`)
			if err != nil {
				return Token{}, fmt.Errorf("invalid string escape at %d:%d: %w", lexer.line, lexer.column, err)
			}
			value.WriteString(decoded[1 : len(decoded)-1])
			lexer.advance()
			continue
		}
		value.WriteRune(character)
		lexer.advance()
	}
	return Token{}, fmt.Errorf("unterminated string at %d:%d", line, column)
}

func (lexer *Lexer) skipComment() {
	for lexer.index < len(lexer.source) && lexer.current() != '\n' {
		lexer.advance()
	}
}

func (lexer *Lexer) current() rune { return lexer.source[lexer.index] }

func (lexer *Lexer) peek() rune {
	if lexer.index+1 >= len(lexer.source) {
		return 0
	}
	return lexer.source[lexer.index+1]
}

func (lexer *Lexer) advance() {
	if lexer.current() == '\n' {
		lexer.line++
		lexer.column = 1
	} else {
		lexer.column++
	}
	lexer.index++
}

func punctuation(character rune) (Kind, bool) {
	kinds := map[rune]Kind{'=': Assign, '>': Greater, '<': Less, '{': LeftBrace, '}': RightBrace, '(': LeftParen, ')': RightParen, '[': LeftBracket, ']': RightBracket, ':': Colon, ',': Comma, '.': Dot, ';': Semicolon}
	kind, ok := kinds[character]
	return kind, ok
}
