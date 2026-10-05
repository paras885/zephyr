package lexer

import "testing"

func TestLexerTokenizesWorkflowSyntax(t *testing.T) {
	source := `namespace ecommerce.orders;

task ChargePayment(input: ChargePaymentInput) -> ChargePaymentOutput {
    timeout 30s
    retries 3 with backoff 200ms
}

workflow Checkout(input: Input) -> Output {
    step charge = ChargePayment({ order_id: input.order_id }) compensate with RefundPayment({ transaction_id: charge.transaction_id });
    if (charge.amount > 10000.0) { fail("high risk"); }
    fork { branch { step notify = SendEmail({}); } }
}`
	tokens, err := New(source).All()
	if err != nil {
		t.Fatal(err)
	}
	want := []Kind{Namespace, Identifier, Dot, Identifier, Semicolon, Task, Identifier, LeftParen, Identifier, Colon, Identifier, RightParen, Arrow, Identifier, LeftBrace, Timeout, Duration, Retries, Number, With, Backoff, Duration, RightBrace, Workflow, Identifier, LeftParen, Identifier, Colon, Identifier, RightParen, Arrow, Identifier, LeftBrace, Step, Identifier, Assign, Identifier}
	if len(tokens) < len(want)+1 {
		t.Fatalf("token count = %d, want at least %d", len(tokens), len(want)+1)
	}
	for index, kind := range want {
		if tokens[index].Kind != kind {
			t.Fatalf("token %d = %s, want %s", index, tokens[index], kind)
		}
	}
	if tokens[len(tokens)-1].Kind != EOF {
		t.Fatalf("last token = %s, want EOF", tokens[len(tokens)-1])
	}
}

func TestLexerRejectsUnexpectedCharacter(t *testing.T) {
	if _, err := New("workflow Demo @").All(); err == nil {
		t.Fatal("lexer accepted an unexpected character")
	}
}

func TestLexerTracksLinesAndColumns(t *testing.T) {
	tokens, err := New("task\n  Run").All()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[1].Line != 2 || tokens[1].Column != 3 {
		t.Fatalf("Run position = %d:%d, want 2:3", tokens[1].Line, tokens[1].Column)
	}
}
