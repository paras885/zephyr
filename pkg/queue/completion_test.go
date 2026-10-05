package queue

import "testing"

func TestCompletionMessageValidation(t *testing.T) {
	valid := CompletionMessage{
		MessageID:  "message-1",
		Kind:       CompletionSucceeded,
		WorkflowID: "workflow-1",
		TaskID:     "task-1",
		NodeID:     "charge",
		LeaseID:    "lease-1",
		LeaseToken: 7,
		Result:     map[string]any{"status": "paid"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid completion rejected: %v", err)
	}
	valid.Kind = CompletionFailed
	valid.Error = "payment declined"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid failure rejected: %v", err)
	}

	invalid := valid
	invalid.MessageID = ""
	if err := invalid.Validate(); err == nil {
		t.Fatal("completion without a message ID was accepted")
	}
	invalid = valid
	invalid.Error = ""
	if err := invalid.Validate(); err == nil {
		t.Fatal("failure without an error was accepted")
	}
	invalid = valid
	invalid.Kind = "unknown"
	if err := invalid.Validate(); err == nil {
		t.Fatal("unknown completion kind was accepted")
	}
}
