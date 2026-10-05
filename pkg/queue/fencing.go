package queue

import (
	"fmt"
	"sync/atomic"
)

type FencingTokenGenerator struct {
	value atomic.Uint64
}

func (generator *FencingTokenGenerator) Next() uint64 {
	return generator.value.Add(1)
}

func ValidateFencingToken(expected, presented uint64) error {
	if expected == 0 {
		return fmt.Errorf("expected fencing token is required")
	}
	if presented != expected {
		return fmt.Errorf("stale fencing token: got %d, want %d", presented, expected)
	}
	return nil
}
