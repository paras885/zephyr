package domain

import (
	"fmt"
	"sync/atomic"
)

var idSequence uint64

// NewID creates a process-unique identifier suitable for in-memory execution.
func NewID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, atomic.AddUint64(&idSequence, 1))
}
