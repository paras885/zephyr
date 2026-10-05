package token

import (
	"context"
	"fmt"
)

type Source interface {
	Token(ctx context.Context) (string, error)
}

type SourceFunc func(ctx context.Context) (string, error)

func (source SourceFunc) Token(ctx context.Context) (string, error) {
	if source == nil {
		return "", fmt.Errorf("token source is required")
	}
	return source(ctx)
}
