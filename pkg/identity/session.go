package identity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrSessionNotFound = errors.New("portal session not found or expired")

// Session data is encrypted by the portal before being passed to storage.
type Session struct {
	Data      string
	ExpiresAt time.Time
}

type SessionStore interface {
	CreateSession(context.Context, string, Session) error
	UpdateSession(context.Context, string, func(*Session) error) error
	DeleteSession(context.Context, string) error
}

type memorySessions struct {
	mu       sync.Mutex
	sessions map[string]Session
}

func NewMemorySessionStore() SessionStore {
	return &memorySessions{sessions: make(map[string]Session)}
}

func (store *memorySessions) CreateSession(ctx context.Context, id string, session Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, value := range store.sessions {
		if !value.ExpiresAt.After(time.Now()) {
			delete(store.sessions, key)
		}
	}
	if _, exists := store.sessions[id]; exists {
		return fmt.Errorf("session ID already exists")
	}
	store.sessions[id] = session
	return nil
}

func (store *memorySessions) UpdateSession(ctx context.Context, id string, operation func(*Session) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	session, ok := store.sessions[id]
	if !ok || !session.ExpiresAt.After(time.Now()) {
		delete(store.sessions, id)
		return ErrSessionNotFound
	}
	if err := operation(&session); err != nil {
		return err
	}
	store.sessions[id] = session
	return nil
}

func (store *memorySessions) DeleteSession(ctx context.Context, id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.sessions, id)
	return ctx.Err()
}
