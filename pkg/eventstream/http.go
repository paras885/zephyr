package eventstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

const WorkflowEventsPath = "/v1/workflows/"

type Config struct {
	PollInterval time.Duration
	BatchSize    int
}

type Handler struct {
	history store.EventHistoryStore
	config  Config
}

func NewHandler(history store.EventHistoryStore, config Config) (*Handler, error) {
	if history == nil {
		return nil, fmt.Errorf("workflow event history store is required")
	}
	if config.PollInterval < 0 || config.BatchSize < 0 {
		return nil, fmt.Errorf("event stream settings cannot be negative")
	}
	if config.PollInterval == 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	return &Handler{history: history, config: config}, nil
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	workflowID, ok := workflowIDFromPath(request.URL.Path)
	if !ok {
		http.NotFound(response, request)
		return
	}
	cursor, err := parseCursor(request)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := response.(http.Flusher)
	if !ok {
		http.Error(response, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	events, err := handler.history.ListEvents(request.Context(), workflowID, cursor, handler.config.BatchSize)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(response, err.Error(), status)
		return
	}
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache, no-transform")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)
	flusher.Flush()
	ticker := time.NewTicker(handler.config.PollInterval)
	defer ticker.Stop()
	for {
		for _, event := range events {
			if err := writeEvent(response, flusher, event); err != nil {
				return
			}
			cursor = event.Sequence
		}
		if len(events) == handler.config.BatchSize {
			var err error
			events, err = handler.history.ListEvents(request.Context(), workflowID, cursor, handler.config.BatchSize)
			if err != nil {
				return
			}
			continue
		}
		select {
		case <-request.Context().Done():
			return
		case <-ticker.C:
			events, err = handler.history.ListEvents(request.Context(), workflowID, cursor, handler.config.BatchSize)
			if err != nil {
				return
			}
			if len(events) == 0 {
				if _, err := fmt.Fprint(response, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

func workflowIDFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, WorkflowEventsPath) || !strings.HasSuffix(path, "/events") {
		return "", false
	}
	workflowID := strings.TrimSuffix(strings.TrimPrefix(path, WorkflowEventsPath), "/events")
	if workflowID == "" || strings.Contains(workflowID, "/") {
		return "", false
	}
	return workflowID, true
}

func parseCursor(request *http.Request) (uint64, error) {
	value := request.Header.Get("Last-Event-ID")
	if value == "" {
		value = request.URL.Query().Get("after")
	}
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid event cursor: %w", err)
	}
	if cursor > uint64(1<<63-1) {
		return 0, fmt.Errorf("event cursor exceeds the supported range")
	}
	return cursor, nil
}

func writeEvent(response http.ResponseWriter, flusher http.Flusher, event domain.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(response, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
