package gateway

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/generator"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

type WorkflowRegistration struct {
	Source  string `json:"source"`
	Version int    `json:"version"`
}

type RegistrationResult struct {
	Name    string            `json:"name"`
	Version int               `json:"version"`
	Files   map[string]string `json:"files"`
}

var errWorkflowCatalogUnavailable = errors.New("workflow catalog unavailable")

func (gateway *Gateway) registeredWorkflow(ctx context.Context, name string, version int) (store.RegisteredWorkflow, error) {
	registry, ok := gateway.executions.(store.WorkflowRegistry)
	if !ok {
		return store.RegisteredWorkflow{}, fmt.Errorf("execution store does not support workflow registration")
	}
	record, err := registry.GetWorkflow(ctx, name, version)
	if errors.Is(err, store.ErrNotFound) {
		return store.RegisteredWorkflow{}, ErrWorkflowNotRegistered
	}
	if err != nil {
		slog.Error("workflow catalog lookup failed", "error", err)
		return store.RegisteredWorkflow{}, errWorkflowCatalogUnavailable
	}
	return record, nil
}

func (gateway *Gateway) serveRegistration(response http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	var input WorkflowRegistration
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid registration JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF || input.Version < 1 || strings.TrimSpace(input.Source) == "" {
		writeError(response, http.StatusBadRequest, "provide one JSON object with source and a positive version")
		return
	}
	definition, err := compiler.Compile(input.Source)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid workflow: "+err.Error())
		return
	}
	definition.Version = input.Version
	files, err := generator.GenerateVersion(input.Source, input.Version)
	if err != nil {
		writeError(response, http.StatusBadRequest, "cannot generate contracts: "+err.Error())
		return
	}
	registry, ok := gateway.executions.(store.WorkflowRegistry)
	if !ok {
		writeError(response, http.StatusNotImplemented, "execution store does not support workflow registration")
		return
	}
	if err := registry.PutWorkflow(request.Context(), store.RegisteredWorkflow{Definition: definition, Source: input.Source}); err != nil {
		if errors.Is(err, store.ErrDefinitionConflict) {
			writeError(response, http.StatusConflict, "workflow name/version is immutable; register a new version")
		} else {
			slog.Error("workflow registration failed", "error", err)
			writeError(response, http.StatusInternalServerError, "could not persist workflow registration")
		}
		return
	}
	output := RegistrationResult{Name: definition.Name, Version: definition.Version, Files: make(map[string]string)}
	for name, body := range files {
		output.Files[name] = string(body)
	}
	slog.Info("workflow registered", "workflow_name", definition.Name, "version", definition.Version)
	writeResult(response, output, nil)
}

func (gateway *Gateway) serveContracts(response http.ResponseWriter, request *http.Request) {
	name := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, WorkflowInstancesPath), "/contracts")
	version, err := parseQueryInt(request, "version", 0)
	if err != nil || version < 0 {
		writeError(response, http.StatusBadRequest, "version must be a non-negative integer")
		return
	}
	record, err := gateway.registeredWorkflow(request.Context(), name, version)
	if err != nil {
		writeResult(response, nil, err)
		return
	}
	if record.Source == "" {
		writeError(response, http.StatusConflict, "source is not stored for this bootstrapped definition; register matching source to enable contracts")
		return
	}
	files, err := generator.GenerateVersion(record.Source, record.Definition.Version)
	if err != nil {
		slog.Error("contract generation failed", "error", err)
		writeError(response, http.StatusInternalServerError, "could not generate workflow contracts")
		return
	}
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		file, err := archive.Create(name)
		if err == nil {
			_, err = file.Write(files[name])
		}
		if err != nil {
			slog.Error("contract archive failed", "error", err)
			writeError(response, http.StatusInternalServerError, "could not package contracts")
			return
		}
	}
	if err := archive.Close(); err != nil {
		writeError(response, http.StatusInternalServerError, "could not package contracts")
		return
	}
	response.Header().Set("Content-Type", "application/zip")
	response.Header().Set("Content-Disposition", `attachment; filename="workflow-contracts.zip"`)
	_, _ = response.Write(buffer.Bytes())
}
