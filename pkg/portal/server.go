package portal

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
)

//go:embed ui/*
var portalFiles embed.FS

func New(api http.Handler) (http.Handler, error) {
	if api == nil {
		return nil, fmt.Errorf("portal API handler is required")
	}
	files, err := fs.Sub(portalFiles, "ui")
	if err != nil {
		return nil, fmt.Errorf("load embedded portal assets: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/", api)
	mux.Handle("/", http.FileServer(http.FS(files)))
	return mux, nil
}
