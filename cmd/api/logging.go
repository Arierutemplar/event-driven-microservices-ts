package main

import (
	"log/slog"
	"os"
	"strings"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// setupLogger configura el logger global a partir de LOG_LEVEL (debug|info|warn|error, por
// defecto info) y LOG_FORMAT (text|json, por defecto text).
func setupLogger() {
	var level slog.Level
	invalid := false
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "", "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level, invalid = slog.LevelInfo, true
	}

	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewTextHandler(os.Stdout, opts)
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(observability.ContextHandler{Handler: h}))

	if invalid {
		slog.Warn("LOG_LEVEL inválido, usando info", "valor", os.Getenv("LOG_LEVEL"))
	}
}
