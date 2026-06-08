package util

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/go-logr/logr"
)

type contextKey string

const loggerKey contextKey = "logger"

// LogFormat represents the log output format.
type LogFormat string

const (
	// LogFormatJSON outputs logs in JSON format.
	LogFormatJSON LogFormat = "json"
	// LogFormatText outputs logs in human-readable text format.
	LogFormatText LogFormat = "text"
)

// NewLogger creates a new slog.Logger with the specified level and format.
func NewLogger(level string, format LogFormat, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}

	slogLevel := parseLogLevel(level)
	opts := &slog.HandlerOptions{
		Level: slogLevel,
	}

	var handler slog.Handler
	if format == LogFormatJSON {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	return slog.New(handler)
}

// parseLogLevel converts a string log level to slog.Level.
func parseLogLevel(level string) slog.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithLogger adds a logger to the context.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggerFromContext extracts a logger from the context.
// Returns the default logger if none is found.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}

// SlogToLogr creates a logr.Logger adapter from an slog.Logger.
func SlogToLogr(logger *slog.Logger) logr.Logger {
	return logr.FromSlogHandler(logger.Handler())
}
