package util

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name   string
		level  string
		format LogFormat
	}{
		{
			name:   "json format info level",
			level:  "info",
			format: LogFormatJSON,
		},
		{
			name:   "text format debug level",
			level:  "debug",
			format: LogFormatText,
		},
		{
			name:   "json format error level",
			level:  "error",
			format: LogFormatJSON,
		},
		{
			name:   "text format warn level",
			level:  "warn",
			format: LogFormatText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(tt.level, tt.format, &buf)
			require.NotNil(t, logger)

			// Write a log message to verify it works
			logger.Info("test message")
		})
	}
}

func TestNewLogger_NilWriter(t *testing.T) {
	// Should default to os.Stdout
	logger := NewLogger("info", LogFormatJSON, nil)
	require.NotNil(t, logger)
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected slog.Level
	}{
		{"debug", "debug", slog.LevelDebug},
		{"DEBUG uppercase", "DEBUG", slog.LevelDebug},
		{"info", "info", slog.LevelInfo},
		{"INFO uppercase", "INFO", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"WARN uppercase", "WARN", slog.LevelWarn},
		{"warning", "warning", slog.LevelWarn},
		{"WARNING uppercase", "WARNING", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"ERROR uppercase", "ERROR", slog.LevelError},
		{"unknown defaults to info", "unknown", slog.LevelInfo},
		{"empty defaults to info", "", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseLogLevel(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWithLogger_LoggerFromContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("info", LogFormatJSON, &buf)

	ctx := context.Background()
	ctx = WithLogger(ctx, logger)

	retrieved := LoggerFromContext(ctx)
	require.NotNil(t, retrieved)
	// The retrieved logger should be the same one we put in
	assert.Equal(t, logger, retrieved)
}

func TestLoggerFromContext_NoLogger(t *testing.T) {
	ctx := context.Background()
	logger := LoggerFromContext(ctx)
	require.NotNil(t, logger, "should return default logger when none in context")
}

func TestSlogToLogr(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("info", LogFormatJSON, &buf)
	logrLogger := SlogToLogr(logger)
	require.NotNil(t, logrLogger)

	// Verify it can log without panicking
	logrLogger.Info("test message")
}
