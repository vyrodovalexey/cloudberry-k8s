// Package httpjson provides the single, shared JSON response/error-envelope
// encoder used by every HTTP-serving package (REST API server, auth
// middleware). Centralizing the envelope shape {"error":{"code","message"}}
// guarantees clients always receive the same error contract regardless of
// which layer rejected the request (code review L-9).
package httpjson

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// ErrorEnvelope is the unified error response body.
type ErrorEnvelope struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail carries the machine-readable code and human-readable message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Write encodes data as a JSON response with the given status. Encoding
// failures are logged on the supplied logger (slog.Default when nil) — the
// status line is already on the wire at that point, so nothing else can be
// reported to the client.
func Write(w http.ResponseWriter, status int, data interface{}, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logger.Error("failed to encode JSON response", "error", err)
	}
}

// WriteError writes the unified error envelope.
func WriteError(w http.ResponseWriter, status int, code, message string, logger *slog.Logger) {
	Write(w, status, ErrorEnvelope{
		Error: ErrorDetail{Code: code, Message: message},
	}, logger)
}
