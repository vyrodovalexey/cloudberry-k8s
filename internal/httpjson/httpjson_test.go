package httpjson_test

import (
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/httpjson"
)

func TestWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	httpjson.Write(rec, 201, map[string]string{"status": "created"}, slog.Default())

	assert.Equal(t, 201, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "created", body["status"])
}

func TestWrite_NilLogger(t *testing.T) {
	rec := httptest.NewRecorder()
	// Nil logger must not panic (falls back to slog.Default).
	httpjson.Write(rec, 200, map[string]string{"ok": "yes"}, nil)
	assert.Equal(t, 200, rec.Code)
}

func TestWrite_EncodeFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	// Channels are not JSON-encodable; the failure is logged, not panicked.
	httpjson.Write(rec, 200, make(chan int), nil)
	assert.Equal(t, 200, rec.Code)
}

func TestWriteError_Envelope(t *testing.T) {
	rec := httptest.NewRecorder()
	httpjson.WriteError(rec, 404, "CLUSTER_NOT_FOUND", "cluster \"x\" not found", nil)

	assert.Equal(t, 404, rec.Code)
	var env httpjson.ErrorEnvelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "CLUSTER_NOT_FOUND", env.Error.Code)
	assert.Equal(t, "cluster \"x\" not found", env.Error.Message)
}
