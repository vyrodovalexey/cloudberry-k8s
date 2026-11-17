package main

// DEF-7 regression (reported by TASK-U022): cloudberry-ctl exited 1 printing NO
// error on API 4xx/5xx responses — the root command sets SilenceErrors, and
// main() mapped *ctl.APIError status codes to exit codes without ever printing
// the error, so only the --password WARNING ever reached stderr. These tests pin
// executeErrorExitCode: 4xx and 5xx responses produce VISIBLE error output on
// stderr (status code + decoded API error, falling back to the raw response
// body) before the non-zero exit, the documented status→exit-code mapping is
// preserved, and non-API errors are surfaced too.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
)

// captureStderr runs fn with os.Stderr swapped for a pipe and returns whatever
// fn wrote to it.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	defer func() { os.Stderr = orig }()
	fn()
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

// apiErrorFromServer performs a real POST through ctl.OperatorClient against a
// mock operator API replying with the given status/body, and returns the error
// the client surfaces (an *ctl.APIError).
func apiErrorFromServer(
	t *testing.T, statusCode int, body string, contentType string,
) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	client := ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:  server.URL,
		Timeout:  5 * time.Second,
		Username: "admin",
		Password: "pass",
	})
	_, err := client.Post(context.Background(), "/data-loading/jobs",
		map[string]string{"name": "dup"})
	require.Error(t, err)
	return err
}

// TestExecuteErrorExitCode_API4xxVisible is the DEF-7 reproduction: a 409
// JOB_EXISTS response (the exact class U022 hit live) must reach stderr with
// status code and decoded API error before exit code 1.
func TestExecuteErrorExitCode_API4xxVisible(t *testing.T) {
	err := apiErrorFromServer(t, http.StatusConflict,
		`{"error":{"code":"JOB_EXISTS","message":"data loading job \"dup\" already exists"}}`,
		"application/json")

	var code int
	stderr := captureStderr(t, func() { code = executeErrorExitCode(err) })

	assert.Equal(t, exitGeneralError, code, "409 must exit 1")
	assert.Contains(t, stderr, "Error:", "the error line itself must be printed")
	assert.Contains(t, stderr, "409", "HTTP status code must be visible")
	assert.Contains(t, stderr, "JOB_EXISTS", "decoded API error code must be visible")
	assert.Contains(t, stderr, "already exists", "decoded API error message must be visible")
}

// TestExecuteErrorExitCode_API5xxVisible covers a 5xx whose body is NOT a JSON
// error envelope: the raw response body must be surfaced instead, before exit 1.
func TestExecuteErrorExitCode_API5xxVisible(t *testing.T) {
	err := apiErrorFromServer(t, http.StatusInternalServerError,
		"gateway exploded", "text/plain")

	var code int
	stderr := captureStderr(t, func() { code = executeErrorExitCode(err) })

	assert.Equal(t, exitGeneralError, code, "500 must exit 1")
	assert.Contains(t, stderr, "500", "HTTP status code must be visible")
	assert.Contains(t, stderr, "gateway exploded", "raw response body must be visible")
}

// TestExecuteErrorExitCode_ExitCodeMapping preserves the documented
// status→exit-code contract while the error is printed: every API status keeps
// its dedicated exit code, unmapped statuses and non-API errors exit 1, and NO
// path returns a code without visible stderr output.
func TestExecuteErrorExitCode_ExitCodeMapping(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCode  int
		wantPrint string
	}{
		{"401 unauthorized", &ctl.APIError{StatusCode: 401, Code: "UNAUTHORIZED", Message: "bad creds"}, exitAuthFailure, "401"},
		{"403 forbidden", &ctl.APIError{StatusCode: 403, Code: "FORBIDDEN", Message: "no"}, exitPermissionDenied, "403"},
		{"404 not found", &ctl.APIError{StatusCode: 404, Code: "NOT_FOUND", Message: "gone"}, exitClusterNotFound, "404"},
		{"408 request timeout", &ctl.APIError{StatusCode: 408, Code: "TIMEOUT", Message: "slow"}, exitTimeout, "408"},
		{"429 rate limited", &ctl.APIError{StatusCode: 429, Code: "RATE_LIMITED", Message: "slow down"}, exitTimeout, "429"},
		{"409 conflict defaults to 1", &ctl.APIError{StatusCode: 409, Code: "JOB_EXISTS", Message: "dup"}, exitGeneralError, "409"},
		{"500 defaults to 1", &ctl.APIError{StatusCode: 500, Code: "INTERNAL", Message: "boom"}, exitGeneralError, "500"},
		{"non-API error", errors.New("operator URL is required"), exitGeneralError, "operator URL is required"},
		{"wrapped API error", fmt.Errorf("listing jobs: %w", &ctl.APIError{StatusCode: 401, Code: "UNAUTHORIZED", Message: "bad creds"}), exitAuthFailure, "401"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			stderr := captureStderr(t, func() { code = executeErrorExitCode(tt.err) })
			assert.Equal(t, tt.wantCode, code)
			assert.NotEmpty(t, stderr, "a non-zero exit must never be silent (DEF-7)")
			assert.Contains(t, stderr, tt.wantPrint)
		})
	}
}

// TestExecuteErrorExitCode_SuccessUnaffected pins the success-path contract: a
// successful API command returns no error (so main() never reaches
// executeErrorExitCode and exits 0) and writes nothing to stderr — the DEF-7 fix
// only adds output on the error path.
func TestExecuteErrorExitCode_SuccessUnaffected(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	var execErr error
	stderr := captureStderr(t, func() { execErr = runAPIGet("/test") })
	require.NoError(t, execErr)
	assert.Empty(t, stderr, "a successful command must print nothing to stderr")
}
