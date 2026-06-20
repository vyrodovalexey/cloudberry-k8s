package main

// Scenario 120 — Usage Reporting CLI tests (C.13).
//
// These drive the `storage usage-report` subcommand end-to-end through the cobra
// command tree against the Scenario 108 recording httptest server (runCtl +
// newCtlRecorderServer), asserting the REAL request path/query string the CLI
// builds. The new --month flag must thread through as the ?month= query param
// the API honors; omitting it must leave the query month-free (current
// behavior); the namespace must always encode.
//
// Catalog IDs covered:
//   120-C13-cli-month    usage-report --month 2026-05 → query has month=2026-05 (+namespace)
//   120-C13-cli-nomonth  usage-report (no --month)    → query has no month key (+namespace)

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 120-C13-cli-month — `storage usage-report --month 2026-05` builds a GET to
// .../storage/usage-report with a well-formed query string carrying
// month=2026-05 AND namespace= (runCtl passes --namespace default). The month
// key exactly matches the API's r.URL.Query().Get("month").
func TestScenario120_CLI_UsageReportMonth(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "usage-report", "--month", "2026-05")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/storage/usage-report", req.path)

	// The query string is well-formed (single, decodable) and carries both keys.
	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "2026-05", values.Get("month"),
		"--month must thread through as the ?month= query param")
	assert.Equal(t, "default", values.Get("namespace"),
		"namespace must always encode")
}

// 120-C13-cli-nomonth — without --month, the CLI builds the request with NO
// month key (the unscoped/current report), while still encoding the namespace.
func TestScenario120_CLI_UsageReportNoMonth(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "usage-report")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/storage/usage-report", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	_, hasMonth := values["month"]
	assert.False(t, hasMonth, "without --month the query must carry no month key")
	assert.Equal(t, "default", values.Get("namespace"),
		"namespace must still encode")
}
