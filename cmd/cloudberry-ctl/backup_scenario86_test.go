package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 86a — buildCreateBackupRequest: full / single-data-file / incremental.
// ---------------------------------------------------------------------------

func gpbackupOptions(t *testing.T, body map[string]interface{}) map[string]interface{} {
	t.Helper()
	gp, ok := body["gpbackupOptions"].(map[string]interface{})
	require.True(t, ok, "body must contain gpbackupOptions object")
	return gp
}

func TestBuildCreateBackupRequest_Full86a(t *testing.T) {
	f := &backupCreateFlags{
		databases:        []string{"mydb"},
		backupType:       "full",
		compressionLevel: 6,
		compressionType:  "zstd",
		jobs:             4,
		includeSchemas:   []string{"public"},
		excludeTables:    []string{"public.temp"},
		withStats:        true,
		withoutGlobals:   true,
	}
	req := buildCreateBackupRequest(f)

	assert.Equal(t, "full", req["type"])
	assert.Equal(t, []string{"mydb"}, req["databases"])

	gp, ok := req["gpbackupOptions"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, int32(6), gp["compressionLevel"])
	assert.Equal(t, "zstd", gp["compressionType"])
	assert.Equal(t, int32(4), gp["jobs"])
	assert.Equal(t, []string{"public"}, gp["includeSchemas"])
	assert.Equal(t, []string{"public.temp"}, gp["excludeTables"])
	assert.Equal(t, true, gp["withStats"])
	assert.Equal(t, true, gp["withoutGlobals"])
	// Variant-exclusive flags stay at their zero values.
	assert.Equal(t, false, gp["singleDataFile"])
	assert.Equal(t, false, gp["incremental"])
	assert.Equal(t, int32(0), gp["copyQueueSize"])
	assert.Equal(t, "", gp["fromTimestamp"])
}

func TestBuildCreateBackupRequest_SingleDataFile86a(t *testing.T) {
	f := &backupCreateFlags{
		databases:      []string{"mydb"},
		backupType:     "full",
		singleDataFile: true,
		copyQueueSize:  4,
	}
	gp := gpbackupOptions(t, buildCreateBackupRequest(f))
	assert.Equal(t, true, gp["singleDataFile"])
	assert.Equal(t, int32(4), gp["copyQueueSize"])
	// Mutual exclusion: jobs not set for single-data-file variant.
	assert.Equal(t, int32(0), gp["jobs"])
}

func TestBuildCreateBackupRequest_Incremental86a(t *testing.T) {
	f := &backupCreateFlags{
		databases:         []string{"mydb"},
		backupType:        "incremental",
		incremental:       true,
		fromTimestamp:     "20260519020000",
		leafPartitionData: true,
	}
	req := buildCreateBackupRequest(f)
	assert.Equal(t, "incremental", req["type"])
	gp := gpbackupOptions(t, req)
	assert.Equal(t, true, gp["incremental"])
	assert.Equal(t, "20260519020000", gp["fromTimestamp"])
	assert.Equal(t, true, gp["leafPartitionData"])
}

// TestBackupCreateCmd_Full86a_PostBody exercises the full create command end to
// end through cobra + httptest, asserting the POST method/path and JSON body.
func TestBackupCreateCmd_Full86a_PostBody(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "backup started"})
	})

	err := runSub(t, newBackupCmd(), []string{"create"}, map[string]string{
		"database":          "mydb",
		"type":              "full",
		"compression-level": "6",
		"compression-type":  "zstd",
		"jobs":              "4",
		"include-schema":    "public",
		"exclude-table":     "public.temp",
		"with-stats":        "true",
		"without-globals":   "true",
	})
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Contains(t, gotPath, "/clusters/test-cluster/backups")
	assert.Contains(t, gotQuery, "namespace=test-ns")
	assert.Equal(t, "full", body["type"])
	gp := gpbackupOptions(t, body)
	assert.Equal(t, float64(6), gp["compressionLevel"])
	assert.Equal(t, "zstd", gp["compressionType"])
	assert.Equal(t, float64(4), gp["jobs"])
	assert.Equal(t, true, gp["withStats"])
	assert.Equal(t, true, gp["withoutGlobals"])
}

func TestBackupCreateCmd_SingleDataFile86a_PostBody(t *testing.T) {
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "backup started"})
	})

	require.NoError(t, runSub(t, newBackupCmd(), []string{"create"}, map[string]string{
		"database":         "mydb",
		"type":             "full",
		"single-data-file": "true",
		"copy-queue-size":  "4",
	}))
	gp := gpbackupOptions(t, body)
	assert.Equal(t, true, gp["singleDataFile"])
	assert.Equal(t, float64(4), gp["copyQueueSize"])
}

func TestBackupCreateCmd_Incremental86a_PostBody(t *testing.T) {
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "backup started"})
	})

	require.NoError(t, runSub(t, newBackupCmd(), []string{"create"}, map[string]string{
		"database":            "mydb",
		"type":                "incremental",
		"incremental":         "true",
		"from-timestamp":      "20260519020000",
		"leaf-partition-data": "true",
	}))
	assert.Equal(t, "incremental", body["type"])
	gp := gpbackupOptions(t, body)
	assert.Equal(t, true, gp["incremental"])
	assert.Equal(t, "20260519020000", gp["fromTimestamp"])
	assert.Equal(t, true, gp["leafPartitionData"])
}

// ---------------------------------------------------------------------------
// 86e — buildRestoreRequest: full flag set incl. resizeCluster.
// ---------------------------------------------------------------------------

func TestBuildRestoreRequest_Full86e(t *testing.T) {
	f := &backupRestoreFlags{
		timestamp:       "20260519020000",
		redirectDb:      "mydb_restored",
		redirectSchema:  "restored",
		createDb:        true,
		includeSchemas:  []string{"public"},
		includeTables:   []string{"public.users"},
		jobs:            4,
		withStats:       true,
		runAnalyze:      true,
		onErrorContinue: true,
		truncateTable:   true,
		resizeCluster:   true,
	}
	req := buildRestoreRequest(f)
	assert.Equal(t, "20260519020000", req["timestamp"])

	gr, ok := req["gprestoreOptions"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, int32(4), gr["jobs"])
	assert.Equal(t, "mydb_restored", gr["redirectDb"])
	assert.Equal(t, "restored", gr["redirectSchema"])
	assert.Equal(t, true, gr["createDb"])
	assert.Equal(t, []string{"public"}, gr["includeSchemas"])
	assert.Equal(t, []string{"public.users"}, gr["includeTables"])
	assert.Equal(t, true, gr["withStats"])
	assert.Equal(t, true, gr["runAnalyze"])
	assert.Equal(t, true, gr["onErrorContinue"])
	assert.Equal(t, true, gr["truncateTable"])
	assert.Equal(t, true, gr["resizeCluster"])
}

func TestBackupRestoreCmd_Full86e_PostBody(t *testing.T) {
	var gotMethod, gotPath string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "restore started"})
	})

	require.NoError(t, runSub(t, newBackupCmd(), []string{"restore"}, map[string]string{
		"timestamp":         "20260519020000",
		"redirect-db":       "mydb_restored",
		"redirect-schema":   "restored",
		"create-db":         "true",
		"include-schema":    "public",
		"include-table":     "public.users",
		"jobs":              "4",
		"with-stats":        "true",
		"run-analyze":       "true",
		"on-error-continue": "true",
		"truncate-table":    "true",
		"resize-cluster":    "true",
	}))

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Contains(t, gotPath, "/clusters/test-cluster/backups/20260519020000/restore")
	gr, ok := body["gprestoreOptions"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, gr["resizeCluster"])
	assert.Equal(t, "mydb_restored", gr["redirectDb"])
	assert.Equal(t, true, gr["runAnalyze"])
	assert.Equal(t, true, gr["truncateTable"])
}

func TestBackupRestoreCmd_MissingTimestamp(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	err := runSub(t, newBackupCmd(), []string{"restore"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--timestamp is required")
}

// ---------------------------------------------------------------------------
// 86b/86c/86d/86f/86j — read/delete command method+path mapping.
// ---------------------------------------------------------------------------

func TestBackupReadCommands_MethodPath(t *testing.T) {
	var gotMethod, gotPath string
	withMockGlobals(t, okJSON(&gotMethod, &gotPath))

	t.Run("86b list", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"list"}, nil))
		assert.Equal(t, http.MethodGet, gotMethod)
		assert.Contains(t, gotPath, "/clusters/test-cluster/backups")
	})

	t.Run("86c status with timestamp", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"status"},
			map[string]string{"timestamp": "20260519020000"}))
		assert.Equal(t, http.MethodGet, gotMethod)
		assert.Contains(t, gotPath, "/backups/20260519020000")
	})

	t.Run("86c status without timestamp falls back to list", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"status"}, nil))
		assert.Equal(t, http.MethodGet, gotMethod)
		assert.Contains(t, gotPath, "/clusters/test-cluster/backups")
	})

	t.Run("86d delete with timestamp", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"delete"},
			map[string]string{"timestamp": "20260519020000"}))
		assert.Equal(t, http.MethodDelete, gotMethod)
		assert.Contains(t, gotPath, "/backups/20260519020000")
	})

	t.Run("86f schedule show", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"schedule"}, nil))
		assert.Equal(t, http.MethodGet, gotMethod)
		assert.Contains(t, gotPath, "/backups/schedule")
	})

	t.Run("86j jobs", func(t *testing.T) {
		require.NoError(t, runSub(t, newBackupCmd(), []string{"jobs"}, nil))
		assert.Equal(t, http.MethodGet, gotMethod)
		assert.Contains(t, gotPath, "/backups/jobs")
	})
}

func TestBackupDeleteCmd_MissingTimestamp(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	err := runSub(t, newBackupCmd(), []string{"delete"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--timestamp is required")
}

// ---------------------------------------------------------------------------
// 86k — buildBackupJobLogsPath + jobs logs streaming with follow/tail.
// ---------------------------------------------------------------------------

func TestBuildBackupJobLogsPath(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "c1"
	globals.namespace = "ns1"

	t.Run("basic", func(t *testing.T) {
		p := buildBackupJobLogsPath(&backupJobsLogsFlags{job: "j1", tail: -1})
		assert.Contains(t, p, "/clusters/c1/backups/jobs/j1/logs")
		assert.Contains(t, p, "namespace=ns1")
		assert.NotContains(t, p, "follow")
		assert.NotContains(t, p, "tailLines")
	})

	t.Run("with follow and tail", func(t *testing.T) {
		p := buildBackupJobLogsPath(&backupJobsLogsFlags{job: "j1", follow: true, tail: 100})
		u, err := url.Parse(p)
		require.NoError(t, err)
		q := u.Query()
		assert.Equal(t, "true", q.Get("follow"))
		assert.Equal(t, "100", q.Get("tailLines"))
		assert.Equal(t, "ns1", q.Get("namespace"))
	})

	t.Run("empty namespace omits query", func(t *testing.T) {
		globals.namespace = ""
		p := buildBackupJobLogsPath(&backupJobsLogsFlags{job: "j1", tail: -1})
		assert.NotContains(t, p, "namespace=")
	})
}

func TestBackupJobsLogsCmd_FollowAndTailQuery(t *testing.T) {
	const logBody = "streaming logs...\n"
	var gotPath, gotQuery string
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(logBody))
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"

	logsCmd := findSubcommand(findSubcommand(newBackupCmd(), "jobs"), "logs")
	require.NotNil(t, logsCmd)

	var buf bytes.Buffer
	logsCmd.SetOut(&buf)
	require.NoError(t, logsCmd.Flags().Set("job", "test-cluster-backup-1"))
	require.NoError(t, logsCmd.Flags().Set("follow", "true"))
	require.NoError(t, logsCmd.Flags().Set("tail", "100"))
	require.NoError(t, logsCmd.RunE(logsCmd, nil))

	assert.Equal(t, logBody, buf.String())
	assert.Contains(t, gotPath, "/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs")
	q, err := url.ParseQuery(gotQuery)
	require.NoError(t, err)
	assert.Equal(t, "true", q.Get("follow"))
	assert.Equal(t, "100", q.Get("tailLines"))
	assert.Equal(t, "test-ns", q.Get("namespace"))
}

func TestBackupJobsLogsCmd_FallbackEmptyNamespace(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "NOT_FOUND", "message": "no route"},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "" // exercises the default-namespace fallback branch
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"

	logsCmd := findSubcommand(findSubcommand(newBackupCmd(), "jobs"), "logs")
	require.NotNil(t, logsCmd)

	var buf bytes.Buffer
	logsCmd.SetOut(&buf)
	require.NoError(t, logsCmd.Flags().Set("job", "j1"))
	require.NoError(t, logsCmd.RunE(logsCmd, nil))

	assert.Contains(t, buf.String(), "kubectl logs -n default job/j1")
}

func TestBackupJobsLogsCmd_MissingCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.namespace = "test-ns"

	logsCmd := findSubcommand(findSubcommand(newBackupCmd(), "jobs"), "logs")
	require.NotNil(t, logsCmd)
	require.NoError(t, logsCmd.Flags().Set("job", "j1"))
	require.Error(t, logsCmd.RunE(logsCmd, nil))
}

func TestRunBackupJobsLogs_NewClientError(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.operatorURL = "" // newClient() returns an error

	cmd := findSubcommand(findSubcommand(newBackupCmd(), "jobs"), "logs")
	require.NotNil(t, cmd)
	err := runBackupJobsLogs(cmd, &backupJobsLogsFlags{job: "j1", tail: -1})
	require.Error(t, err)
}
