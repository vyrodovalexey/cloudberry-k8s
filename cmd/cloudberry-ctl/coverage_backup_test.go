package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withMockGlobals points the CLI at a mock server and restores globals on
// cleanup. The handler may capture the request for later assertions.
func withMockGlobals(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := setupMockServer(t, handler)
	saved := globals
	t.Cleanup(func() { globals = saved })
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"
}

// okJSON is a handler that returns a trivial JSON body and optionally records
// the request method and path into the provided pointers.
func okJSON(method, path *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if method != nil {
			*method = r.Method
		}
		if path != nil {
			*path = r.URL.Path
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

// runSub looks up a (possibly nested) subcommand path, applies flags and runs it.
func runSub(t *testing.T, root *cobra.Command, names []string, flags map[string]string) error {
	t.Helper()
	cmd := root
	for _, name := range names {
		cmd = findSubcommand(cmd, name)
		require.NotNil(t, cmd, "subcommand %q must exist", name)
	}
	for k, v := range flags {
		require.NoError(t, cmd.Flags().Set(k, v))
	}
	return cmd.RunE(cmd, nil)
}

// ---------------------------------------------------------------------------
// runAPIPatch direct coverage
// ---------------------------------------------------------------------------

func TestRunAPIPatch_Success(t *testing.T) {
	var gotMethod string
	withMockGlobals(t, okJSON(&gotMethod, nil))
	require.NoError(t, runAPIPatch("/test", map[string]interface{}{"a": 1}))
	assert.Equal(t, http.MethodPatch, gotMethod)
}

func TestRunAPIPatch_ClientError(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = ""
	globals.timeout = "5s"
	err := runAPIPatch("/test", map[string]interface{}{"a": 1})
	require.Error(t, err)
}

func TestRunAPIPatch_ServerError(t *testing.T) {
	withMockGlobals(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "INTERNAL", "message": "boom"},
		})
	})
	err := runAPIPatch("/test", map[string]interface{}{"a": 1})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// backup schedule set/suspend/resume PATCH bodies
// ---------------------------------------------------------------------------

func TestBackupScheduleSet_PatchBody(t *testing.T) {
	var gotMethod, gotPath string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	require.NoError(t, runSub(t, newBackupCmd(),
		[]string{"schedule", "set"}, map[string]string{"cron": "0 4 * * *"}))
	assert.Equal(t, http.MethodPatch, gotMethod)
	assert.Contains(t, gotPath, "/backups/schedule")
	assert.Equal(t, "0 4 * * *", body["schedule"])
}

func TestBackupScheduleSuspendResume_PatchBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		suspend bool
	}{
		{"suspend", true},
		{"resume", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]interface{}
			withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&body)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			})
			require.NoError(t, runSub(t, newBackupCmd(),
				[]string{"schedule", tc.name}, nil))
			assert.Equal(t, tc.suspend, body["suspend"])
		})
	}
}

// ---------------------------------------------------------------------------
// backup restore request body
// ---------------------------------------------------------------------------

func TestBackupRestoreCmd_RequestBody(t *testing.T) {
	var gotPath string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	err := runSub(t, newBackupCmd(), []string{"restore"}, map[string]string{
		"timestamp":   "20260519020000",
		"redirect-db": "newdb",
		"jobs":        "8",
		"create-db":   "true",
	})
	require.NoError(t, err)
	assert.Contains(t, gotPath, "/backups/20260519020000/restore")
	assert.Equal(t, "20260519020000", body["timestamp"])
	gr, ok := body["gprestoreOptions"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "newdb", gr["redirectDb"])
	assert.Equal(t, float64(8), gr["jobs"])
	assert.Equal(t, true, gr["createDb"])
}

// ---------------------------------------------------------------------------
// migrate request body
// ---------------------------------------------------------------------------

func TestMigrateCmd_PostsRequest(t *testing.T) {
	var gotMethod, gotPath string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	cmd := newMigrateCmd()
	require.NoError(t, cmd.Flags().Set("source-cluster", "src"))
	require.NoError(t, cmd.Flags().Set("target-cluster", "dst"))
	require.NoError(t, cmd.Flags().Set("database", "appdb"))
	require.NoError(t, cmd.Flags().Set("tables", "public.a,public.b"))
	require.NoError(t, cmd.Flags().Set("truncate", "true"))
	require.NoError(t, cmd.RunE(cmd, nil))

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Contains(t, gotPath, "/migrate")
	assert.Equal(t, "src", body["sourceCluster"])
	assert.Equal(t, "dst", body["targetCluster"])
	assert.Equal(t, "appdb", body["database"])
	assert.Equal(t, true, body["truncate"])
}

func TestMigrateCmd_MissingTargetCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	cmd := newMigrateCmd()
	require.NoError(t, cmd.Flags().Set("source-cluster", "src"))
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--target-cluster is required")
}

// ---------------------------------------------------------------------------
// queries command success paths (detail/cancel/move/list/export/plan-check)
// ---------------------------------------------------------------------------

func TestQueryCommands_SuccessPaths(t *testing.T) {
	var gotMethod, gotPath string
	withMockGlobals(t, okJSON(&gotMethod, &gotPath))

	t.Run("detail", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryDetailCmd(), nil,
			map[string]string{"query-id": "1234"}))
		assert.Contains(t, gotPath, "/queries/1234")
	})

	t.Run("cancel with reason", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryCancelCmd(), nil,
			map[string]string{"query-id": "1234", "reason": "too slow"}))
		assert.Equal(t, http.MethodPost, gotMethod)
		assert.Contains(t, gotPath, "/queries/1234/cancel")
	})

	t.Run("cancel without reason", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryCancelCmd(), nil,
			map[string]string{"query-id": "5678"}))
		assert.Contains(t, gotPath, "/queries/5678/cancel")
	})

	t.Run("move", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryMoveCmd(), nil,
			map[string]string{"query-id": "9", "target-group": "rg1"}))
		assert.Contains(t, gotPath, "/queries/9/move")
	})

	t.Run("list with status", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryListCmd(), nil,
			map[string]string{"status": "running"}))
		assert.Contains(t, gotPath, "/sessions")
	})
}

func TestQueryCommands_MissingArgs(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"

	t.Run("detail missing id", func(t *testing.T) {
		err := runSub(t, newQueryDetailCmd(), nil, nil)
		require.Error(t, err)
	})
	t.Run("cancel missing id", func(t *testing.T) {
		err := runSub(t, newQueryCancelCmd(), nil, nil)
		require.Error(t, err)
	})
	t.Run("move missing id", func(t *testing.T) {
		err := runSub(t, newQueryMoveCmd(), nil, nil)
		require.Error(t, err)
	})
	t.Run("move missing target", func(t *testing.T) {
		err := runSub(t, newQueryMoveCmd(), nil, map[string]string{"query-id": "1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "target group is required")
	})
}

func TestPlanCheckCmd_InlineAndFile(t *testing.T) {
	var gotPath string
	var body map[string]interface{}
	withMockGlobals(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	t.Run("inline plan", func(t *testing.T) {
		require.NoError(t, runSub(t, newPlanCheckCmd(), nil,
			map[string]string{"plan": "Seq Scan on orders"}))
		assert.Contains(t, gotPath, "/queries/plan-check")
		assert.Equal(t, "Seq Scan on orders", body["planText"])
	})

	t.Run("plan from file", func(t *testing.T) {
		dir := t.TempDir()
		fp := filepath.Join(dir, "plan.txt")
		require.NoError(t, os.WriteFile(fp, []byte("Hash Join"), 0o600))
		require.NoError(t, runSub(t, newPlanCheckCmd(), nil,
			map[string]string{"file": fp}))
		assert.Equal(t, "Hash Join", body["planText"])
	})
}

func TestPlanCheckCmd_Errors(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"

	t.Run("no input", func(t *testing.T) {
		err := runSub(t, newPlanCheckCmd(), nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "either --file")
	})

	t.Run("missing file", func(t *testing.T) {
		err := runSub(t, newPlanCheckCmd(), nil,
			map[string]string{"file": "/no/such/file.txt"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reading plan file")
	})

	t.Run("empty plan text", func(t *testing.T) {
		err := runSub(t, newPlanCheckCmd(), nil, map[string]string{"plan": "   "})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plan text is empty")
	})
}

func TestQueryMonitorCmd_SuccessPaths(t *testing.T) {
	var gotMethod, gotPath string
	withMockGlobals(t, okJSON(&gotMethod, &gotPath))

	for _, tc := range []struct {
		name       string
		wantMethod string
		wantPath   string
	}{
		{"pause", http.MethodPost, "/queries/monitor/pause"},
		{"resume", http.MethodPost, "/queries/monitor/resume"},
		{"state", http.MethodGet, "/queries/monitor/state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, runSub(t, newQueryMonitorCmd(), []string{tc.name}, nil))
			assert.Equal(t, tc.wantMethod, gotMethod)
			assert.Contains(t, gotPath, tc.wantPath)
		})
	}
}

func TestQueryExportCmd_ToFileAndStdout(t *testing.T) {
	withMockGlobals(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("pid,query\n1,select 1\n"))
	})

	t.Run("to file", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "queries.csv")
		require.NoError(t, runSub(t, newQueryExportCmd(), nil,
			map[string]string{"output-file": out}))
		data, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.Contains(t, string(data), "select 1")
	})

	t.Run("to stdout", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryExportCmd(), nil, nil))
	})

	t.Run("unsupported format", func(t *testing.T) {
		err := runSub(t, newQueryExportCmd(), nil, map[string]string{"format": "json"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported export format")
	})
}

func TestQueryHistoryExportCSV_ToFileAndStdout(t *testing.T) {
	withMockGlobals(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("ts,query\n1,select 1\n"))
	})

	t.Run("to stdout via export subcommand", func(t *testing.T) {
		require.NoError(t, runSub(t, newQueryHistoryCmd(), []string{"export"}, nil))
	})

	t.Run("to file", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "history.csv")
		require.NoError(t, runQueryHistoryExportCSV("24h", "alice", "appdb", "sel%", "wildcard", out))
		data, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.Contains(t, string(data), "select 1")
	})
}

func TestQueryHistoryList_ExportShortcut(t *testing.T) {
	withMockGlobals(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("ts,query\n"))
	})
	// histExport == "csv" routes through the export endpoint.
	require.NoError(t, runQueryHistoryList("24h", "", "", "", "", "", "csv", 0, 0))
}

// ---------------------------------------------------------------------------
// metrics command
// ---------------------------------------------------------------------------

func TestMetricsCmd_Exporters(t *testing.T) {
	var gotPath string
	withMockGlobals(t, okJSON(nil, &gotPath))
	require.NoError(t, runSub(t, newMetricsCmd(), []string{"exporters"}, nil))
	assert.Contains(t, gotPath, "/metrics/exporters")
}
