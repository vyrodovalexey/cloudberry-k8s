package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
)

// findSubcommand searches for a subcommand by name in a cobra.Command.
func findSubcommand(parent *cobra.Command, name string) *cobra.Command {
	for _, cmd := range parent.Commands() {
		if cmd.Name() == name {
			return cmd
		}
	}
	return nil
}

// subcommandNames returns the names of all subcommands of a cobra.Command.
func subcommandNames(cmd *cobra.Command) []string {
	names := make([]string, 0, len(cmd.Commands()))
	for _, c := range cmd.Commands() {
		names = append(names, c.Name())
	}
	return names
}

// ---------------------------------------------------------------------------
// Root command structure
// ---------------------------------------------------------------------------

func TestNewRootCmd(t *testing.T) {
	root := newRootCmd()
	require.NotNil(t, root)
	assert.Equal(t, appName, root.Use)
	assert.True(t, root.SilenceUsage)
	assert.True(t, root.SilenceErrors)
}

func TestRootCmd_HasWorkloadSubcommand(t *testing.T) {
	root := newRootCmd()
	workload := findSubcommand(root, "workload")
	require.NotNil(t, workload, "root should have a 'workload' subcommand")
}

// ---------------------------------------------------------------------------
// workload command
// ---------------------------------------------------------------------------

func TestWorkloadCmd_Subcommands(t *testing.T) {
	cmd := newWorkloadCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "workload", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "status")
	assert.Contains(t, names, "resource-groups")
	assert.Contains(t, names, "rules")
	assert.Contains(t, names, "idle-rules")
}

// ---------------------------------------------------------------------------
// workload resource-groups
// ---------------------------------------------------------------------------

func TestWorkloadResourceGroupsCmd_Subcommands(t *testing.T) {
	cmd := newWorkloadResourceGroupsCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "resource-groups", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "create")
}

func TestWorkloadResourceGroupsCreateCmd_Flags(t *testing.T) {
	cmd := newWorkloadResourceGroupsCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd, "resource-groups should have a 'create' subcommand")

	tests := []struct {
		flagName string
		wantType string
	}{
		{"name", "string"},
		{"concurrency", "int32"},
	}

	for _, tt := range tests {
		t.Run(tt.flagName, func(t *testing.T) {
			flag := createCmd.Flags().Lookup(tt.flagName)
			require.NotNil(t, flag, "create should have --%s flag", tt.flagName)
			assert.Equal(t, tt.wantType, flag.Value.Type())
		})
	}
}

// ---------------------------------------------------------------------------
// workload rules
// ---------------------------------------------------------------------------

func TestWorkloadRulesCmd_Subcommands(t *testing.T) {
	cmd := newWorkloadRulesCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "rules", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "create")
	assert.Contains(t, names, "import")
	assert.Contains(t, names, "export")
}

func TestWorkloadRulesCreateCmd_Flags(t *testing.T) {
	cmd := newWorkloadRulesCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd, "rules should have a 'create' subcommand")

	// --name flag
	nameFlag := createCmd.Flags().Lookup("name")
	require.NotNil(t, nameFlag, "create should have --name flag")
	assert.Equal(t, "string", nameFlag.Value.Type())

	// -f / --file flag
	fileFlag := createCmd.Flags().Lookup("file")
	require.NotNil(t, fileFlag, "create should have --file flag")
	assert.Equal(t, "string", fileFlag.Value.Type())
	assert.Equal(t, "f", fileFlag.Shorthand, "file flag should have -f shorthand")
}

func TestWorkloadRulesImportCmd_Flags(t *testing.T) {
	cmd := newWorkloadRulesImportCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "import", cmd.Use)

	fileFlag := cmd.Flags().Lookup("file")
	require.NotNil(t, fileFlag, "import should have --file flag")
	assert.Equal(t, "string", fileFlag.Value.Type())
	assert.Equal(t, "f", fileFlag.Shorthand, "file flag should have -f shorthand")
}

func TestWorkloadRulesExportCmd_Flags(t *testing.T) {
	cmd := newWorkloadRulesExportCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "export", cmd.Use)

	outputFlag := cmd.Flags().Lookup("output-file")
	require.NotNil(t, outputFlag, "export should have --output-file flag")
	assert.Equal(t, "string", outputFlag.Value.Type())
	assert.Equal(t, "O", outputFlag.Shorthand, "output-file flag should have -O shorthand")
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func TestAppendNamespaceQuery(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		namespace string
		expected  string
	}{
		{
			name:      "empty namespace",
			path:      "/clusters/test/workload",
			namespace: "",
			expected:  "/clusters/test/workload",
		},
		{
			name:      "with namespace",
			path:      "/clusters/test/workload",
			namespace: "production",
			expected:  "/clusters/test/workload?namespace=production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendNamespaceQuery(tt.path, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNotImplemented(t *testing.T) {
	err := notImplemented("test-command")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test-command")
	assert.Contains(t, err.Error(), "not yet implemented")
}

// ---------------------------------------------------------------------------
// extractRulesFromResponse
// ---------------------------------------------------------------------------

func TestExtractRulesFromResponse(t *testing.T) {
	tests := []struct {
		name      string
		body      map[string]interface{}
		wantCount int
		wantErr   string
	}{
		{
			name: "valid rules array",
			body: map[string]interface{}{
				"rules": []interface{}{
					map[string]interface{}{
						"name":   "rule1",
						"action": "cancel",
					},
					map[string]interface{}{
						"name":   "rule2",
						"action": "log",
					},
				},
			},
			wantCount: 2,
		},
		{
			name:      "no rules key",
			body:      map[string]interface{}{"other": "data"},
			wantCount: 0,
		},
		{
			name:      "empty body",
			body:      map[string]interface{}{},
			wantCount: 0,
		},
		{
			name: "empty rules array",
			body: map[string]interface{}{
				"rules": []interface{}{},
			},
			wantCount: 0,
		},
		{
			name: "rules is not an array",
			body: map[string]interface{}{
				"rules": "not-an-array",
			},
			wantErr: "unexpected rules format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := extractRulesFromResponse(tt.body)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Len(t, rules, tt.wantCount)
			}
		})
	}
}

func TestExtractRulesFromResponse_FieldMapping(t *testing.T) {
	body := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{
				"name":          "cpu-rule",
				"enabled":       true,
				"resourceGroup": "analytics",
				"queryTag":      "etl",
				"role":          "analyst",
				"action":        "cancel",
				"moveTarget":    "overflow",
				"threshold":     "80",
				"thresholdType": "cpu_time",
				"priority":      float64(10),
			},
		},
	}

	rules, err := extractRulesFromResponse(body)
	require.NoError(t, err)
	require.Len(t, rules, 1)

	rule := rules[0]
	assert.Equal(t, "cpu-rule", rule.Name)
	assert.True(t, rule.Enabled)
	assert.Equal(t, "analytics", rule.ResourceGroup)
	assert.Equal(t, "etl", rule.QueryTag)
	assert.Equal(t, "analyst", rule.Role)
	assert.Equal(t, "cancel", rule.Action)
	assert.Equal(t, "overflow", rule.MoveTarget)
	assert.Equal(t, "80", rule.Threshold)
	assert.Equal(t, "cpu_time", rule.ThresholdType)
	assert.Equal(t, int32(10), rule.Priority)
}

// ---------------------------------------------------------------------------
// importRuleResult constants
// ---------------------------------------------------------------------------

func TestImportRuleResultConstants(t *testing.T) {
	assert.Equal(t, importRuleResult(0), importCreated)
	assert.Equal(t, importRuleResult(1), importUpdated)
	assert.Equal(t, importRuleResult(2), importFailed)
}

// ---------------------------------------------------------------------------
// Command tree completeness
// ---------------------------------------------------------------------------

func TestRootCmd_AllTopLevelSubcommands(t *testing.T) {
	root := newRootCmd()
	names := subcommandNames(root)

	expectedCmds := []string{
		"version",
		"cluster",
		"config",
		"segments",
		"ha",
		"sessions",
		"maintenance",
		"auth",
		"inspect",
		"resource-group",
		"resource-queue",
		"workload",
		"queries",
		"backup",
		"data-loading",
		"storage",
		"completion",
	}

	for _, expected := range expectedCmds {
		assert.Contains(t, names, expected, "root should have %q subcommand", expected)
	}
}

func TestWorkloadCmd_StatusSubcommand(t *testing.T) {
	cmd := newWorkloadCmd()
	statusCmd := findSubcommand(cmd, "status")
	require.NotNil(t, statusCmd, "workload should have a 'status' subcommand")
	assert.Equal(t, "status", statusCmd.Use)
}

func TestWorkloadCmd_IdleRulesSubcommand(t *testing.T) {
	cmd := newWorkloadCmd()
	idleRulesCmd := findSubcommand(cmd, "idle-rules")
	require.NotNil(t, idleRulesCmd, "workload should have an 'idle-rules' subcommand")
	assert.Equal(t, "idle-rules", idleRulesCmd.Use)
}

// ---------------------------------------------------------------------------
// Global flags
// ---------------------------------------------------------------------------

func TestRootCmd_GlobalFlags(t *testing.T) {
	root := newRootCmd()
	pf := root.PersistentFlags()

	flagTests := []struct {
		name     string
		flagName string
	}{
		{"cluster", "cluster"},
		{"namespace", "namespace"},
		{"kubeconfig", "kubeconfig"},
		{"context", "context"},
		{"operator-url", "operator-url"},
		{"auth-method", "auth-method"},
		{"username", "username"},
		{"password", "password"},
		{"output", "output"},
		{"verbose", "verbose"},
		{"timeout", "timeout"},
	}

	for _, tt := range flagTests {
		t.Run(tt.name, func(t *testing.T) {
			flag := pf.Lookup(tt.flagName)
			require.NotNil(t, flag, "root should have --%s persistent flag", tt.flagName)
		})
	}
}

func TestRootCmd_OutputFlagShorthand(t *testing.T) {
	root := newRootCmd()
	flag := root.PersistentFlags().Lookup("output")
	require.NotNil(t, flag)
	assert.Equal(t, "o", flag.Shorthand)
}

func TestRootCmd_VerboseFlagShorthand(t *testing.T) {
	root := newRootCmd()
	flag := root.PersistentFlags().Lookup("verbose")
	require.NotNil(t, flag)
	assert.Equal(t, "v", flag.Shorthand)
}

// ---------------------------------------------------------------------------
// requireCluster
// ---------------------------------------------------------------------------

func TestRequireCluster(t *testing.T) {
	tests := []struct {
		name    string
		cluster string
		wantErr bool
	}{
		{
			name:    "empty cluster returns error",
			cluster: "",
			wantErr: true,
		},
		{
			name:    "non-empty cluster succeeds",
			cluster: "my-cluster",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saved := globals.cluster
			defer func() { globals.cluster = saved }()
			globals.cluster = tt.cluster

			err := requireCluster()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "cluster name is required")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// newClient
// ---------------------------------------------------------------------------

func TestNewClient(t *testing.T) {
	tests := []struct {
		name        string
		operatorURL string
		timeout     string
		wantErr     bool
		errContains string
	}{
		{
			name:        "empty operator URL returns error",
			operatorURL: "",
			timeout:     "5m",
			wantErr:     true,
			errContains: "operator URL is required",
		},
		{
			name:        "invalid timeout returns error",
			operatorURL: "http://localhost:8443",
			timeout:     "not-a-duration",
			wantErr:     true,
			errContains: "invalid timeout",
		},
		{
			name:        "valid config creates client",
			operatorURL: "http://localhost:8443",
			timeout:     "30s",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saved := globals
			defer func() { globals = saved }()
			globals.operatorURL = tt.operatorURL
			globals.timeout = tt.timeout
			globals.username = "admin"
			globals.password = "pass"
			globals.authMethod = "basic"

			client, err := newClient()
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// newFormatter
// ---------------------------------------------------------------------------

func TestNewFormatter(t *testing.T) {
	saved := globals.output
	defer func() { globals.output = saved }()

	globals.output = "json"
	f := newFormatter()
	require.NotNil(t, f)
}

// ---------------------------------------------------------------------------
// cmdContext
// ---------------------------------------------------------------------------

func TestCmdContext(t *testing.T) {
	saved := globals.timeout
	defer func() { globals.timeout = saved }()

	t.Run("valid timeout", func(t *testing.T) {
		globals.timeout = "10s"
		ctx, cancel := cmdContext()
		defer cancel()
		require.NotNil(t, ctx)
		// Context should not be done yet.
		select {
		case <-ctx.Done():
			t.Fatal("context should not be done yet")
		default:
		}
	})

	t.Run("invalid timeout uses default", func(t *testing.T) {
		globals.timeout = "invalid"
		ctx, cancel := cmdContext()
		defer cancel()
		require.NotNil(t, ctx)
	})
}

// ---------------------------------------------------------------------------
// applyViperValue
// ---------------------------------------------------------------------------

func TestApplyViperValue(t *testing.T) {
	t.Run("does not override when flag is changed", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.PersistentFlags().String("test-flag", "", "test")
		_ = cmd.PersistentFlags().Set("test-flag", "cli-value")

		savedRoot := rootCmd
		defer func() { rootCmd = savedRoot }()
		rootCmd = cmd

		dst := "original"
		applyViperValue(&dst, "test-flag", "test-flag")
		assert.Equal(t, "original", dst)
	})

	t.Run("applies viper value when flag not changed", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.PersistentFlags().String("test-flag", "", "test")

		savedRoot := rootCmd
		defer func() { rootCmd = savedRoot }()
		rootCmd = cmd

		dst := "original"
		// No viper value set, so dst should remain unchanged.
		applyViperValue(&dst, "test-flag", "nonexistent-key")
		assert.Equal(t, "original", dst)
	})

	t.Run("nil rootCmd does nothing", func(t *testing.T) {
		savedRoot := rootCmd
		defer func() { rootCmd = savedRoot }()
		rootCmd = nil

		dst := "original"
		applyViperValue(&dst, "test-flag", "test-key")
		assert.Equal(t, "original", dst)
	})
}

// ---------------------------------------------------------------------------
// bindEnvVars
// ---------------------------------------------------------------------------

func TestBindEnvVars(t *testing.T) {
	t.Setenv("CLOUDBERRY_CLUSTER", "test-cluster")
	t.Setenv("CLOUDBERRY_NAMESPACE", "test-ns")
	t.Setenv("CLOUDBERRY_OPERATOR_URL", "http://test:8443")

	bindEnvVars()

	// The bound environment variables must be resolvable through viper.
	assert.Equal(t, "test-cluster", viper.GetString("cluster"))
	assert.Equal(t, "test-ns", viper.GetString("namespace"))
	assert.Equal(t, "http://test:8443", viper.GetString("operator-url"))
}

// ---------------------------------------------------------------------------
// initConfig
// ---------------------------------------------------------------------------

func TestInitConfig(t *testing.T) {
	savedRoot := rootCmd
	savedGlobals := globals
	defer func() {
		rootCmd = savedRoot
		globals = savedGlobals
	}()

	rootCmd = newRootCmd()
	globals = globalFlags{timeout: "5m"}

	// initConfig must run without a config file and leave the explicitly
	// set globals intact (no config-file override happened).
	initConfig()
	assert.Equal(t, "5m", globals.timeout, "initConfig must not clobber explicit globals")
}

// ---------------------------------------------------------------------------
// Version command execution
// ---------------------------------------------------------------------------

func TestVersionCmd_Execute(t *testing.T) {
	cmd := newVersionCmd()
	require.NotNil(t, cmd)

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Completion command
// ---------------------------------------------------------------------------

func TestCompletionCmd_Subcommands(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"bash completion", []string{"bash"}, false},
		{"zsh completion", []string{"zsh"}, false},
		{"fish completion", []string{"fish"}, false},
		{"unsupported shell", []string{"powershell"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRootCmd()
			completionCmd := findSubcommand(root, "completion")
			require.NotNil(t, completionCmd)

			var buf bytes.Buffer
			root.SetOut(&buf)
			root.SetArgs(append([]string{"completion"}, tt.args...))
			err := root.Execute()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Command RunE error paths (cluster required)
// ---------------------------------------------------------------------------

func TestClusterCommands_RequireCluster(t *testing.T) {
	// All these commands should fail when cluster is empty.
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	tests := []struct {
		name    string
		cmdFunc func() *cobra.Command
		subcmd  string
	}{
		{"cluster status", newClusterCmd, "status"},
		{"cluster start", newClusterCmd, "start"},
		{"cluster stop", newClusterCmd, "stop"},
		{"cluster restart", newClusterCmd, "restart"},
		{"cluster delete", newClusterCmd, "delete"},
		{"cluster scale-status", newClusterCmd, "scale-status"},
		{"cluster upgrade", newClusterCmd, "upgrade"},
		{"config get", newConfigCmd, "get"},
		{"config reload", newConfigCmd, "reload"},
		{"config reset", newConfigCmd, "reset"},
		{"segments list", newSegmentsCmd, "list"},
		{"segments status", newSegmentsCmd, "status"},
		{"segments inspect", newSegmentsCmd, "inspect"},
		{"mirroring status", newMirroringCmd, "status"},
		{"mirroring enable", newMirroringCmd, "enable"},
		{"mirroring disable", newMirroringCmd, "disable"},
		{"recovery start", newRecoveryCmd, "start"},
		{"recovery status", newRecoveryCmd, "status"},
		{"recovery cancel", newRecoveryCmd, "cancel"},
		{"standby status", newStandbyCmd, "status"},
		{"standby activate", newStandbyCmd, "activate"},
		{"standby reinitialize", newStandbyCmd, "reinitialize"},
		{"standby restore-roles", newStandbyCmd, "restore-roles"},
		{"sessions list", newSessionsCmd, "list"},
		{"maintenance vacuum", newMaintenanceCmd, "vacuum"},
		{"maintenance analyze", newMaintenanceCmd, "analyze"},
		{"maintenance reindex", newMaintenanceCmd, "reindex"},
		{"maintenance check-catalog", newMaintenanceCmd, "check-catalog"},
		{"maintenance jobs", newMaintenanceCmd, "jobs"},
		{"workload status", newWorkloadCmd, "status"},
		{"workload idle-rules", newWorkloadCmd, "idle-rules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := tt.cmdFunc()
			sub := findSubcommand(cmd, tt.subcmd)
			require.NotNil(t, sub, "should have %q subcommand", tt.subcmd)

			err := sub.RunE(sub, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cluster name is required")
		})
	}
}

// ---------------------------------------------------------------------------
// Not-implemented commands
// ---------------------------------------------------------------------------

func TestNotImplementedCommands(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	tests := []struct {
		name    string
		cmdFunc func() *cobra.Command
		subcmd  string
	}{
		{"cluster upgrade", newClusterCmd, "upgrade"},
		{"config reset", newConfigCmd, "reset"},
		{"hba list", newHBACmd, "list"},
		{"hba update", newHBACmd, "update"},
		{"hba history", newHBACmd, "history"},
		{"mirroring enable", newMirroringCmd, "enable"},
		{"mirroring disable", newMirroringCmd, "disable"},
		{"recovery cancel", newRecoveryCmd, "cancel"},
		{"standby reinitialize", newStandbyCmd, "reinitialize"},
		{"standby restore-roles", newStandbyCmd, "restore-roles"},
		{"fts status", newFTSCmd, "status"},
		{"fts configure", newFTSCmd, "configure"},
		{"maintenance check-catalog", newMaintenanceCmd, "check-catalog"},
		{"maintenance jobs", newMaintenanceCmd, "jobs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := tt.cmdFunc()
			sub := findSubcommand(cmd, tt.subcmd)
			require.NotNil(t, sub, "should have %q subcommand", tt.subcmd)

			err := sub.RunE(sub, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not yet implemented")
		})
	}
}

// ---------------------------------------------------------------------------
// Roles subcommands
// ---------------------------------------------------------------------------

func TestRolesCmd_Subcommands(t *testing.T) {
	cmd := newRolesCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "roles", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "create")
	assert.Contains(t, names, "update")
	assert.Contains(t, names, "delete")
}

func TestRolesCmd_AllNotImplemented(t *testing.T) {
	cmd := newRolesCmd()
	for _, sub := range cmd.Commands() {
		t.Run(sub.Name(), func(t *testing.T) {
			err := sub.RunE(sub, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not yet implemented")
		})
	}
}

// ---------------------------------------------------------------------------
// Auth command
// ---------------------------------------------------------------------------

func TestAuthCmd_Subcommands(t *testing.T) {
	cmd := newAuthCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "auth", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "login")
	assert.Contains(t, names, "logout")
	assert.Contains(t, names, "status")
	assert.Contains(t, names, "rotate-password")
	assert.Contains(t, names, "roles")
}

func TestAuthLoginCmd_HasBasicFlag(t *testing.T) {
	cmd := newAuthLoginCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "login", cmd.Use)

	basicFlag := cmd.Flags().Lookup("basic")
	require.NotNil(t, basicFlag, "login should have --basic flag")
	assert.Equal(t, "bool", basicFlag.Value.Type())
}

func TestAuthLoginBasic_MissingUsername(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.username = ""
	globals.password = "pass"
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"
	globals.authMethod = "basic"

	err := runAuthLoginBasic()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is required")
}

func TestAuthLoginBasic_MissingPassword(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"
	globals.authMethod = "basic"

	err := runAuthLoginBasic()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password is required")
}

func TestAuthLoginBasic_Success(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]string{"code": "UNAUTHORIZED", "message": "invalid credentials"},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = "secret"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"
	globals.output = "table"

	err := runAuthLoginBasic()
	require.NoError(t, err)
}

func TestAuthLoginBasic_InvalidPassword(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "correct-password" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]string{"code": "UNAUTHORIZED", "message": "invalid credentials"},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = "wrong-password"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"
	globals.output = "table"

	err := runAuthLoginBasic()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "login failed")
}

func TestAuthLoginOIDC_MissingIssuerURLWithoutCredentials(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.username = ""
	globals.password = ""
	globals.issuerURL = ""

	err := runAuthLoginOIDC()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer URL is required")
}

func TestAuthLoginOIDC_SuccessWithCredentials(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "oidc-user"
	globals.password = "oidc-token"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "oidc"
	globals.output = "table"

	err := runAuthLoginOIDC()
	require.NoError(t, err)
}

func TestAuthStatus_Authenticated(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = "secret"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"
	globals.output = "json"

	err := runAuthStatus()
	require.NoError(t, err)
}

func TestAuthStatus_NotAuthenticated(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "UNAUTHORIZED", "message": "invalid credentials"},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = "wrong"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"
	globals.output = "json"

	// runAuthStatus should succeed (it reports status, doesn't fail on auth errors).
	err := runAuthStatus()
	require.NoError(t, err)
}

func TestAuthStatus_MissingOperatorURL(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = ""
	globals.timeout = "5s"

	err := runAuthStatus()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operator URL is required")
}

func TestAuthLogout(t *testing.T) {
	err := runAuthLogout()
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// FTS command
// ---------------------------------------------------------------------------

func TestFTSCmd_Subcommands(t *testing.T) {
	cmd := newFTSCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "fts", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "status")
	assert.Contains(t, names, "configure")
}

// ---------------------------------------------------------------------------
// HBA command
// ---------------------------------------------------------------------------

func TestHBACmd_Subcommands(t *testing.T) {
	cmd := newHBACmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "hba", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "update")
	assert.Contains(t, names, "history")
}

// ---------------------------------------------------------------------------
// Cluster command
// ---------------------------------------------------------------------------

func TestClusterCmd_Subcommands(t *testing.T) {
	cmd := newClusterCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "cluster", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"status", "start", "stop", "restart", "create", "delete", "scale-status", "upgrade"}
	for _, e := range expected {
		assert.Contains(t, names, e, "cluster should have %q subcommand", e)
	}
}

// ---------------------------------------------------------------------------
// Config command
// ---------------------------------------------------------------------------

func TestConfigCmd_Subcommands(t *testing.T) {
	cmd := newConfigCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "config", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"get", "set", "reset", "reload", "hba"}
	for _, e := range expected {
		assert.Contains(t, names, e, "config should have %q subcommand", e)
	}
}

func TestConfigSetCmd_RequiresArgs(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	cmd := newConfigCmd()
	setCmd := findSubcommand(cmd, "set")
	require.NotNil(t, setCmd)

	// No args should fail.
	err := setCmd.RunE(setCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage: config set")

	// One arg should fail.
	err = setCmd.RunE(setCmd, []string{"key"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage: config set")
}

// ---------------------------------------------------------------------------
// Segments command
// ---------------------------------------------------------------------------

func TestSegmentsCmd_Subcommands(t *testing.T) {
	cmd := newSegmentsCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "segments", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "status")
	assert.Contains(t, names, "inspect")
}

// ---------------------------------------------------------------------------
// HA command
// ---------------------------------------------------------------------------

func TestHACmd_Subcommands(t *testing.T) {
	cmd := newHACmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "ha", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "mirroring")
	assert.Contains(t, names, "recovery")
	assert.Contains(t, names, "standby")
	assert.Contains(t, names, "fts")
	assert.Contains(t, names, "rebalance")
}

func TestHARebalanceCmd_Flags(t *testing.T) {
	cmd := newHACmd()
	rebalanceCmd := findSubcommand(cmd, "rebalance")
	require.NotNil(t, rebalanceCmd)

	statusFlag := rebalanceCmd.Flags().Lookup("status")
	require.NotNil(t, statusFlag)
	assert.Equal(t, "bool", statusFlag.Value.Type())

	tablesFlag := rebalanceCmd.Flags().Lookup("tables")
	require.NotNil(t, tablesFlag)
	assert.Equal(t, "string", tablesFlag.Value.Type())
}

// ---------------------------------------------------------------------------
// Sessions command
// ---------------------------------------------------------------------------

func TestSessionsCmd_Subcommands(t *testing.T) {
	cmd := newSessionsCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "sessions", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "cancel-query")
	assert.Contains(t, names, "terminate")
}

// ---------------------------------------------------------------------------
// Maintenance command
// ---------------------------------------------------------------------------

func TestMaintenanceCmd_Subcommands(t *testing.T) {
	cmd := newMaintenanceCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "maintenance", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"vacuum", "analyze", "reindex", "check-catalog", "jobs"}
	for _, e := range expected {
		assert.Contains(t, names, e, "maintenance should have %q subcommand", e)
	}
}

// ---------------------------------------------------------------------------
// Inspect command
// ---------------------------------------------------------------------------

func TestInspectCmd_Subcommands(t *testing.T) {
	cmd := newInspectCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "inspect", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"disk-usage", "skew", "bloat", "missing-stats", "connections", "locks", "logs"}
	for _, e := range expected {
		assert.Contains(t, names, e, "inspect should have %q subcommand", e)
	}
}

func TestInspectCmd_LogsNotImplemented(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"

	cmd := newInspectCmd()
	logsCmd := findSubcommand(cmd, "logs")
	require.NotNil(t, logsCmd)

	err := logsCmd.RunE(logsCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

// ---------------------------------------------------------------------------
// Resource Group command
// ---------------------------------------------------------------------------

func TestResourceGroupCmd_Subcommands(t *testing.T) {
	cmd := newResourceGroupCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "resource-group", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"list", "create", "update", "delete", "assign"}
	for _, e := range expected {
		assert.Contains(t, names, e, "resource-group should have %q subcommand", e)
	}
}

func TestResourceGroupCmd_CreateFlags(t *testing.T) {
	cmd := newResourceGroupCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	flags := []string{"name", "concurrency", "cpu-max-percent", "memory-limit"}
	for _, f := range flags {
		assert.NotNil(t, createCmd.Flags().Lookup(f), "create should have --%s flag", f)
	}
}

func TestResourceGroupCmd_DeleteFlags(t *testing.T) {
	cmd := newResourceGroupCmd()
	deleteCmd := findSubcommand(cmd, "delete")
	require.NotNil(t, deleteCmd)
	assert.NotNil(t, deleteCmd.Flags().Lookup("name"))
}

func TestResourceGroupCmd_AssignFlags(t *testing.T) {
	cmd := newResourceGroupCmd()
	assignCmd := findSubcommand(cmd, "assign")
	require.NotNil(t, assignCmd)
	assert.NotNil(t, assignCmd.Flags().Lookup("group"))
	assert.NotNil(t, assignCmd.Flags().Lookup("role"))
}

func TestResourceGroupCmd_UpdateNotImplemented(t *testing.T) {
	cmd := newResourceGroupCmd()
	updateCmd := findSubcommand(cmd, "update")
	require.NotNil(t, updateCmd)

	err := updateCmd.RunE(updateCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

// ---------------------------------------------------------------------------
// Resource Queue command
// ---------------------------------------------------------------------------

func TestResourceQueueCmd_Subcommands(t *testing.T) {
	cmd := newResourceQueueCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "resource-queue", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "create")
	assert.Contains(t, names, "delete")
}

func TestResourceQueueCmd_CreateFlags(t *testing.T) {
	cmd := newResourceQueueCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	flags := []string{"name", "active-statements", "memory-limit", "priority", "max-cost"}
	for _, f := range flags {
		assert.NotNil(t, createCmd.Flags().Lookup(f), "create should have --%s flag", f)
	}
}

func TestResourceQueueCmd_DeleteFlags(t *testing.T) {
	cmd := newResourceQueueCmd()
	deleteCmd := findSubcommand(cmd, "delete")
	require.NotNil(t, deleteCmd)
	assert.NotNil(t, deleteCmd.Flags().Lookup("name"))
}

// ---------------------------------------------------------------------------
// Query command
// ---------------------------------------------------------------------------

func TestQueryCmd_Subcommands(t *testing.T) {
	cmd := newQueryCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "queries", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"active", "slow", "history", "status"}
	for _, e := range expected {
		assert.Contains(t, names, e, "queries should have %q subcommand", e)
	}
}

// ---------------------------------------------------------------------------
// Backup command
// ---------------------------------------------------------------------------

func TestBackupCmd_Subcommands(t *testing.T) {
	cmd := newBackupCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "backup", cmd.Use)

	names := subcommandNames(cmd)
	expected := []string{"create", "list", "delete", "restore", "status", "schedule", "jobs"}
	for _, e := range expected {
		assert.Contains(t, names, e, "backup should have %q subcommand", e)
	}
}

func TestBackupCmd_ScheduleSubcommands(t *testing.T) {
	cmd := newBackupCmd()
	scheduleCmd := findSubcommand(cmd, "schedule")
	require.NotNil(t, scheduleCmd)

	names := subcommandNames(scheduleCmd)
	for _, e := range []string{"set", "suspend", "resume"} {
		assert.Contains(t, names, e, "schedule should have %q subcommand", e)
	}
}

func TestBackupCmd_JobsSubcommands(t *testing.T) {
	cmd := newBackupCmd()
	jobsCmd := findSubcommand(cmd, "jobs")
	require.NotNil(t, jobsCmd)
	assert.Contains(t, subcommandNames(jobsCmd), "logs")
}

// ---------------------------------------------------------------------------
// Storage command
// ---------------------------------------------------------------------------

func TestStorageCmd_Subcommands(t *testing.T) {
	cmd := newStorageCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "storage", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "disk-usage")
	assert.Contains(t, names, "tables")
	assert.Contains(t, names, "recommendations")
	assert.Contains(t, names, "usage-report")
}

func TestStorageCmd_TablesSubcommands(t *testing.T) {
	cmd := newStorageCmd()
	tablesCmd := findSubcommand(cmd, "tables")
	require.NotNil(t, tablesCmd)

	names := subcommandNames(tablesCmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "detail")
}

func TestStorageCmd_RecommendationsSubcommands(t *testing.T) {
	cmd := newStorageCmd()
	recCmd := findSubcommand(cmd, "recommendations")
	require.NotNil(t, recCmd)

	names := subcommandNames(recCmd)
	assert.Contains(t, names, "list")
	assert.Contains(t, names, "scan")
}

// ---------------------------------------------------------------------------
// Data Loading command
// ---------------------------------------------------------------------------

func TestDataLoadingCmd_Subcommands(t *testing.T) {
	cmd := newDataLoadingCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "data-loading", cmd.Use)

	names := subcommandNames(cmd)
	assert.Contains(t, names, "jobs")
	assert.Contains(t, names, "status")
}

func TestDataLoadingCmd_JobsSubcommands(t *testing.T) {
	cmd := newDataLoadingCmd()
	jobsCmd := findSubcommand(cmd, "jobs")
	require.NotNil(t, jobsCmd)

	names := subcommandNames(jobsCmd)
	expected := []string{"list", "create", "start", "stop", "delete"}
	for _, e := range expected {
		assert.Contains(t, names, e, "jobs should have %q subcommand", e)
	}
}

// ---------------------------------------------------------------------------
// runAPIGet / runAPIPost / runAPIDelete with mock server
// ---------------------------------------------------------------------------

func setupMockServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestRunAPIGet_Success(t *testing.T) {
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

	err := runAPIGet("/test")
	require.NoError(t, err)
}

func TestRunAPIGet_ClientError(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = ""
	globals.timeout = "5s"

	err := runAPIGet("/test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operator URL is required")
}

func TestRunAPIPost_Success(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	err := runAPIPost("/test", map[string]string{"key": "value"})
	require.NoError(t, err)
}

func TestRunAPIPost_ClientError(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = ""
	globals.timeout = "5s"

	err := runAPIPost("/test", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operator URL is required")
}

func TestRunAPIDelete_Success(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	err := runAPIDelete("/test")
	require.NoError(t, err)
}

func TestRunAPIDelete_ClientError(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = ""
	globals.timeout = "5s"

	err := runAPIDelete("/test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operator URL is required")
}

// ---------------------------------------------------------------------------
// Command RunE with mock server (API calls)
// ---------------------------------------------------------------------------

func TestClusterStatusCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "running"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	statusCmd := findSubcommand(cmd, "status")
	require.NotNil(t, statusCmd)

	err := statusCmd.RunE(statusCmd, nil)
	require.NoError(t, err)
}

func TestClusterCreateCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	err := createCmd.RunE(createCmd, nil)
	require.NoError(t, err)
}

func TestConfigSetCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newConfigCmd()
	setCmd := findSubcommand(cmd, "set")
	require.NotNil(t, setCmd)

	err := setCmd.RunE(setCmd, []string{"max_connections", "200"})
	require.NoError(t, err)
}

func TestHARebalanceCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newHACmd()
	rebalanceCmd := findSubcommand(cmd, "rebalance")
	require.NotNil(t, rebalanceCmd)

	t.Run("basic rebalance", func(t *testing.T) {
		err := rebalanceCmd.RunE(rebalanceCmd, nil)
		require.NoError(t, err)
	})

	t.Run("rebalance with status flag", func(t *testing.T) {
		_ = rebalanceCmd.Flags().Set("status", "true")
		err := rebalanceCmd.RunE(rebalanceCmd, nil)
		require.NoError(t, err)
		_ = rebalanceCmd.Flags().Set("status", "false")
	})

	t.Run("rebalance with tables flag", func(t *testing.T) {
		_ = rebalanceCmd.Flags().Set("tables", "public.t1,public.t2")
		err := rebalanceCmd.RunE(rebalanceCmd, nil)
		require.NoError(t, err)
		_ = rebalanceCmd.Flags().Set("tables", "")
	})
}

// ---------------------------------------------------------------------------
// upsertRule
// ---------------------------------------------------------------------------

func TestUpsertRule_CreateSuccess(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	apiClient := ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    server.URL,
		Username:   "admin",
		Password:   "pass",
		AuthMethod: "basic",
		Timeout:    5e9,
	})

	rule := &ctl.WorkloadRuleFile{
		Name:   "test-rule",
		Action: "cancel",
	}

	result := upsertRule(context.Background(), apiClient, rule)
	assert.Equal(t, importCreated, result)
}

func TestUpsertRule_DuplicateTriggersUpdate(t *testing.T) {
	callCount := 0
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]string{
					"code":    "DUPLICATE_RULE",
					"message": "rule already exists",
				},
			})
			return
		}
		// PUT succeeds.
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	apiClient := ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    server.URL,
		Username:   "admin",
		Password:   "pass",
		AuthMethod: "basic",
		Timeout:    5e9,
	})

	rule := &ctl.WorkloadRuleFile{
		Name:   "test-rule",
		Action: "cancel",
	}

	result := upsertRule(context.Background(), apiClient, rule)
	assert.Equal(t, importUpdated, result)
}

func TestUpsertRule_CreateFails(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{
				"code":    "INTERNAL_ERROR",
				"message": "server error",
			},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	apiClient := ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    server.URL,
		Username:   "admin",
		Password:   "pass",
		AuthMethod: "basic",
		Timeout:    5e9,
	})

	rule := &ctl.WorkloadRuleFile{
		Name:   "test-rule",
		Action: "cancel",
	}

	result := upsertRule(context.Background(), apiClient, rule)
	assert.Equal(t, importFailed, result)
}

// ---------------------------------------------------------------------------
// Workload rules create - missing file
// ---------------------------------------------------------------------------

func TestWorkloadRulesCreateCmd_MissingFile(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	cmd := newWorkloadRulesCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	// No file flag set.
	err := createCmd.RunE(createCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rule file is required")
}

// ---------------------------------------------------------------------------
// Workload rules import - missing file
// ---------------------------------------------------------------------------

func TestWorkloadRulesImportCmd_MissingFile(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	cmd := newWorkloadRulesImportCmd()
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rules file is required")
}

// ---------------------------------------------------------------------------
// Workload rules export - cluster required
// ---------------------------------------------------------------------------

func TestWorkloadRulesExportCmd_RequiresCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadRulesExportCmd()
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

// ---------------------------------------------------------------------------
// Workload resource-groups create - missing name
// ---------------------------------------------------------------------------

func TestWorkloadResourceGroupsCreateCmd_MissingName(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	cmd := newWorkloadResourceGroupsCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	// Name flag not set (empty).
	err := createCmd.RunE(createCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource group name is required")
}

// ---------------------------------------------------------------------------
// Inspect commands with mock server
// ---------------------------------------------------------------------------

func TestInspectCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	subcmds := []string{"disk-usage", "skew", "bloat", "missing-stats", "connections", "locks"}
	for _, name := range subcmds {
		t.Run(name, func(t *testing.T) {
			cmd := newInspectCmd()
			sub := findSubcommand(cmd, name)
			require.NotNil(t, sub)
			err := sub.RunE(sub, nil)
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Backup commands with mock server
// ---------------------------------------------------------------------------

func TestBackupCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("create", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "create")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("list", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "list")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("status", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "status")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "delete")
		require.NotNil(t, sub)
		require.NoError(t, sub.Flags().Set("timestamp", "20260519020000"))
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("restore", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "restore")
		require.NotNil(t, sub)
		require.NoError(t, sub.Flags().Set("timestamp", "20260519020000"))
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("schedule", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "schedule")
		require.NotNil(t, sub)
		require.NoError(t, sub.RunE(sub, nil))
	})

	t.Run("schedule set", func(t *testing.T) {
		cmd := newBackupCmd()
		scheduleCmd := findSubcommand(cmd, "schedule")
		setCmd := findSubcommand(scheduleCmd, "set")
		require.NotNil(t, setCmd)
		require.NoError(t, setCmd.Flags().Set("cron", "0 3 * * *"))
		require.NoError(t, setCmd.RunE(setCmd, nil))
	})

	t.Run("schedule suspend", func(t *testing.T) {
		cmd := newBackupCmd()
		scheduleCmd := findSubcommand(cmd, "schedule")
		suspendCmd := findSubcommand(scheduleCmd, "suspend")
		require.NotNil(t, suspendCmd)
		require.NoError(t, suspendCmd.RunE(suspendCmd, nil))
	})

	t.Run("schedule resume", func(t *testing.T) {
		cmd := newBackupCmd()
		scheduleCmd := findSubcommand(cmd, "schedule")
		resumeCmd := findSubcommand(scheduleCmd, "resume")
		require.NotNil(t, resumeCmd)
		require.NoError(t, resumeCmd.RunE(resumeCmd, nil))
	})

	t.Run("jobs", func(t *testing.T) {
		cmd := newBackupCmd()
		sub := findSubcommand(cmd, "jobs")
		require.NotNil(t, sub)
		require.NoError(t, sub.RunE(sub, nil))
	})
}

func TestBackupCreateCmd_RequestBody(t *testing.T) {
	var captured map[string]interface{}
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.output = "json"

	cmd := newBackupCmd()
	sub := findSubcommand(cmd, "create")
	require.NoError(t, sub.Flags().Set("type", "incremental"))
	require.NoError(t, sub.Flags().Set("database", "mydb"))
	require.NoError(t, sub.Flags().Set("jobs", "4"))
	require.NoError(t, sub.Flags().Set("with-stats", "true"))
	require.NoError(t, sub.RunE(sub, nil))

	require.Equal(t, "incremental", captured["type"])
	gp, ok := captured["gpbackupOptions"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(4), gp["jobs"])
	assert.Equal(t, true, gp["withStats"])
}

func TestMigrateCmd_Subcommand(t *testing.T) {
	cmd := newMigrateCmd()
	require.NotNil(t, cmd)
	assert.Equal(t, "migrate", cmd.Use)
	for _, f := range []string{
		"source-cluster", "target-cluster", "database", "tables",
		"truncate", "redirect-db", "redirect-schema", "jobs",
	} {
		assert.NotNil(t, cmd.Flags().Lookup(f), "migrate should have --%s flag", f)
	}
}

func TestMigrateCmd_RequestBody(t *testing.T) {
	var captured map[string]interface{}
	var gotPath string
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.namespace = ""
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.output = "json"

	cmd := newMigrateCmd()
	require.NoError(t, cmd.Flags().Set("source-cluster", "src"))
	require.NoError(t, cmd.Flags().Set("target-cluster", "dst"))
	require.NoError(t, cmd.Flags().Set("database", "mydb"))
	require.NoError(t, cmd.Flags().Set("tables", "public.users,public.orders"))
	require.NoError(t, cmd.Flags().Set("truncate", "true"))
	require.NoError(t, cmd.RunE(cmd, nil))

	assert.Contains(t, gotPath, "/clusters/src/migrate")
	assert.Equal(t, "src", captured["sourceCluster"])
	assert.Equal(t, "dst", captured["targetCluster"])
	assert.Equal(t, "mydb", captured["database"])
	assert.Equal(t, true, captured["truncate"])
	tables, ok := captured[fieldTables].([]interface{})
	require.True(t, ok)
	assert.Len(t, tables, 2)
}

func TestMigrateCmd_MissingFlags(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()

	cmd := newMigrateCmd()
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--source-cluster is required")

	require.NoError(t, cmd.Flags().Set("source-cluster", "src"))
	err = cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--target-cluster is required")
}

func TestBackupStatusCmd_WithTimestamp(t *testing.T) {
	var gotPath string
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = ""
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.output = "json"

	cmd := newBackupCmd()
	sub := findSubcommand(cmd, "status")
	require.NoError(t, sub.Flags().Set("timestamp", "20260519020000"))
	require.NoError(t, sub.RunE(sub, nil))
	assert.Contains(t, gotPath, "/backups/20260519020000")
}

func TestBackupCmd_MissingTimestampErrors(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"

	cmd := newBackupCmd()
	for _, name := range []string{"delete", "restore"} {
		sub := findSubcommand(cmd, name)
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--timestamp is required")
	}
}

func TestBackupScheduleSetCmd_MissingCron(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"

	cmd := newBackupCmd()
	scheduleCmd := findSubcommand(cmd, "schedule")
	setCmd := findSubcommand(scheduleCmd, "set")
	err := setCmd.RunE(setCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--cron is required")
}

func TestBackupJobsLogsCmd_MissingJob(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"

	logsCmd := findSubcommand(findSubcommand(newBackupCmd(), "jobs"), "logs")
	require.NotNil(t, logsCmd)

	// Missing --job is an error.
	require.Error(t, logsCmd.RunE(logsCmd, nil))
}

func TestBackupJobsLogsCmd_Stream(t *testing.T) {
	const logBody = "gpbackup started\nBackup completed successfully\n"
	var gotPath string
	server := setupMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
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
	require.NoError(t, logsCmd.RunE(logsCmd, nil))

	assert.Equal(t, logBody, buf.String())
	assert.Equal(t,
		"/api/v1alpha1/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs",
		gotPath)
}

func TestBackupJobsLogsCmd_Fallback(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "NOT_FOUND", "message": "no route"},
		})
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
	require.NoError(t, logsCmd.RunE(logsCmd, nil))

	assert.Contains(t, buf.String(), "kubectl logs")
	assert.Contains(t, buf.String(), "job/test-cluster-backup-1")
}

// ---------------------------------------------------------------------------
// Storage commands with mock server
// ---------------------------------------------------------------------------

func TestStorageCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("disk-usage", func(t *testing.T) {
		cmd := newStorageCmd()
		sub := findSubcommand(cmd, "disk-usage")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("usage-report", func(t *testing.T) {
		cmd := newStorageCmd()
		sub := findSubcommand(cmd, "usage-report")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("tables list", func(t *testing.T) {
		cmd := newStorageCmd()
		tablesCmd := findSubcommand(cmd, "tables")
		require.NotNil(t, tablesCmd)
		listCmd := findSubcommand(tablesCmd, "list")
		require.NotNil(t, listCmd)
		err := listCmd.RunE(listCmd, nil)
		require.NoError(t, err)
	})

	t.Run("tables detail", func(t *testing.T) {
		cmd := newStorageCmd()
		tablesCmd := findSubcommand(cmd, "tables")
		require.NotNil(t, tablesCmd)
		detailCmd := findSubcommand(tablesCmd, "detail")
		require.NotNil(t, detailCmd)
		err := detailCmd.RunE(detailCmd, []string{"public", "users"})
		require.NoError(t, err)
	})

	t.Run("recommendations list", func(t *testing.T) {
		cmd := newStorageCmd()
		recCmd := findSubcommand(cmd, "recommendations")
		require.NotNil(t, recCmd)
		listCmd := findSubcommand(recCmd, "list")
		require.NotNil(t, listCmd)
		err := listCmd.RunE(listCmd, nil)
		require.NoError(t, err)
	})

	t.Run("recommendations scan", func(t *testing.T) {
		cmd := newStorageCmd()
		recCmd := findSubcommand(cmd, "recommendations")
		require.NotNil(t, recCmd)
		scanCmd := findSubcommand(recCmd, "scan")
		require.NotNil(t, scanCmd)
		err := scanCmd.RunE(scanCmd, nil)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Data loading commands with mock server
// ---------------------------------------------------------------------------

func TestDataLoadingCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("status", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		sub := findSubcommand(cmd, "status")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("jobs list", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		jobsCmd := findSubcommand(cmd, "jobs")
		require.NotNil(t, jobsCmd)
		listCmd := findSubcommand(jobsCmd, "list")
		require.NotNil(t, listCmd)
		err := listCmd.RunE(listCmd, nil)
		require.NoError(t, err)
	})

	t.Run("jobs create", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		jobsCmd := findSubcommand(cmd, "jobs")
		require.NotNil(t, jobsCmd)
		createCmd := findSubcommand(jobsCmd, "create")
		require.NotNil(t, createCmd)
		err := createCmd.RunE(createCmd, nil)
		require.NoError(t, err)
	})

	t.Run("jobs start", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		jobsCmd := findSubcommand(cmd, "jobs")
		require.NotNil(t, jobsCmd)
		startCmd := findSubcommand(jobsCmd, "start")
		require.NotNil(t, startCmd)
		err := startCmd.RunE(startCmd, []string{"job-1"})
		require.NoError(t, err)
	})

	t.Run("jobs stop", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		jobsCmd := findSubcommand(cmd, "jobs")
		require.NotNil(t, jobsCmd)
		stopCmd := findSubcommand(jobsCmd, "stop")
		require.NotNil(t, stopCmd)
		err := stopCmd.RunE(stopCmd, []string{"job-1"})
		require.NoError(t, err)
	})

	t.Run("jobs delete", func(t *testing.T) {
		cmd := newDataLoadingCmd()
		jobsCmd := findSubcommand(cmd, "jobs")
		require.NotNil(t, jobsCmd)
		deleteCmd := findSubcommand(jobsCmd, "delete")
		require.NotNil(t, deleteCmd)
		err := deleteCmd.RunE(deleteCmd, []string{"job-1"})
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Sessions commands with mock server
// ---------------------------------------------------------------------------

func TestSessionsCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("list", func(t *testing.T) {
		cmd := newSessionsCmd()
		sub := findSubcommand(cmd, "list")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("cancel-query", func(t *testing.T) {
		cmd := newSessionsCmd()
		sub := findSubcommand(cmd, "cancel-query")
		require.NotNil(t, sub)
		err := sub.RunE(sub, []string{"1234"})
		require.NoError(t, err)
	})

	t.Run("terminate", func(t *testing.T) {
		cmd := newSessionsCmd()
		sub := findSubcommand(cmd, "terminate")
		require.NotNil(t, sub)
		err := sub.RunE(sub, []string{"1234"})
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Query commands with mock server
// ---------------------------------------------------------------------------

func TestQueryCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	subcmds := []string{"active", "slow", "status"}
	for _, name := range subcmds {
		t.Run(name, func(t *testing.T) {
			cmd := newQueryCmd()
			sub := findSubcommand(cmd, name)
			require.NotNil(t, sub)
			err := sub.RunE(sub, nil)
			require.NoError(t, err)
		})
	}

	// "history" is a command group — test its "list" subcommand.
	t.Run("history/list", func(t *testing.T) {
		cmd := newQueryCmd()
		historySub := findSubcommand(cmd, "history")
		require.NotNil(t, historySub)
		listSub := findSubcommand(historySub, "list")
		require.NotNil(t, listSub)
		err := listSub.RunE(listSub, nil)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Maintenance commands with mock server
// ---------------------------------------------------------------------------

func TestMaintenanceCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	subcmds := []string{"vacuum", "analyze", "reindex"}
	for _, name := range subcmds {
		t.Run(name, func(t *testing.T) {
			cmd := newMaintenanceCmd()
			sub := findSubcommand(cmd, name)
			require.NotNil(t, sub)
			err := sub.RunE(sub, nil)
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Resource group/queue commands with mock server
// ---------------------------------------------------------------------------

func TestResourceGroupCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("list", func(t *testing.T) {
		cmd := newResourceGroupCmd()
		sub := findSubcommand(cmd, "list")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("create", func(t *testing.T) {
		cmd := newResourceGroupCmd()
		sub := findSubcommand(cmd, "create")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		cmd := newResourceGroupCmd()
		sub := findSubcommand(cmd, "delete")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("assign", func(t *testing.T) {
		cmd := newResourceGroupCmd()
		sub := findSubcommand(cmd, "assign")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})
}

func TestResourceQueueCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("list", func(t *testing.T) {
		cmd := newResourceQueueCmd()
		sub := findSubcommand(cmd, "list")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("create", func(t *testing.T) {
		cmd := newResourceQueueCmd()
		sub := findSubcommand(cmd, "create")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		cmd := newResourceQueueCmd()
		sub := findSubcommand(cmd, "delete")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Workload rules export with mock server
// ---------------------------------------------------------------------------

func TestWorkloadRulesExportCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"name":   "rule1",
					"action": "cancel",
				},
			},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("export to stdout", func(t *testing.T) {
		cmd := newWorkloadRulesExportCmd()
		err := cmd.RunE(cmd, nil)
		require.NoError(t, err)
	})

	t.Run("export to file", func(t *testing.T) {
		tmpFile := fmt.Sprintf("%s/test-rules-export.yaml", os.TempDir())
		defer os.Remove(tmpFile)

		cmd := newWorkloadRulesExportCmd()
		_ = cmd.Flags().Set("output-file", tmpFile)
		err := cmd.RunE(cmd, nil)
		require.NoError(t, err)

		// Verify file was created.
		_, statErr := os.Stat(tmpFile)
		assert.NoError(t, statErr)
	})
}

// ---------------------------------------------------------------------------
// Workload rules import with mock server
// ---------------------------------------------------------------------------

func TestWorkloadRulesImportCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	// Create a temp rules file.
	tmpFile := fmt.Sprintf("%s/test-rules-import.yaml", os.TempDir())
	defer os.Remove(tmpFile)
	content := `- name: test-rule
  action: cancel
  enabled: true
  threshold: "100"
  thresholdType: duration
`
	require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0o644))

	cmd := newWorkloadRulesImportCmd()
	_ = cmd.Flags().Set("file", tmpFile)
	err := cmd.RunE(cmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Standby and mirroring commands with mock server
// ---------------------------------------------------------------------------

func TestStandbyCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("status", func(t *testing.T) {
		cmd := newStandbyCmd()
		sub := findSubcommand(cmd, "status")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("activate", func(t *testing.T) {
		cmd := newStandbyCmd()
		sub := findSubcommand(cmd, "activate")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})
}

func TestMirroringCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("status", func(t *testing.T) {
		cmd := newMirroringCmd()
		sub := findSubcommand(cmd, "status")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Auth login command RunE paths
// ---------------------------------------------------------------------------

func TestAuthLoginCmd_BasicFlag(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "admin"
	globals.password = "secret"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "basic"
	globals.output = "table"

	cmd := newAuthLoginCmd()
	_ = cmd.Flags().Set("basic", "true")
	err := cmd.RunE(cmd, nil)
	require.NoError(t, err)
}

func TestAuthLoginCmd_OIDCWithCredentials(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.username = "oidc-user"
	globals.password = "oidc-token"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.authMethod = "oidc"
	globals.output = "table"

	cmd := newAuthLoginCmd()
	// No --basic flag, so it uses OIDC path with credentials.
	err := cmd.RunE(cmd, nil)
	require.NoError(t, err)
}

func TestAuthLoginCmd_OIDCWithoutCredentials_MissingIssuerURL(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.username = ""
	globals.password = ""
	globals.issuerURL = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	cmd := newAuthLoginCmd()
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer URL is required")
}

// ---------------------------------------------------------------------------
// Auth rotate-password command
// ---------------------------------------------------------------------------

func TestAuthRotatePasswordCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "rotated"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newAuthCmd()
	rotateCmd := findSubcommand(cmd, "rotate-password")
	require.NotNil(t, rotateCmd)

	err := rotateCmd.RunE(rotateCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Workload rules list with mock server
// ---------------------------------------------------------------------------

func TestWorkloadRulesListCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{"name": "rule1", "action": "cancel"},
			},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newWorkloadRulesCmd()
	listCmd := findSubcommand(cmd, "list")
	require.NotNil(t, listCmd)

	err := listCmd.RunE(listCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Workload resource-groups list with mock server
// ---------------------------------------------------------------------------

func TestWorkloadResourceGroupsListCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"groups": []interface{}{},
		})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newWorkloadResourceGroupsCmd()
	listCmd := findSubcommand(cmd, "list")
	require.NotNil(t, listCmd)

	err := listCmd.RunE(listCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Workload resource-groups create with mock server
// ---------------------------------------------------------------------------

func TestWorkloadResourceGroupsCreateCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newWorkloadResourceGroupsCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	_ = createCmd.Flags().Set("name", "analytics")
	_ = createCmd.Flags().Set("concurrency", "10")
	err := createCmd.RunE(createCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Cluster start/stop/restart/delete with mock server
// ---------------------------------------------------------------------------

func TestClusterStartCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "started"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	startCmd := findSubcommand(cmd, "start")
	require.NotNil(t, startCmd)

	err := startCmd.RunE(startCmd, nil)
	require.NoError(t, err)
}

func TestClusterStopCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	stopCmd := findSubcommand(cmd, "stop")
	require.NotNil(t, stopCmd)

	err := stopCmd.RunE(stopCmd, nil)
	require.NoError(t, err)
}

func TestClusterRestartCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "restarted"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	restartCmd := findSubcommand(cmd, "restart")
	require.NotNil(t, restartCmd)

	err := restartCmd.RunE(restartCmd, nil)
	require.NoError(t, err)
}

func TestClusterDeleteCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	deleteCmd := findSubcommand(cmd, "delete")
	require.NotNil(t, deleteCmd)

	err := deleteCmd.RunE(deleteCmd, nil)
	require.NoError(t, err)
}

func TestClusterScaleStatusCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newClusterCmd()
	scaleStatusCmd := findSubcommand(cmd, "scale-status")
	require.NotNil(t, scaleStatusCmd)

	err := scaleStatusCmd.RunE(scaleStatusCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Config get/reload with mock server
// ---------------------------------------------------------------------------

func TestConfigGetCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"max_connections": "200"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newConfigCmd()
	getCmd := findSubcommand(cmd, "get")
	require.NotNil(t, getCmd)

	err := getCmd.RunE(getCmd, nil)
	require.NoError(t, err)
}

func TestConfigReloadCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "reloaded"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newConfigCmd()
	reloadCmd := findSubcommand(cmd, "reload")
	require.NotNil(t, reloadCmd)

	err := reloadCmd.RunE(reloadCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Segments commands with mock server
// ---------------------------------------------------------------------------

func TestSegmentsCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	subcmds := []string{"list", "status", "inspect"}
	for _, name := range subcmds {
		t.Run(name, func(t *testing.T) {
			cmd := newSegmentsCmd()
			sub := findSubcommand(cmd, name)
			require.NotNil(t, sub)
			err := sub.RunE(sub, nil)
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Workload rules create with valid file
// ---------------------------------------------------------------------------

func TestWorkloadRulesCreateCmd_WithValidFile(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	// Create a temp rule file.
	tmpFile := fmt.Sprintf("%s/test-rule-create.yaml", os.TempDir())
	defer os.Remove(tmpFile)
	content := `name: test-rule
action: cancel
enabled: true
threshold: "100"
thresholdType: duration
`
	require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0o644))

	cmd := newWorkloadRulesCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	_ = createCmd.Flags().Set("file", tmpFile)
	err := createCmd.RunE(createCmd, nil)
	require.NoError(t, err)
}

func TestRecoveryCommands_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	t.Run("start", func(t *testing.T) {
		cmd := newRecoveryCmd()
		sub := findSubcommand(cmd, "start")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})

	t.Run("status", func(t *testing.T) {
		cmd := newRecoveryCmd()
		sub := findSubcommand(cmd, "status")
		require.NotNil(t, sub)
		err := sub.RunE(sub, nil)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Additional requireCluster tests for commands
// ---------------------------------------------------------------------------

func TestAdditionalClusterRequiredCommands(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	tests := []struct {
		name    string
		cmdFunc func() *cobra.Command
		subcmd  string
	}{
		{"inspect disk-usage", newInspectCmd, "disk-usage"},
		{"inspect skew", newInspectCmd, "skew"},
		{"inspect bloat", newInspectCmd, "bloat"},
		{"inspect missing-stats", newInspectCmd, "missing-stats"},
		{"inspect connections", newInspectCmd, "connections"},
		{"inspect locks", newInspectCmd, "locks"},
		{"inspect logs", newInspectCmd, "logs"},
		{"queries active", newQueryCmd, "active"},
		{"queries slow", newQueryCmd, "slow"},
		// "history" is a command group — skip it here; tested separately below.
		{"queries status", newQueryCmd, "status"},
		{"backup create", newBackupCmd, "create"},
		{"backup list", newBackupCmd, "list"},
		{"backup status", newBackupCmd, "status"},
		{"backup schedule", newBackupCmd, "schedule"},
		{"storage disk-usage", newStorageCmd, "disk-usage"},
		{"storage usage-report", newStorageCmd, "usage-report"},
		{"resource-group list", newResourceGroupCmd, "list"},
		{"resource-group create", newResourceGroupCmd, "create"},
		{"resource-group delete", newResourceGroupCmd, "delete"},
		{"resource-group assign", newResourceGroupCmd, "assign"},
		{"resource-queue list", newResourceQueueCmd, "list"},
		{"resource-queue create", newResourceQueueCmd, "create"},
		{"resource-queue delete", newResourceQueueCmd, "delete"},
		{"data-loading status", newDataLoadingCmd, "status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := tt.cmdFunc()
			sub := findSubcommand(cmd, tt.subcmd)
			require.NotNil(t, sub, "should have %q subcommand", tt.subcmd)

			err := sub.RunE(sub, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cluster name is required")
		})
	}

	// Test "queries history" subcommands (history is a command group).
	historySubcmds := []string{"list", "detail", "export"}
	for _, name := range historySubcmds {
		t.Run("queries history/"+name, func(t *testing.T) {
			cmd := newQueryCmd()
			historySub := findSubcommand(cmd, "history")
			require.NotNil(t, historySub, "should have history subcommand")
			sub := findSubcommand(historySub, name)
			require.NotNil(t, sub, "should have %q subcommand under history", name)
			err := sub.RunE(sub, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cluster name is required")
		})
	}
}

// ---------------------------------------------------------------------------
// Workload commands with mock server - additional paths
// ---------------------------------------------------------------------------

func TestWorkloadStatusCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newWorkloadCmd()
	statusCmd := findSubcommand(cmd, "status")
	require.NotNil(t, statusCmd)

	err := statusCmd.RunE(statusCmd, nil)
	require.NoError(t, err)
}

func TestWorkloadIdleRulesCmd_WithMockServer(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.cluster = "test-cluster"
	globals.namespace = "test-ns"
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newWorkloadCmd()
	idleRulesCmd := findSubcommand(cmd, "idle-rules")
	require.NotNil(t, idleRulesCmd)

	err := idleRulesCmd.RunE(idleRulesCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Sessions cancel-query and terminate require cluster
// ---------------------------------------------------------------------------

func TestSessionsCommands_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	tests := []struct {
		name   string
		subcmd string
	}{
		{"cancel-query", "cancel-query"},
		{"terminate", "terminate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newSessionsCmd()
			sub := findSubcommand(cmd, tt.subcmd)
			require.NotNil(t, sub)
			err := sub.RunE(sub, []string{"1234"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cluster name is required")
		})
	}
}

// ---------------------------------------------------------------------------
// Auth logout command
// ---------------------------------------------------------------------------

func TestAuthLogoutCmd_Execute(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.output = "table"

	cmd := newAuthCmd()
	logoutCmd := findSubcommand(cmd, "logout")
	require.NotNil(t, logoutCmd)

	err := logoutCmd.RunE(logoutCmd, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Auth status command
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Data loading jobs subcommands require cluster
// ---------------------------------------------------------------------------

func TestDataLoadingJobsCommands_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	tests := []struct {
		name   string
		subcmd string
		args   []string
	}{
		{"list", "list", nil},
		{"create", "create", nil},
		{"start", "start", []string{"job-1"}},
		{"stop", "stop", []string{"job-1"}},
		{"delete", "delete", []string{"job-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newDataLoadingCmd()
			jobsCmd := findSubcommand(cmd, "jobs")
			require.NotNil(t, jobsCmd)
			sub := findSubcommand(jobsCmd, tt.subcmd)
			require.NotNil(t, sub)
			err := sub.RunE(sub, tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cluster name is required")
		})
	}
}

// ---------------------------------------------------------------------------
// Storage tables subcommands require cluster
// ---------------------------------------------------------------------------

func TestStorageTablesCommands_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	t.Run("list", func(t *testing.T) {
		cmd := newStorageCmd()
		tablesCmd := findSubcommand(cmd, "tables")
		require.NotNil(t, tablesCmd)
		listCmd := findSubcommand(tablesCmd, "list")
		require.NotNil(t, listCmd)
		err := listCmd.RunE(listCmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cluster name is required")
	})

	t.Run("detail", func(t *testing.T) {
		cmd := newStorageCmd()
		tablesCmd := findSubcommand(cmd, "tables")
		require.NotNil(t, tablesCmd)
		detailCmd := findSubcommand(tablesCmd, "detail")
		require.NotNil(t, detailCmd)
		err := detailCmd.RunE(detailCmd, []string{"public", "users"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cluster name is required")
	})
}

// ---------------------------------------------------------------------------
// Storage recommendations subcommands require cluster
// ---------------------------------------------------------------------------

func TestStorageRecommendationsCommands_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""
	globals.operatorURL = "http://localhost:8443"
	globals.timeout = "5s"

	t.Run("list", func(t *testing.T) {
		cmd := newStorageCmd()
		recCmd := findSubcommand(cmd, "recommendations")
		require.NotNil(t, recCmd)
		listCmd := findSubcommand(recCmd, "list")
		require.NotNil(t, listCmd)
		err := listCmd.RunE(listCmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cluster name is required")
	})

	t.Run("scan", func(t *testing.T) {
		cmd := newStorageCmd()
		recCmd := findSubcommand(cmd, "recommendations")
		require.NotNil(t, recCmd)
		scanCmd := findSubcommand(recCmd, "scan")
		require.NotNil(t, scanCmd)
		err := scanCmd.RunE(scanCmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cluster name is required")
	})
}

// ---------------------------------------------------------------------------
// Workload rules require cluster
// ---------------------------------------------------------------------------

func TestWorkloadRulesListCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadRulesCmd()
	listCmd := findSubcommand(cmd, "list")
	require.NotNil(t, listCmd)

	err := listCmd.RunE(listCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

func TestWorkloadRulesCreateCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadRulesCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	err := createCmd.RunE(createCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

func TestWorkloadRulesImportCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadRulesImportCmd()
	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

// ---------------------------------------------------------------------------
// Workload resource-groups require cluster
// ---------------------------------------------------------------------------

func TestWorkloadResourceGroupsListCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadResourceGroupsCmd()
	listCmd := findSubcommand(cmd, "list")
	require.NotNil(t, listCmd)

	err := listCmd.RunE(listCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

func TestWorkloadResourceGroupsCreateCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newWorkloadResourceGroupsCmd()
	createCmd := findSubcommand(cmd, "create")
	require.NotNil(t, createCmd)

	err := createCmd.RunE(createCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

// ---------------------------------------------------------------------------
// Backup commands require cluster
// ---------------------------------------------------------------------------

func TestBackupDeleteCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newBackupCmd()
	deleteCmd := findSubcommand(cmd, "delete")
	require.NotNil(t, deleteCmd)

	err := deleteCmd.RunE(deleteCmd, []string{"backup-123"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

func TestBackupRestoreCmd_RequireCluster(t *testing.T) {
	saved := globals
	defer func() { globals = saved }()
	globals.cluster = ""

	cmd := newBackupCmd()
	restoreCmd := findSubcommand(cmd, "restore")
	require.NotNil(t, restoreCmd)

	err := restoreCmd.RunE(restoreCmd, []string{"backup-123"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster name is required")
}

func TestAuthStatusCmd_Execute(t *testing.T) {
	server := setupMockServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}})
	})

	saved := globals
	defer func() { globals = saved }()
	globals.operatorURL = server.URL
	globals.timeout = "5s"
	globals.username = "admin"
	globals.password = "pass"
	globals.authMethod = "basic"
	globals.output = "json"

	cmd := newAuthCmd()
	statusCmd := findSubcommand(cmd, "status")
	require.NotNil(t, statusCmd)

	err := statusCmd.RunE(statusCmd, nil)
	require.NoError(t, err)
}
