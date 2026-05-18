// Package main is the entry point for the cloudberry-ctl CLI utility.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
)

const appName = "cloudberry-ctl"

// appendNamespaceQuery appends the namespace query parameter to a path if namespace is non-empty.
func appendNamespaceQuery(path, namespace string) string {
	if namespace != "" {
		path += "?" + url.Values{"namespace": {namespace}}.Encode()
	}
	return path
}

// notImplemented returns a standardized error for commands that are not yet implemented.
func notImplemented(name string) error {
	return fmt.Errorf("command %q is not yet implemented", name)
}

// version is set via ldflags at build time (e.g. -X main.version=...).
var version = "dev" //nolint:gochecknoglobals // set by ldflags

// Exit codes.
const (
	exitSuccess          = 0
	exitGeneralError     = 1
	exitInvalidArgs      = 2
	exitAuthFailure      = 3
	exitPermissionDenied = 4
	exitClusterNotFound  = 5
	exitTimeout          = 6
	exitConnectionError  = 7
)

// Command name constants to avoid string duplication.
const (
	cmdStatus = "status"
	cmdList   = "list"
	cmdCreate = "create"
	cmdUpdate = "update"
	cmdDelete = "delete"
	cmdStart  = "start"
	cmdStop   = "stop"
)

// globalFlags holds the global CLI flags.
type globalFlags struct {
	cluster     string
	namespace   string
	kubeconfig  string
	context     string
	operatorURL string
	authMethod  string
	username    string
	password    string
	output      string
	verbose     bool
	timeout     string
}

var globals globalFlags

func main() {
	if err := newRootCmd().Execute(); err != nil {
		var apiErr *ctl.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.StatusCode {
			case http.StatusUnauthorized:
				os.Exit(exitAuthFailure)
			case http.StatusForbidden:
				os.Exit(exitPermissionDenied)
			case http.StatusNotFound:
				os.Exit(exitClusterNotFound)
			case http.StatusRequestTimeout:
				os.Exit(exitTimeout)
			case http.StatusTooManyRequests:
				os.Exit(exitTimeout)
			default:
				os.Exit(exitGeneralError)
			}
		}
		os.Exit(exitGeneralError)
	}
}

// newClient creates an OperatorClient from the global flags.
func newClient() (*ctl.OperatorClient, error) {
	if globals.operatorURL == "" {
		return nil, fmt.Errorf("operator URL is required (set --operator-url or CLOUDBERRY_OPERATOR_URL)")
	}

	timeout, err := time.ParseDuration(globals.timeout)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout %q: %w", globals.timeout, err)
	}

	return ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    globals.operatorURL,
		Username:   globals.username,
		Password:   globals.password,
		AuthMethod: globals.authMethod,
		Timeout:    timeout,
		Verbose:    globals.verbose,
	}), nil
}

// newFormatter creates a Formatter from the global flags.
func newFormatter() *ctl.Formatter {
	return ctl.NewFormatter(globals.output, os.Stdout)
}

// requireCluster returns an error if the cluster flag is not set.
func requireCluster() error {
	if globals.cluster == "" {
		return fmt.Errorf("cluster name is required (set --cluster or CLOUDBERRY_CLUSTER)")
	}
	return nil
}

// cmdContext creates a context that respects both the configured timeout and
// OS signals (SIGINT, SIGTERM). Pressing Ctrl+C will cancel in-flight API
// requests. The returned cancel function releases both the signal handler
// and the timeout resources.
func cmdContext() (context.Context, context.CancelFunc) {
	sigCtx, sigCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	timeout, err := time.ParseDuration(globals.timeout)
	if err != nil {
		timeout = 5 * time.Minute
	}
	tCtx, tCancel := context.WithTimeout(sigCtx, timeout)
	return tCtx, func() {
		tCancel()
		sigCancel()
	}
}

// runAPIGet is a helper that creates a client, performs a GET request, and formats the output.
func runAPIGet(path string) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	resp, apiErr := client.Get(ctx, path)
	if apiErr != nil {
		return apiErr
	}
	return newFormatter().Format(resp.Body)
}

// runAPIPost is a helper that creates a client, performs a POST request, and formats the output.
func runAPIPost(path string, body interface{}) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	resp, apiErr := client.Post(ctx, path, body)
	if apiErr != nil {
		return apiErr
	}
	return newFormatter().Format(resp.Body)
}

// runAPIDelete is a helper that creates a client, performs a DELETE request, and formats the output.
func runAPIDelete(path string) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	resp, apiErr := client.Delete(ctx, path)
	if apiErr != nil {
		return apiErr
	}
	return newFormatter().Format(resp.Body)
}

// newRootCmd creates the root command.
func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   appName,
		Short: "Cloudberry Database cluster management CLI",
		Long: `cloudberry-ctl is a command-line utility that provides imperative access
to Cloudberry cluster management operations through the Cloudberry Operator API.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Store reference for initConfig to access flag.Changed.
	rootCmd = cmd

	// Bind global flags.
	pf := cmd.PersistentFlags()
	pf.StringVar(&globals.cluster, "cluster", "", "Target cluster name")
	pf.StringVar(&globals.namespace, "namespace", "cloudberry-test", "Kubernetes namespace")
	pf.StringVar(&globals.kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	pf.StringVar(&globals.context, "context", "", "Kubernetes context")
	pf.StringVar(&globals.operatorURL, "operator-url", "", "Operator API URL")
	pf.StringVar(&globals.authMethod, "auth-method", "basic", "Auth method (basic/oidc)")
	pf.StringVar(&globals.username, "username", "", "Basic auth username")
	pf.StringVar(&globals.password, "password", "", "Basic auth password")
	pf.StringVarP(&globals.output, "output", "o", "table", "Output format (table/json/yaml)")
	pf.BoolVarP(&globals.verbose, "verbose", "v", false, "Enable verbose output")
	pf.StringVar(&globals.timeout, "timeout", "5m", "Operation timeout")

	// Bind environment variables.
	bindEnvVars()

	// Load config file.
	cobra.OnInitialize(initConfig)

	// Add subcommands.
	cmd.AddCommand(
		newVersionCmd(),
		newClusterCmd(),
		newConfigCmd(),
		newSegmentsCmd(),
		newHACmd(),
		newSessionsCmd(),
		newMaintenanceCmd(),
		newAuthCmd(),
		newInspectCmd(),
		newResourceGroupCmd(),
		newResourceQueueCmd(),
		newWorkloadCmd(),
		newQueryCmd(),
		newBackupCmd(),
		newDataLoadingCmd(),
		newStorageCmd(),
		newCompletionCmd(),
	)

	return cmd
}

// bindEnvVars binds environment variables to flags.
func bindEnvVars() {
	viper.SetEnvPrefix("CLOUDBERRY")
	viper.AutomaticEnv()

	// Bind specific env vars.
	envBindings := map[string]string{
		"cluster":      "CLOUDBERRY_CLUSTER",
		"namespace":    "CLOUDBERRY_NAMESPACE",
		"operator-url": "CLOUDBERRY_OPERATOR_URL",
		"auth-method":  "CLOUDBERRY_AUTH_METHOD",
		"username":     "CLOUDBERRY_USERNAME",
		"password":     "CLOUDBERRY_PASSWORD",
		"timeout":      "CLOUDBERRY_TIMEOUT",
		"output":       "CLOUDBERRY_OUTPUT",
	}

	for flag, env := range envBindings {
		if val := os.Getenv(env); val != "" {
			viper.Set(flag, val)
		}
	}
}

// rootCmd is stored so initConfig can access flag.Changed.
var rootCmd *cobra.Command //nolint:gochecknoglobals // needed for initConfig

// applyViperValue applies the viper value to dst only if the flag was not
// explicitly set on the command line. This ensures the priority order:
// CLI flag > env var > config file > default.
func applyViperValue(dst *string, flagName, viperKey string) {
	if rootCmd != nil && rootCmd.PersistentFlags().Changed(flagName) {
		return // user explicitly set this flag on the command line
	}
	if v := viper.GetString(viperKey); v != "" {
		*dst = v
	}
}

// initConfig reads the config file and applies viper values to global flags.
// Priority order: CLI flag > env var > config file > default.
func initConfig() {
	viper.SetConfigName(".cloudberry-ctl")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME")
	viper.AddConfigPath(".")

	// Config file is optional.
	_ = viper.ReadInConfig()

	// Apply viper values (env vars + config file) to globals struct,
	// but only when the flag was not explicitly set on the command line.
	applyViperValue(&globals.cluster, "cluster", "cluster")
	applyViperValue(&globals.namespace, "namespace", "namespace")
	applyViperValue(&globals.operatorURL, "operator-url", "operator-url")
	applyViperValue(&globals.authMethod, "auth-method", "auth-method")
	applyViperValue(&globals.username, "username", "username")
	applyViperValue(&globals.password, "password", "password")
	applyViperValue(&globals.timeout, "timeout", "timeout")
	applyViperValue(&globals.output, "output", "output")

	// Warn if password was provided via the --password CLI flag (visible in
	// process listings and shell history). Environment variable is preferred.
	if rootCmd != nil && rootCmd.PersistentFlags().Changed("password") {
		fmt.Fprintln(os.Stderr,
			"WARNING: Password provided via --password flag is visible in process "+
				"listings and shell history. Use the CLOUDBERRY_PASSWORD environment "+
				"variable instead for improved security.")
	}
}

// newVersionCmd creates the version command.
func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Fprintf(os.Stdout, "%s version %s\n", appName, version)
		},
	}
}

// newCompletionCmd creates the completion command.
func newCompletionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish]",
		Short: "Generate shell completion scripts",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			default:
				return fmt.Errorf("unsupported shell: %s", args[0])
			}
		},
	}
	return cmd
}

// newClusterCmd creates the cluster command group.
func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster lifecycle management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show cluster status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterStatusPath(globals.cluster, globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdStart,
			Short: "Start cluster",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterActionPath(globals.cluster, "start", globals.namespace), nil)
			},
		},
		&cobra.Command{
			Use:   cmdStop,
			Short: "Stop cluster",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterActionPath(globals.cluster, "stop", globals.namespace), nil)
			},
		},
		&cobra.Command{
			Use:   "restart",
			Short: "Restart cluster",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterActionPath(globals.cluster, "restart", globals.namespace), nil)
			},
		},
		&cobra.Command{
			Use:   cmdCreate,
			Short: "Create cluster from spec",
			RunE: func(_ *cobra.Command, _ []string) error {
				return runAPIPost(ctl.ClustersPath(), nil)
			},
		},
		&cobra.Command{
			Use:   cmdDelete,
			Short: "Delete cluster",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIDelete(ctl.ClusterPath(globals.cluster, globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "scale-status",
			Short: "Show scale operation status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "scale/status", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "upgrade",
			Short: "Upgrade cluster version",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("cluster upgrade")
			},
		},
	)

	return cmd
}

// newConfigCmd creates the config command group.
func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configuration management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "get",
			Short: "Get parameter value(s)",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "config", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "set",
			Short: "Set parameter value",
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				if len(args) < 2 {
					return fmt.Errorf("usage: config set <key> <value>")
				}
				body := map[string]interface{}{
					"parameters": map[string]string{args[0]: args[1]},
				}
				client, err := newClient()
				if err != nil {
					return err
				}
				ctx, cancel := cmdContext()
				defer cancel()
				configPath := ctl.ClusterSubresourcePath(
					globals.cluster, "config", globals.namespace,
				)
				resp, apiErr := client.Put(ctx, configPath, body)
				if apiErr != nil {
					return apiErr
				}
				return newFormatter().Format(resp.Body)
			},
		},
		&cobra.Command{
			Use:   "reset",
			Short: "Reset parameter to default",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("config reset")
			},
		},
		&cobra.Command{
			Use:   "reload",
			Short: "Reload configuration",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterActionPath(globals.cluster, "reload", globals.namespace), nil)
			},
		},
		newHBACmd(),
	)

	return cmd
}

// newHBACmd creates the HBA subcommand group.
func newHBACmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hba",
		Short: "HBA rules management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List HBA rules",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("hba list")
			},
		},
		&cobra.Command{
			Use:   cmdUpdate,
			Short: "Update HBA rules",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("hba update")
			},
		},
		&cobra.Command{
			Use:   "history",
			Short: "View HBA change history",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("hba history")
			},
		},
	)

	return cmd
}

// newSegmentsCmd creates the segments command group.
func newSegmentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segments",
		Short: "Segment management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List all segments",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "segments", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show segment status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "segments", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "inspect",
			Short: "Detailed segment info",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "segments", globals.namespace))
			},
		},
	)

	return cmd
}

// newHACmd creates the HA command group.
func newHACmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ha",
		Short: "High availability management",
	}

	rebalanceCmd := &cobra.Command{
		Use:   "rebalance",
		Short: "Rebalance segments",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}

			statusFlag, _ := cmd.Flags().GetBool("status")
			if statusFlag {
				return runAPIGet(ctl.ClusterSubresourcePath(
					globals.cluster, "rebalance/status", globals.namespace))
			}

			tables, _ := cmd.Flags().GetString("tables")
			if tables != "" {
				body := map[string]interface{}{
					"tables": strings.Split(tables, ","),
				}
				return runAPIPost(ctl.ClusterActionPath(
					globals.cluster, "rebalance", globals.namespace), body)
			}

			return runAPIPost(ctl.ClusterActionPath(
				globals.cluster, "rebalance", globals.namespace), nil)
		},
	}
	rebalanceCmd.Flags().Bool("status", false, "Show rebalance status")
	rebalanceCmd.Flags().String("tables", "", "Comma-separated list of tables to rebalance")

	cmd.AddCommand(
		newMirroringCmd(),
		newRecoveryCmd(),
		newStandbyCmd(),
		newFTSCmd(),
		rebalanceCmd,
	)

	return cmd
}

// newMirroringCmd creates the mirroring subcommand group.
func newMirroringCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mirroring",
		Short: "Mirroring management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show mirroring status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "mirroring", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "enable",
			Short: "Enable mirroring",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("mirroring enable")
			},
		},
		&cobra.Command{
			Use:   "disable",
			Short: "Disable mirroring",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("mirroring disable")
			},
		},
	)

	return cmd
}

// newRecoveryCmd creates the recovery subcommand group.
func newRecoveryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recovery",
		Short: "Segment recovery",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStart,
			Short: "Start recovery",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				body := map[string]string{"type": "incremental"}
				return runAPIPost(ctl.ClusterSubresourcePath(globals.cluster, "recovery", globals.namespace), body)
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show recovery status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterStatusPath(globals.cluster, globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "cancel",
			Short: "Cancel recovery",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("recovery cancel")
			},
		},
	)

	return cmd
}

// newStandbyCmd creates the standby subcommand group.
func newStandbyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "standby",
		Short: "Coordinator standby management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show standby status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "standby", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "activate",
			Short: "Activate standby",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "standby/activate", globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
		&cobra.Command{
			Use:   "reinitialize",
			Short: "Reinitialize standby",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("standby reinitialize")
			},
		},
		&cobra.Command{
			Use:   "restore-roles",
			Short: "Restore original roles",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("standby restore-roles")
			},
		},
	)

	return cmd
}

// newFTSCmd creates the FTS subcommand group.
func newFTSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fts",
		Short: "Fault tolerance service",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show FTS status",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("fts status")
			},
		},
		&cobra.Command{
			Use:   "configure",
			Short: "Configure FTS parameters",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("fts configure")
			},
		},
	)

	return cmd
}

// newSessionsCmd creates the sessions command group.
func newSessionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sessions",
		Short: "Session management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List active sessions",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "sessions", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "cancel-query [pid]",
			Short: "Cancel running query",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/sessions/%s/cancel",
						url.PathEscape(globals.cluster),
						url.PathEscape(args[0])),
					globals.namespace)
				return runAPIPost(path, nil)
			},
		},
		&cobra.Command{
			Use:   "terminate [pid]",
			Short: "Terminate session",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/sessions/%s",
						url.PathEscape(globals.cluster),
						url.PathEscape(args[0])),
					globals.namespace)
				return runAPIDelete(path)
			},
		},
	)

	return cmd
}

// newMaintenanceCmd creates the maintenance command group.
func newMaintenanceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Maintenance operations",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "vacuum",
			Short: "Run vacuum",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "maintenance/vacuum", globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
		&cobra.Command{
			Use:   "analyze",
			Short: "Run analyze",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "maintenance/analyze", globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
		&cobra.Command{
			Use:   "reindex",
			Short: "Run reindex",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "maintenance/reindex", globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
		&cobra.Command{
			Use:   "check-catalog",
			Short: "Run catalog check",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("maintenance check-catalog")
			},
		},
		&cobra.Command{
			Use:   "jobs",
			Short: "List maintenance jobs",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("maintenance jobs")
			},
		},
	)

	return cmd
}

// newAuthCmd creates the auth command group.
func newAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "login",
			Short: "Authenticate with operator",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("auth login")
			},
		},
		&cobra.Command{
			Use:   "logout",
			Short: "Clear cached credentials",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("auth logout")
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show auth status",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("auth status")
			},
		},
		&cobra.Command{
			Use:   "rotate-password",
			Short: "Rotate admin password",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("auth rotate-password")
			},
		},
		newRolesCmd(),
	)

	return cmd
}

// newRolesCmd creates the roles subcommand group.
func newRolesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "roles",
		Short: "Manage roles",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List roles",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("roles list")
			},
		},
		&cobra.Command{
			Use:   cmdCreate,
			Short: "Create role",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("roles create")
			},
		},
		&cobra.Command{
			Use:   cmdUpdate,
			Short: "Update role",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("roles update")
			},
		},
		&cobra.Command{
			Use:   cmdDelete,
			Short: "Delete role",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("roles delete")
			},
		},
	)

	return cmd
}

// newInspectCmd creates the inspect command group.
func newInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspection commands",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "disk-usage",
			Short: "Show disk usage",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "storage/disk-usage", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "skew",
			Short: "Show data distribution skew",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "storage/recommendations", globals.namespace,
				)
				return runAPIGet(p)
			},
		},
		&cobra.Command{
			Use:   "bloat",
			Short: "Show table bloat",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "storage/recommendations", globals.namespace,
				)
				return runAPIGet(p)
			},
		},
		&cobra.Command{
			Use:   "missing-stats",
			Short: "Show tables missing stats",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "storage/tables", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "connections",
			Short: "Show connection info",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "sessions", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "locks",
			Short: "Show lock info",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "sessions", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "logs",
			Short: "View server logs",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("inspect logs")
			},
		},
	)

	return cmd
}

// newResourceGroupCmd creates the resource-group command group.
func newResourceGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-group",
		Short: "Resource group management",
	}

	// list subcommand.
	listCmd := &cobra.Command{
		Use:   cmdList,
		Short: "List resource groups",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/resource-groups", globals.namespace))
		},
	}

	// create subcommand with flags.
	var createName string
	var createConcurrency int32
	var createCPUMaxPercent int32
	var createMemoryLimit int32
	createCmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create resource group",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			body := map[string]interface{}{
				"name":          createName,
				"concurrency":   createConcurrency,
				"cpuMaxPercent": createCPUMaxPercent,
				"memoryLimit":   createMemoryLimit,
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/resource-groups", globals.namespace), body)
		},
	}
	createCmd.Flags().StringVar(&createName, "name", "", "Resource group name")
	createCmd.Flags().Int32Var(&createConcurrency, "concurrency", 0, "Concurrency limit")
	createCmd.Flags().Int32Var(&createCPUMaxPercent, "cpu-max-percent", 0, "CPU max percent")
	createCmd.Flags().Int32Var(&createMemoryLimit, "memory-limit", 0, "Memory limit")

	// delete subcommand with flag.
	var deleteName string
	deleteCmd := &cobra.Command{
		Use:   cmdDelete,
		Short: "Delete resource group",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/workload/resource-groups/%s",
					url.PathEscape(globals.cluster), url.PathEscape(deleteName)),
				globals.namespace)
			return runAPIDelete(path)
		},
	}
	deleteCmd.Flags().StringVar(&deleteName, "name", "", "Resource group name to delete")

	// assign subcommand with flags.
	var assignGroup string
	var assignRole string
	assignCmd := &cobra.Command{
		Use:   "assign",
		Short: "Assign role to group",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/workload/resource-groups/%s/assign",
					url.PathEscape(globals.cluster), url.PathEscape(assignGroup)),
				globals.namespace)
			body := map[string]interface{}{
				"role": assignRole,
			}
			return runAPIPost(path, body)
		},
	}
	assignCmd.Flags().StringVar(&assignGroup, "group", "", "Resource group name")
	assignCmd.Flags().StringVar(&assignRole, "role", "", "Role to assign")

	cmd.AddCommand(
		listCmd,
		createCmd,
		&cobra.Command{
			Use:   cmdUpdate,
			Short: "Update resource group",
			RunE: func(_ *cobra.Command, _ []string) error {
				return notImplemented("resource-group update")
			},
		},
		deleteCmd,
		assignCmd,
	)

	return cmd
}

// newResourceQueueCmd creates the resource-queue command group.
func newResourceQueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-queue",
		Short: "Resource queue management",
	}

	// list subcommand.
	listCmd := &cobra.Command{
		Use:   cmdList,
		Short: "List resource queues",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/resource-queues", globals.namespace))
		},
	}

	// create subcommand with flags.
	var createName string
	var createActiveStatements int32
	var createMemoryLimit string
	var createPriority string
	var createMaxCost float64
	createCmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create resource queue",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			body := map[string]interface{}{
				"name":             createName,
				"activeStatements": createActiveStatements,
				"memoryLimit":      createMemoryLimit,
				"priority":         createPriority,
				"maxCost":          createMaxCost,
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/resource-queues", globals.namespace), body)
		},
	}
	createCmd.Flags().StringVar(&createName, "name", "", "Resource queue name")
	createCmd.Flags().Int32Var(&createActiveStatements, "active-statements", 0, "Maximum active statements")
	createCmd.Flags().StringVar(&createMemoryLimit, "memory-limit", "", "Memory limit (e.g., 2GB)")
	createCmd.Flags().StringVar(&createPriority, "priority", "", "Queue priority (LOW, MEDIUM, HIGH, MAX)")
	createCmd.Flags().Float64Var(&createMaxCost, "max-cost", 0, "Maximum query cost")

	// delete subcommand with flag.
	var deleteName string
	deleteCmd := &cobra.Command{
		Use:   cmdDelete,
		Short: "Delete resource queue",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/workload/resource-queues/%s",
					url.PathEscape(globals.cluster), url.PathEscape(deleteName)),
				globals.namespace)
			return runAPIDelete(path)
		},
	}
	deleteCmd.Flags().StringVar(&deleteName, "name", "", "Resource queue name to delete")

	cmd.AddCommand(
		listCmd,
		createCmd,
		deleteCmd,
	)

	return cmd
}

// newWorkloadCmd creates the workload command group.
func newWorkloadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workload",
		Short: "Workload management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show workload management status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "workload", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "rules",
			Short: "List workload rules",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "workload/rules", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "idle-rules",
			Short: "List idle session rules",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "workload", globals.namespace))
			},
		},
	)

	return cmd
}

// newQueryCmd creates the query monitoring command group.
func newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queries",
		Short: "Query monitoring and analysis",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "active",
			Short: "Show active queries",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "queries/active", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "slow",
			Short: "Show slow queries",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "queries", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "history",
			Short: "Show query history",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "queries", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show query monitoring status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "queries", globals.namespace))
			},
		},
	)

	return cmd
}

// newBackupCmd creates the backup command group.
func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup and restore management",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdCreate,
			Short: "Create a new backup",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace), nil)
			},
		},
		&cobra.Command{
			Use:   cmdList,
			Short: "List available backups",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdDelete + " [backup-id]",
			Short: "Delete a backup",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/backups/%s", url.PathEscape(globals.cluster), url.PathEscape(args[0])),
					globals.namespace)
				return runAPIDelete(path)
			},
		},
		&cobra.Command{
			Use:   "restore [backup-id]",
			Short: "Restore from a backup",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/backups/%s/restore",
						url.PathEscape(globals.cluster),
						url.PathEscape(args[0])),
					globals.namespace)
				return runAPIPost(path, nil)
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show backup status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "schedule",
			Short: "Manage backup schedule",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return notImplemented("backup schedule")
			},
		},
	)

	return cmd
}

// newStorageCmd creates the storage management command group.
func newStorageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Storage management and recommendations",
	}

	tablesCmd := &cobra.Command{
		Use:   "tables",
		Short: "Table storage management",
	}

	tablesCmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List tables with storage info",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "storage/tables", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   "detail [schema] [table]",
			Short: "Show table detail",
			Args:  cobra.ExactArgs(2),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/storage/tables/%s/%s",
						url.PathEscape(globals.cluster), url.PathEscape(args[0]), url.PathEscape(args[1])),
					globals.namespace)
				return runAPIGet(path)
			},
		},
	)

	recommendationsCmd := &cobra.Command{
		Use:   "recommendations",
		Short: "Storage recommendations",
	}

	recommendationsCmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List recommendations",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "storage/recommendations",
					globals.namespace,
				)
				return runAPIGet(p)
			},
		},
		&cobra.Command{
			Use:   "scan",
			Short: "Trigger recommendation scan",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "storage/recommendations/scan",
					globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
	)

	cmd.AddCommand(
		&cobra.Command{
			Use:   "disk-usage",
			Short: "Show disk usage",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "storage/disk-usage", globals.namespace))
			},
		},
		tablesCmd,
		recommendationsCmd,
		&cobra.Command{
			Use:   "usage-report",
			Short: "Show usage report",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "storage/usage-report", globals.namespace))
			},
		},
	)

	return cmd
}

// newDataLoadingCmd creates the data loading command group.
func newDataLoadingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data-loading",
		Short: "Data loading management",
	}

	jobsCmd := &cobra.Command{
		Use:   "jobs",
		Short: "Manage data loading jobs",
	}

	jobsCmd.AddCommand(
		&cobra.Command{
			Use:   cmdList,
			Short: "List data loading jobs",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "data-loading/jobs", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdCreate,
			Short: "Create a data loading job",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				p := ctl.ClusterSubresourcePath(
					globals.cluster, "data-loading/jobs",
					globals.namespace,
				)
				return runAPIPost(p, nil)
			},
		},
		&cobra.Command{
			Use:   cmdStart + " [job-name]",
			Short: "Start a data loading job",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/data-loading/jobs/%s/start",
						url.PathEscape(globals.cluster), url.PathEscape(args[0])),
					globals.namespace)
				return runAPIPost(path, nil)
			},
		},
		&cobra.Command{
			Use:   cmdStop + " [job-name]",
			Short: "Stop a data loading job",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/data-loading/jobs/%s/stop",
						url.PathEscape(globals.cluster), url.PathEscape(args[0])),
					globals.namespace)
				return runAPIPost(path, nil)
			},
		},
		&cobra.Command{
			Use:   cmdDelete + " [job-name]",
			Short: "Delete a data loading job",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				path := appendNamespaceQuery(
					fmt.Sprintf("/clusters/%s/data-loading/jobs/%s",
						url.PathEscape(globals.cluster), url.PathEscape(args[0])),
					globals.namespace)
				return runAPIDelete(path)
			},
		},
	)

	cmd.AddCommand(
		jobsCmd,
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show data loading status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "data-loading/jobs", globals.namespace))
			},
		},
	)

	return cmd
}
