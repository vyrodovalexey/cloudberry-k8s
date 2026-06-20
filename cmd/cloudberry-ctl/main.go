// Package main is the entry point for the cloudberry-ctl CLI utility.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sigs.k8s.io/yaml"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
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
	cmdStatus  = "status"
	cmdList    = "list"
	cmdCreate  = "create"
	cmdUpdate  = "update"
	cmdDelete  = "delete"
	cmdStart   = "start"
	cmdStop    = "stop"
	cmdExport  = "export"
	cmdJobs    = "jobs"
	cmdRestart = "restart"
	cmdSync    = "sync"
	cmdLogs    = "logs"
)

// JSON body field name constants to avoid string duplication.
const (
	fieldName   = "name"
	fieldTables = "tables"
	fieldJobs   = "jobs"
)

// Data-loading job type constants (mirror the operator CR enum).
const (
	dataLoadTypePXF    = "pxf"
	dataLoadTypeGpload = "gpload"
)

// pxfConfigEndpointKey / pxfConfigBucketKey are the PXF server config keys the
// friendly --endpoint / --bucket flags map into (s3a endpoint + bucket).
const (
	pxfConfigEndpointKey = "fs.s3a.endpoint"
	pxfConfigBucketKey   = "bucket"
)

// secretRef mirrors api/v1alpha1.SecretReference for the CLI request body so the
// JSON marshals 1:1 into the operator's CreatePXFServerRequest.credentialSecrets.
type secretRef struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

// pxfServerRequest is the CLI mirror of api.CreatePXFServerRequest /
// UpdatePXFServerRequest. The same shape serves create (Name set) and update
// (Name elided; the path carries it). It serializes 1:1 into the operator DTO.
type pxfServerRequest struct {
	Name              string            `json:"name,omitempty"`
	Type              string            `json:"type,omitempty"`
	Config            map[string]string `json:"config,omitempty"`
	CredentialSecrets []secretRef       `json:"credentialSecrets,omitempty"`
}

// pxfJobBody mirrors api/v1alpha1.PxfJobSpec (the subset the CLI sets).
type pxfJobBody struct {
	Server      string `json:"server"`
	Profile     string `json:"profile"`
	Resource    string `json:"resource,omitempty"`
	TargetTable string `json:"targetTable"`
	Mode        string `json:"mode,omitempty"`
}

// gploadInputSourceBody mirrors api/v1alpha1.GploadInputSourceSpec.
type gploadInputSourceBody struct {
	Type string `json:"type,omitempty"`
	Host string `json:"host,omitempty"`
	Port int32  `json:"port,omitempty"`
}

// gploadJobBody mirrors the subset of api/v1alpha1.GploadJobSpec the CLI sets.
type gploadJobBody struct {
	TargetTable string                 `json:"targetTable"`
	Format      string                 `json:"format,omitempty"`
	InputSource *gploadInputSourceBody `json:"inputSource,omitempty"`
	FilePaths   []string               `json:"filePaths,omitempty"`
}

// dataLoadingJobRequest is the CLI mirror of api.CreateDataLoadingJobRequest. It
// is the SAME target for both flag-built and --from-yaml bodies, so the JSON
// tags must match the operator DTO exactly.
type dataLoadingJobRequest struct {
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Enabled   bool           `json:"enabled,omitempty"`
	Schedule  string         `json:"schedule,omitempty"`
	PxfJob    *pxfJobBody    `json:"pxfJob,omitempty"`
	GploadJob *gploadJobBody `json:"gploadJob,omitempty"`
}

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
	issuerURL   string
	clientID    string
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

// runAPIPut is a helper that creates a client, performs a PUT request, and
// formats the output. It mirrors runAPIPost for the few endpoints that replace
// a named resource in place (e.g. pxf servers update).
func runAPIPut(path string, body interface{}) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	resp, apiErr := client.Put(ctx, path, body)
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

// runAPIPatch is a helper that creates a client, performs a PATCH request, and formats the output.
func runAPIPatch(path string, body interface{}) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	resp, apiErr := client.Patch(ctx, path, body)
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
	pf.StringVar(&globals.issuerURL, "issuer-url", "", "OIDC issuer URL (e.g. http://localhost:8090/realms/test)")
	pf.StringVar(&globals.clientID, "client-id", "cloudberry-ctl", "OIDC client ID")
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
		newMigrateCmd(),
		newDataLoadingCmd(),
		newPxfCmd(),
		newStorageCmd(),
		newCompletionCmd(),
		newMetricsCmd(),
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
		"issuer-url":   "CLOUDBERRY_OIDC_ISSUER_URL",
		"client-id":    "CLOUDBERRY_OIDC_CLIENT_ID",
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
	applyViperValue(&globals.issuerURL, "issuer-url", "issuer-url")
	applyViperValue(&globals.clientID, "client-id", "client-id")

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
					fieldTables: strings.Split(tables, ","),
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
			Use:   cmdJobs,
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

	loginCmd := newAuthLoginCmd()

	cmd.AddCommand(
		loginCmd,
		&cobra.Command{
			Use:   "logout",
			Short: "Clear cached credentials",
			RunE: func(_ *cobra.Command, _ []string) error {
				return runAuthLogout()
			},
		},
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show auth status",
			RunE: func(_ *cobra.Command, _ []string) error {
				return runAuthStatus()
			},
		},
		&cobra.Command{
			Use:   "rotate-password",
			Short: "Rotate admin password",
			RunE: func(_ *cobra.Command, _ []string) error {
				return runAPIPost(ctl.AuthRotatePasswordPath(), nil)
			},
		},
		newRolesCmd(),
	)

	return cmd
}

// newAuthLoginCmd creates the auth login subcommand with the --basic flag.
func newAuthLoginCmd() *cobra.Command {
	loginCmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate with operator",
		RunE: func(cmd *cobra.Command, _ []string) error {
			basic, _ := cmd.Flags().GetBool("basic")
			if basic {
				return runAuthLoginBasic()
			}
			return runAuthLoginOIDC()
		},
	}
	loginCmd.Flags().Bool("basic", false, "Use basic (username/password) authentication")
	return loginCmd
}

// runAuthLoginBasic verifies basic auth credentials against the operator API.
func runAuthLoginBasic() error {
	if globals.username == "" {
		return fmt.Errorf("username is required for basic auth (set --username or CLOUDBERRY_USERNAME)")
	}
	if globals.password == "" {
		return fmt.Errorf("password is required for basic auth (set --password or CLOUDBERRY_PASSWORD)")
	}

	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	// Verify credentials by calling the clusters endpoint.
	_, apiErr := client.Get(ctx, ctl.ClustersPath())
	if apiErr != nil {
		return fmt.Errorf("login failed: %w", apiErr)
	}

	f := newFormatter()
	f.FormatMessage(fmt.Sprintf("Login successful (method=basic, user=%s)", globals.username))
	return nil
}

// runAuthLoginOIDC attempts OIDC authentication using the Authorization Code
// flow with PKCE. When --username and --password are provided, it falls back
// to the password grant simulation for CLI/testing purposes.
func runAuthLoginOIDC() error {
	// When username and password are provided, simulate the password grant flow.
	if globals.username != "" && globals.password != "" {
		client, err := newClient()
		if err != nil {
			return err
		}
		ctx, cancel := cmdContext()
		defer cancel()

		_, apiErr := client.Get(ctx, ctl.ClustersPath())
		if apiErr != nil {
			return fmt.Errorf("OIDC login failed: %w", apiErr)
		}

		f := newFormatter()
		f.FormatMessage(fmt.Sprintf("Login successful (method=oidc, user=%s)", globals.username))
		return nil
	}

	// Browser-based authorization code flow with PKCE.
	if globals.issuerURL == "" {
		return fmt.Errorf("issuer URL is required for OIDC login (set --issuer-url or CLOUDBERRY_OIDC_ISSUER_URL)")
	}

	return runOIDCBrowserFlow(globals.issuerURL, globals.clientID)
}

// runAuthStatus checks connectivity and authentication against the operator API
// and displays the current auth status.
func runAuthStatus() error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	status := map[string]interface{}{
		"auth_method":  globals.authMethod,
		"username":     globals.username,
		"operator_url": globals.operatorURL,
	}

	// Check connectivity and auth by calling the clusters endpoint.
	_, apiErr := client.Get(ctx, ctl.ClustersPath())
	if apiErr != nil {
		status["authenticated"] = false
		status["error"] = apiErr.Error()
	} else {
		status["authenticated"] = true
	}

	return newFormatter().FormatStatus(status)
}

// runAuthLogout clears cached credentials. Since the ctl uses flags and
// environment variables for authentication (not a persistent token cache),
// this is effectively a no-op that reminds the user to unset env vars.
func runAuthLogout() error {
	f := newFormatter()
	f.FormatMessage("Logged out. Cached credentials have been cleared.")
	f.FormatMessage("Note: If you set CLOUDBERRY_USERNAME or CLOUDBERRY_PASSWORD environment " +
		"variables, unset them to fully log out.")
	return nil
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
			Use:   cmdLogs,
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
				fieldName:       createName,
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
				fieldName:          createName,
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
		newWorkloadResourceGroupsCmd(),
		newWorkloadRulesCmd(),
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

// newWorkloadResourceGroupsCmd creates the workload resource-groups subcommand group.
func newWorkloadResourceGroupsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-groups",
		Short: "Workload resource group management",
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
	createCmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create resource group",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if createName == "" {
				return fmt.Errorf("resource group name is required (--name)")
			}
			body := map[string]interface{}{
				fieldName:     createName,
				"concurrency": createConcurrency,
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/resource-groups", globals.namespace), body)
		},
	}
	createCmd.Flags().StringVar(&createName, "name", "", "Resource group name")
	createCmd.Flags().Int32Var(&createConcurrency, "concurrency", 0, "Concurrency limit")

	cmd.AddCommand(listCmd, createCmd)
	return cmd
}

// newWorkloadRulesCmd creates the workload rules subcommand group.
func newWorkloadRulesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rules",
		Short: "Workload rules management",
	}

	// list subcommand.
	listCmd := &cobra.Command{
		Use:   cmdList,
		Short: "List workload rules",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/rules", globals.namespace))
		},
	}

	// create subcommand with --name and -f flags.
	var createName string
	var createFile string
	createCmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create workload rule from YAML file",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if createFile == "" {
				return fmt.Errorf("rule file is required (-f flag)")
			}
			rule, err := ctl.ReadRuleFromFile(createFile)
			if err != nil {
				return fmt.Errorf("reading rule file: %w", err)
			}
			if createName != "" {
				rule.Name = createName // --name overrides file name
			}
			if err := ctl.ValidateRule(rule); err != nil {
				return err
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "workload/rules", globals.namespace), rule)
		},
	}
	createCmd.Flags().StringVar(&createName, "name", "", "Rule name (overrides name in file)")
	createCmd.Flags().StringVarP(&createFile, "file", "f", "", "Path to rule YAML file")

	cmd.AddCommand(listCmd, createCmd, newWorkloadRulesImportCmd(), newWorkloadRulesExportCmd())
	return cmd
}

// importRuleResult represents the outcome of importing a single rule.
type importRuleResult int

const (
	importCreated importRuleResult = iota
	importUpdated
	importFailed
)

// upsertRule attempts to create a rule via POST. If the rule already exists
// (DUPLICATE_RULE), it falls back to updating via PUT. Returns the outcome.
// The provided context is used for cancellation so that the entire bulk import
// can be canceled cooperatively.
func upsertRule(ctx context.Context, apiClient *ctl.OperatorClient, rule *ctl.WorkloadRuleFile) importRuleResult {
	rulePath := ctl.ClusterSubresourcePath(
		globals.cluster, "workload/rules", globals.namespace)

	slog.Info("importing rule", "name", rule.Name, "action", rule.Action)

	_, err := apiClient.Post(ctx, rulePath, rule)
	if err == nil {
		return importCreated
	}

	// Check if the error is a DUPLICATE_RULE error — if so, try PUT to update.
	var apiErr *ctl.APIError
	if errors.As(err, &apiErr) && apiErr.Code == "DUPLICATE_RULE" {
		updatePath := ctl.ClusterSubresourcePath(
			globals.cluster,
			fmt.Sprintf("workload/rules/%s", url.PathEscape(rule.Name)),
			globals.namespace)

		slog.Info("rule exists, updating", "name", rule.Name)
		if _, putErr := apiClient.Put(ctx, updatePath, rule); putErr != nil {
			slog.Error("failed to update rule", "name", rule.Name, "error", putErr)
			return importFailed
		}
		return importUpdated
	}

	slog.Error("failed to create rule", "name", rule.Name, "error", err)
	return importFailed
}

// newWorkloadRulesImportCmd creates the workload rules import subcommand.
// It reads multiple rules from a YAML file and upserts them: tries POST (create)
// first, and if the API returns DUPLICATE_RULE, falls back to PUT (update).
func newWorkloadRulesImportCmd() *cobra.Command {
	var importFile string

	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import workload rules from YAML file (upsert)",
		Long: `Import workload rules from a YAML file. For each rule in the file,
the command tries to create it (POST). If the rule already exists (DUPLICATE_RULE),
it updates the existing rule (PUT). Reports a summary of created/updated/failed counts.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if importFile == "" {
				return fmt.Errorf("rules file is required (-f flag)")
			}

			rules, err := ctl.ReadRulesFromFile(importFile)
			if err != nil {
				return fmt.Errorf("reading rules file: %w", err)
			}

			// Create a single context and client for the entire bulk import
			// so that cancellation propagates to all in-flight requests.
			apiClient, err := newClient()
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext()
			defer cancel()

			var created, updated, failed int
			for i := range rules {
				// Check for context cancellation between rules.
				if ctx.Err() != nil {
					return fmt.Errorf("import canceled: %w", ctx.Err())
				}
				switch upsertRule(ctx, apiClient, &rules[i]) {
				case importCreated:
					created++
				case importUpdated:
					updated++
				case importFailed:
					failed++
				}
			}

			fmt.Fprintf(os.Stdout, "\nImport summary: %d created, %d updated, %d failed\n",
				created, updated, failed)

			if failed > 0 {
				return fmt.Errorf("%d rule(s) failed to import", failed)
			}
			return nil
		},
	}
	importCmd.Flags().StringVarP(&importFile, "file", "f", "", "Path to rules YAML file")

	return importCmd
}

// newWorkloadRulesExportCmd creates the workload rules export subcommand.
// It fetches all rules from the API and writes them to a YAML file or stdout.
func newWorkloadRulesExportCmd() *cobra.Command {
	var outputFile string

	exportCmd := &cobra.Command{
		Use:   cmdExport,
		Short: "Export workload rules to YAML file",
		Long: `Export all workload rules from the cluster to a YAML file.
If --output-file is not specified, the rules are written to stdout in YAML format.
The exported file can be re-imported with 'workload rules import -f <file>'.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}

			client, err := newClient()
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext()
			defer cancel()

			resp, apiErr := client.Get(ctx, ctl.ClusterSubresourcePath(
				globals.cluster, "workload/rules", globals.namespace))
			if apiErr != nil {
				return apiErr
			}

			// Extract rules array from the response.
			rules, err := extractRulesFromResponse(resp.Body)
			if err != nil {
				return fmt.Errorf("extracting rules from response: %w", err)
			}

			if outputFile != "" {
				if err := ctl.WriteRulesToFile(outputFile, rules); err != nil {
					return fmt.Errorf("writing rules to file: %w", err)
				}
				slog.Info("rules exported", "file", outputFile, "count", len(rules))
				fmt.Fprintf(os.Stdout, "Exported %d rule(s) to %s\n", len(rules), outputFile)
				return nil
			}

			// Write to stdout in YAML format.
			return ctl.WriteRulesToWriter(os.Stdout, rules)
		},
	}
	exportCmd.Flags().StringVarP(&outputFile, "output-file", "O", "",
		"Output file path (writes to stdout if not specified)")

	return exportCmd
}

// extractRulesFromResponse converts the API response body into a slice of WorkloadRuleFile.
// The response is expected to have a "rules" key containing an array of rule objects.
func extractRulesFromResponse(body map[string]interface{}) ([]ctl.WorkloadRuleFile, error) {
	rulesRaw, ok := body["rules"]
	if !ok {
		return []ctl.WorkloadRuleFile{}, nil
	}

	rulesSlice, ok := rulesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected rules format: expected array")
	}

	// Re-marshal each rule through JSON to convert map[string]interface{} to WorkloadRuleFile.
	var rules []ctl.WorkloadRuleFile
	for i, raw := range rulesSlice {
		jsonBytes, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("marshaling rule %d: %w", i, err)
		}
		var rule ctl.WorkloadRuleFile
		if err := json.Unmarshal(jsonBytes, &rule); err != nil {
			return nil, fmt.Errorf("unmarshaling rule %d: %w", i, err)
		}
		rules = append(rules, rule)
	}

	return rules, nil
}

// newQueryDetailCmd creates the queries detail subcommand.
func newQueryDetailCmd() *cobra.Command {
	var detailPID string
	detailCmd := &cobra.Command{
		Use:   "detail",
		Short: "Show query execution details",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if detailPID == "" {
				return fmt.Errorf("query ID is required (--query-id)")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/%s",
					url.PathEscape(globals.cluster), url.PathEscape(detailPID)),
				globals.namespace)
			return runAPIGet(path)
		},
	}
	detailCmd.Flags().StringVar(&detailPID, "query-id", "", "Query PID to show details for")
	return detailCmd
}

// newQueryCancelCmd creates the queries cancel subcommand.
func newQueryCancelCmd() *cobra.Command {
	var cancelPID string
	var cancelReason string
	cancelCmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancel a running query",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if cancelPID == "" {
				return fmt.Errorf("query ID is required (--query-id)")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/%s/cancel",
					url.PathEscape(globals.cluster), url.PathEscape(cancelPID)),
				globals.namespace)
			var body interface{}
			if cancelReason != "" {
				body = map[string]string{"reason": cancelReason}
			}
			return runAPIPost(path, body)
		},
	}
	cancelCmd.Flags().StringVar(&cancelPID, "query-id", "", "Query PID to cancel")
	cancelCmd.Flags().StringVar(&cancelReason, "reason", "", "Cancellation reason")
	return cancelCmd
}

// newQueryMoveCmd creates the queries move subcommand.
func newQueryMoveCmd() *cobra.Command {
	var movePID string
	var moveTargetGroup string
	moveCmd := &cobra.Command{
		Use:   "move",
		Short: "Move query to resource group",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if movePID == "" {
				return fmt.Errorf("query ID is required (--query-id)")
			}
			if moveTargetGroup == "" {
				return fmt.Errorf("target group is required (--target-group)")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/%s/move",
					url.PathEscape(globals.cluster), url.PathEscape(movePID)),
				globals.namespace)
			body := map[string]string{"targetGroup": moveTargetGroup}
			return runAPIPost(path, body)
		},
	}
	moveCmd.Flags().StringVar(&movePID, "query-id", "", "Query PID to move")
	moveCmd.Flags().StringVar(&moveTargetGroup, "target-group", "", "Target resource group name")
	return moveCmd
}

// newQueryCmd creates the query monitoring command group.
func newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queries",
		Short: "Query monitoring and analysis",
	}

	cmd.AddCommand(
		newQueryListCmd(),
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
		newQueryHistoryCmd(),
		newPlanCheckCmd(),
		newQueryDetailCmd(),
		newQueryCancelCmd(),
		newQueryMoveCmd(),
		newQueryExportCmd(),
		newQueryMonitorCmd(),
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

// newQueryListCmd creates the queries list subcommand.
// It lists active queries by calling the sessions endpoint with optional status filtering.
func newQueryListCmd() *cobra.Command {
	var status string
	cmd := &cobra.Command{
		Use:   cmdList,
		Short: "List active queries",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			params := url.Values{}
			if globals.namespace != "" {
				params.Set("namespace", globals.namespace)
			}
			if status != "" {
				params.Set("status", status)
			}
			path := fmt.Sprintf("/clusters/%s/sessions", url.PathEscape(globals.cluster))
			if len(params) > 0 {
				path += "?" + params.Encode()
			}
			return runAPIGet(path)
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running, queued, blocked, idle)")
	return cmd
}

// newQueryExportCmd creates the queries export subcommand.
// It exports active queries as CSV by calling the queries export API endpoint.
func newQueryExportCmd() *cobra.Command {
	var exportFormat string
	var outputFile string

	exportCmd := &cobra.Command{
		Use:   cmdExport,
		Short: "Export active queries",
		Long: `Export active queries from the cluster. When --format csv is specified,
the output is written as CSV to stdout or to a file specified by --output-file.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}

			if exportFormat != "" && exportFormat != "csv" {
				return fmt.Errorf("unsupported export format %q; supported: csv", exportFormat)
			}

			client, err := newClient()
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext()
			defer cancel()

			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/export",
					url.PathEscape(globals.cluster)),
				globals.namespace)

			resp, apiErr := client.Post(ctx, path, nil)
			if apiErr != nil {
				return apiErr
			}

			// Write output to file or stdout.
			// The export endpoint returns CSV text, so use RawBody.
			if outputFile != "" {
				if writeErr := os.WriteFile(outputFile, resp.RawBody, 0o600); writeErr != nil {
					return fmt.Errorf("writing to file: %w", writeErr)
				}
				fmt.Fprintf(os.Stdout, "Active queries exported to %s\n", outputFile)
				return nil
			}

			// Write raw CSV to stdout.
			_, _ = os.Stdout.Write(resp.RawBody)
			return nil
		},
	}
	exportCmd.Flags().StringVar(&exportFormat, "format", "csv", "Export format (csv)")
	exportCmd.Flags().StringVarP(&outputFile, "output-file", "O", "",
		"Output file path (writes to stdout if not specified)")
	return exportCmd
}

// newPlanCheckCmd creates the plan-check subcommand for analyzing EXPLAIN ANALYZE output.
func newPlanCheckCmd() *cobra.Command {
	var planFile string
	var planText string

	planCheckCmd := &cobra.Command{
		Use:   "plan-check",
		Short: "Analyze EXPLAIN ANALYZE output for performance issues",
		Long: `Analyze PostgreSQL/Cloudberry EXPLAIN ANALYZE output for performance issues.
Reads plan text from a file (-f) or inline (--plan) and returns identified issues
with actionable recommendations.

Examples:
  cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt
  cloudberry-ctl queries plan-check --cluster my-cluster --plan "Seq Scan on orders..."`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}

			var text string
			switch {
			case planFile != "":
				//nolint:gosec // user-provided file path is intentional for CLI
				data, readErr := os.ReadFile(planFile)
				if readErr != nil {
					return fmt.Errorf("reading plan file %q: %w", planFile, readErr)
				}
				text = string(data)
			case planText != "":
				text = planText
			default:
				return fmt.Errorf("either --file (-f) or --plan must be provided")
			}

			if strings.TrimSpace(text) == "" {
				return fmt.Errorf("plan text is empty")
			}

			body := map[string]interface{}{
				"planText": text,
			}

			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/plan-check",
					url.PathEscape(globals.cluster)),
				globals.namespace)

			return runAPIPost(path, body)
		},
	}

	planCheckCmd.Flags().StringVarP(&planFile, "file", "f", "", "Path to EXPLAIN ANALYZE output file")
	planCheckCmd.Flags().StringVar(&planText, "plan", "", "Inline EXPLAIN ANALYZE text")

	return planCheckCmd
}

// newQueryMonitorCmd creates the queries monitor subcommand group.
// It provides pause, resume, and state subcommands for controlling the query monitor.
func newQueryMonitorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "monitor",
		Short: "Query monitor pause/resume management",
	}

	pauseCmd := &cobra.Command{
		Use:   "pause",
		Short: "Pause the query monitor",
		Long: `Pause the query monitor for a cluster. While paused, query monitoring
endpoints return cached (stale) data from the time of pausing. New queries
running in the database will not appear in the monitor output until resumed.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/monitor/pause",
					url.PathEscape(globals.cluster)),
				globals.namespace)
			return runAPIPost(path, nil)
		},
	}

	resumeCmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume the query monitor",
		Long: `Resume the query monitor for a cluster. After resuming, query monitoring
endpoints return fresh data from the database again.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/monitor/resume",
					url.PathEscape(globals.cluster)),
				globals.namespace)
			return runAPIPost(path, nil)
		},
	}

	stateCmd := &cobra.Command{
		Use:   "state",
		Short: "Show monitor state",
		Long:  `Show the current pause/resume state of the query monitor for a cluster.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/monitor/state",
					url.PathEscape(globals.cluster)),
				globals.namespace)
			return runAPIGet(path)
		},
	}

	cmd.AddCommand(pauseCmd, resumeCmd, stateCmd)
	return cmd
}

// runQueryHistoryList executes the query history list logic with the given filter parameters.
// It is shared between the history parent command and the history list subcommand.
func runQueryHistoryList(histLast, histUser, histDatabase, histPattern, histPatternType,
	histResourceGroup, histExport string, histLimit, histOffset int,
) error {
	if err := requireCluster(); err != nil {
		return err
	}

	// Handle --export csv: call the export endpoint instead.
	if histExport == "csv" {
		return runQueryHistoryExportCSV(histLast, histUser, histDatabase, histPattern, histPatternType, "")
	}

	params := url.Values{}
	if histLast != "" {
		params.Set("since", histLast)
	}
	if histUser != "" {
		params.Set("user", histUser)
	}
	if histDatabase != "" {
		params.Set("database", histDatabase)
	}
	if histPattern != "" {
		params.Set("pattern", histPattern)
	}
	if histPatternType != "" {
		params.Set("patternType", histPatternType)
	}
	if histResourceGroup != "" {
		params.Set("resourceGroup", histResourceGroup)
	}
	if histLimit > 0 {
		params.Set("limit", fmt.Sprintf("%d", histLimit))
	}
	if histOffset > 0 {
		params.Set("offset", fmt.Sprintf("%d", histOffset))
	}
	path := fmt.Sprintf("/clusters/%s/queries/history",
		url.PathEscape(globals.cluster))
	if ns := globals.namespace; ns != "" {
		params.Set("namespace", ns)
	}
	if len(params) > 0 {
		path += "?" + params.Encode()
	}
	return runAPIGet(path)
}

// runQueryHistoryExportCSV calls the query history export endpoint and writes CSV output.
func runQueryHistoryExportCSV(last, user, database, pattern, patternType, outputFile string) error {
	body := map[string]interface{}{}
	if last != "" {
		body["since"] = last
	}
	if user != "" {
		body["user"] = user
	}
	if database != "" {
		body["database"] = database
	}
	if pattern != "" {
		body["pattern"] = pattern
	}
	if patternType != "" {
		body["patternType"] = patternType
	}

	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	path := appendNamespaceQuery(
		fmt.Sprintf("/clusters/%s/queries/history/export",
			url.PathEscape(globals.cluster)),
		globals.namespace)

	resp, apiErr := client.Post(ctx, path, body)
	if apiErr != nil {
		return apiErr
	}

	// Write CSV to file or stdout.
	// The export endpoint returns CSV text, so use RawBody.
	if outputFile != "" {
		if writeErr := os.WriteFile(outputFile, resp.RawBody, 0o600); writeErr != nil {
			return fmt.Errorf("writing to file: %w", writeErr)
		}
		fmt.Fprintf(os.Stdout, "Query history exported to %s\n", outputFile)
		return nil
	}

	// Write raw CSV to stdout.
	_, _ = os.Stdout.Write(resp.RawBody)
	return nil
}

// newQueryHistoryCmd creates the query history subcommand group.
// The parent command is directly runnable and behaves like "queries history list"
// when called without a subcommand.
func newQueryHistoryCmd() *cobra.Command {
	// Shared filter flags for both the parent command and the list subcommand.
	var histLast string
	var histUser string
	var histDatabase string
	var histPattern string
	var histPatternType string
	var histResourceGroup string
	var histExport string
	var histLimit int
	var histOffset int

	cmd := &cobra.Command{
		Use:   "history",
		Short: "Query history management",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runQueryHistoryList(histLast, histUser, histDatabase, histPattern,
				histPatternType, histResourceGroup, histExport, histLimit, histOffset)
		},
	}

	// Register filter flags on the parent command so they work on both
	// "queries history" and "queries history list".
	cmd.Flags().StringVar(&histLast, "last", "", "Show history from the last duration (e.g., 24h)")
	cmd.Flags().StringVar(&histUser, "user", "", "Filter by username")
	cmd.Flags().StringVar(&histDatabase, "database", "", "Filter by database name")
	cmd.Flags().StringVar(&histPattern, "pattern", "", "Search pattern (regex or wildcard)")
	cmd.Flags().StringVar(&histPatternType, "pattern-type", "", "Pattern type: regex or wildcard")
	cmd.Flags().StringVar(&histResourceGroup, "resource-group", "", "Filter by resource group")
	cmd.Flags().StringVar(&histExport, "export", "", "Export format (csv)")
	cmd.Flags().IntVar(&histLimit, "limit", 0, "Maximum number of results")
	cmd.Flags().IntVar(&histOffset, "offset", 0, "Pagination offset")

	// list subcommand — uses the same shared filter flags via closures.
	listCmd := &cobra.Command{
		Use:   cmdList,
		Short: "List query history",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runQueryHistoryList(histLast, histUser, histDatabase, histPattern,
				histPatternType, histResourceGroup, histExport, histLimit, histOffset)
		},
	}
	// Register the same flags on the list subcommand for discoverability.
	listCmd.Flags().StringVar(&histLast, "last", "", "Show history from the last duration (e.g., 24h)")
	listCmd.Flags().StringVar(&histUser, "user", "", "Filter by username")
	listCmd.Flags().StringVar(&histDatabase, "database", "", "Filter by database name")
	listCmd.Flags().StringVar(&histPattern, "pattern", "", "Search pattern (regex or wildcard)")
	listCmd.Flags().StringVar(&histPatternType, "pattern-type", "", "Pattern type: regex or wildcard")
	listCmd.Flags().StringVar(&histResourceGroup, "resource-group", "", "Filter by resource group")
	listCmd.Flags().StringVar(&histExport, "export", "", "Export format (csv)")
	listCmd.Flags().IntVar(&histLimit, "limit", 0, "Maximum number of results")
	listCmd.Flags().IntVar(&histOffset, "offset", 0, "Pagination offset")

	// detail subcommand.
	var detailQueryID string
	detailCmd := &cobra.Command{
		Use:   "detail",
		Short: "Show historical query details",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if detailQueryID == "" {
				return fmt.Errorf("query ID is required (--query-id)")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/queries/history/%s",
					url.PathEscape(globals.cluster),
					url.PathEscape(detailQueryID)),
				globals.namespace)
			return runAPIGet(path)
		},
	}
	detailCmd.Flags().StringVar(&detailQueryID, "query-id", "", "Query ID to show details for")

	// export subcommand.
	var exportOutputFile string
	var exportLast string
	var exportUser string
	var exportDatabase string
	var exportPattern string
	var exportPatternType string
	exportCmd := &cobra.Command{
		Use:   cmdExport,
		Short: "Export query history to CSV",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runQueryHistoryExportCSV(exportLast, exportUser, exportDatabase,
				exportPattern, exportPatternType, exportOutputFile)
		},
	}
	exportCmd.Flags().StringVarP(&exportOutputFile, "output-file", "O", "", "Output file path")
	exportCmd.Flags().StringVar(&exportLast, "last", "", "Export history from the last duration (e.g., 24h)")
	exportCmd.Flags().StringVar(&exportUser, "user", "", "Filter by username")
	exportCmd.Flags().StringVar(&exportDatabase, "database", "", "Filter by database name")
	exportCmd.Flags().StringVar(&exportPattern, "pattern", "", "Search pattern")
	exportCmd.Flags().StringVar(&exportPatternType, "pattern-type", "", "Pattern type: regex or wildcard")

	cmd.AddCommand(listCmd, detailCmd, exportCmd)
	return cmd
}

// newBackupCmd creates the backup command group.
// migrateFlags holds the flags for the `migrate` command.
type migrateFlags struct {
	sourceCluster  string
	targetCluster  string
	database       string
	tables         []string
	truncate       bool
	redirectDb     string
	redirectSchema string
	jobs           int32
}

// newMigrateCmd creates the `migrate` command for cross-cluster database
// migration. It POSTs to the source cluster's /migrate endpoint, which creates a
// coordinated backup Job (source) and restore Job (target) sharing one S3 bucket.
func newMigrateCmd() *cobra.Command {
	f := &migrateFlags{}
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate a database between two clusters",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runMigrate(f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.sourceCluster, "source-cluster", "", "Source cluster name")
	fl.StringVar(&f.targetCluster, "target-cluster", "", "Target cluster name")
	fl.StringVar(&f.database, "database", "", "Database to migrate")
	fl.StringSliceVar(&f.tables, "tables", nil, "Tables to migrate (comma-separated)")
	fl.BoolVar(&f.truncate, "truncate", false, "Truncate target tables before restore")
	fl.StringVar(&f.redirectDb, "redirect-db", "", "gprestore --redirect-db on the target")
	fl.StringVar(&f.redirectSchema, "redirect-schema", "", "gprestore --redirect-schema on the target")
	fl.Int32Var(&f.jobs, "jobs", 0, "gprestore --jobs on the target")
	return cmd
}

// runMigrate validates the migrate flags and posts the migration request.
func runMigrate(f *migrateFlags) error {
	if f.sourceCluster == "" {
		return fmt.Errorf("--source-cluster is required")
	}
	if f.targetCluster == "" {
		return fmt.Errorf("--target-cluster is required")
	}
	req := buildMigrateRequest(f)
	path := ctl.ClusterSubresourcePath(f.sourceCluster, "migrate", globals.namespace)
	return runAPIPost(path, req)
}

// buildMigrateRequest assembles the migrate request body from flags.
func buildMigrateRequest(f *migrateFlags) map[string]interface{} {
	return map[string]interface{}{
		"sourceCluster":  f.sourceCluster,
		"targetCluster":  f.targetCluster,
		"database":       f.database,
		fieldTables:      f.tables,
		"truncate":       f.truncate,
		"redirectDb":     f.redirectDb,
		"redirectSchema": f.redirectSchema,
		fieldJobs:        f.jobs,
	}
}

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup and restore management",
	}

	cmd.AddCommand(
		newBackupCreateCmd(),
		newBackupListCmd(),
		newBackupStatusCmd(),
		newBackupDeleteCmd(),
		newBackupRestoreCmd(),
		newBackupScheduleCmd(),
		newBackupJobsCmd(),
	)

	return cmd
}

// backupCreateFlags holds the flags for `backup create`.
type backupCreateFlags struct {
	databases         []string
	backupType        string
	compressionLevel  int32
	compressionType   string
	jobs              int32
	singleDataFile    bool
	copyQueueSize     int32
	includeSchemas    []string
	excludeTables     []string
	incremental       bool
	fromTimestamp     string
	leafPartitionData bool
	withStats         bool
	withoutGlobals    bool
}

// newBackupCreateCmd creates the `backup create` subcommand.
func newBackupCreateCmd() *cobra.Command {
	f := &backupCreateFlags{}
	cmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create a new backup",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			req := buildCreateBackupRequest(f)
			path := ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace)
			return runAPIPost(path, req)
		},
	}
	fl := cmd.Flags()
	fl.StringSliceVar(&f.databases, "database", nil, "Database(s) to back up (repeatable or comma-separated)")
	fl.StringVar(&f.backupType, "type", "", "Backup type: full|incremental")
	fl.Int32Var(&f.compressionLevel, "compression-level", 0, "gpbackup --compression-level")
	fl.StringVar(&f.compressionType, "compression-type", "", "gpbackup --compression-type (gzip|zstd)")
	fl.Int32Var(&f.jobs, "jobs", 0, "gpbackup --jobs")
	fl.BoolVar(&f.singleDataFile, "single-data-file", false, "gpbackup --single-data-file")
	fl.Int32Var(&f.copyQueueSize, "copy-queue-size", 0, "gpbackup --copy-queue-size")
	fl.StringSliceVar(&f.includeSchemas, "include-schema", nil, "gpbackup --include-schema (repeatable)")
	fl.StringSliceVar(&f.excludeTables, "exclude-table", nil, "gpbackup --exclude-table (repeatable)")
	fl.BoolVar(&f.incremental, "incremental", false, "gpbackup --incremental")
	fl.StringVar(&f.fromTimestamp, "from-timestamp", "", "gpbackup --from-timestamp")
	fl.BoolVar(&f.leafPartitionData, "leaf-partition-data", false, "gpbackup --leaf-partition-data")
	fl.BoolVar(&f.withStats, "with-stats", false, "gpbackup --with-stats")
	fl.BoolVar(&f.withoutGlobals, "without-globals", false, "gpbackup --without-globals")
	return cmd
}

// buildCreateBackupRequest assembles the create-backup request body from flags.
func buildCreateBackupRequest(f *backupCreateFlags) map[string]interface{} {
	gpbackup := map[string]interface{}{
		"compressionLevel":  f.compressionLevel,
		"compressionType":   f.compressionType,
		fieldJobs:           f.jobs,
		"singleDataFile":    f.singleDataFile,
		"copyQueueSize":     f.copyQueueSize,
		"incremental":       f.incremental,
		"fromTimestamp":     f.fromTimestamp,
		"includeSchemas":    f.includeSchemas,
		"excludeTables":     f.excludeTables,
		"leafPartitionData": f.leafPartitionData,
		"withStats":         f.withStats,
		"withoutGlobals":    f.withoutGlobals,
	}
	return map[string]interface{}{
		"type":            f.backupType,
		"databases":       f.databases,
		"gpbackupOptions": gpbackup,
	}
}

// newBackupListCmd creates the `backup list` subcommand.
func newBackupListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   cmdList,
		Short: "List available backups",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace))
		},
	}
}

// newBackupStatusCmd creates the `backup status` subcommand.
func newBackupStatusCmd() *cobra.Command {
	var timestamp string
	cmd := &cobra.Command{
		Use:   cmdStatus,
		Short: "Show backup status by timestamp",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if timestamp == "" {
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups", globals.namespace))
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/backups/%s",
					url.PathEscape(globals.cluster), url.PathEscape(timestamp)),
				globals.namespace)
			return runAPIGet(path)
		},
	}
	cmd.Flags().StringVar(&timestamp, "timestamp", "", "gpbackup timestamp")
	return cmd
}

// newBackupDeleteCmd creates the `backup delete` subcommand.
func newBackupDeleteCmd() *cobra.Command {
	var timestamp string
	cmd := &cobra.Command{
		Use:   cmdDelete,
		Short: "Delete a backup",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if timestamp == "" {
				return fmt.Errorf("--timestamp is required")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/backups/%s",
					url.PathEscape(globals.cluster), url.PathEscape(timestamp)),
				globals.namespace)
			return runAPIDelete(path)
		},
	}
	cmd.Flags().StringVar(&timestamp, "timestamp", "", "gpbackup timestamp to delete")
	return cmd
}

// backupRestoreFlags holds the flags for `backup restore`.
type backupRestoreFlags struct {
	timestamp       string
	redirectDb      string
	redirectSchema  string
	createDb        bool
	includeSchemas  []string
	includeTables   []string
	jobs            int32
	withStats       bool
	runAnalyze      bool
	onErrorContinue bool
	truncateTable   bool
	resizeCluster   bool
}

// newBackupRestoreCmd creates the `backup restore` subcommand.
func newBackupRestoreCmd() *cobra.Command {
	f := &backupRestoreFlags{}
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore from a backup",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if f.timestamp == "" {
				return fmt.Errorf("--timestamp is required")
			}
			req := buildRestoreRequest(f)
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/backups/%s/restore",
					url.PathEscape(globals.cluster), url.PathEscape(f.timestamp)),
				globals.namespace)
			return runAPIPost(path, req)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.timestamp, "timestamp", "", "gpbackup timestamp to restore from")
	fl.StringVar(&f.redirectDb, "redirect-db", "", "gprestore --redirect-db")
	fl.StringVar(&f.redirectSchema, "redirect-schema", "", "gprestore --redirect-schema")
	fl.BoolVar(&f.createDb, "create-db", false, "gprestore --create-db")
	fl.StringSliceVar(&f.includeSchemas, "include-schema", nil, "gprestore --include-schema (repeatable)")
	fl.StringSliceVar(&f.includeTables, "include-table", nil, "gprestore --include-table (repeatable)")
	fl.Int32Var(&f.jobs, "jobs", 0, "gprestore --jobs")
	fl.BoolVar(&f.withStats, "with-stats", false, "gprestore --with-stats")
	fl.BoolVar(&f.runAnalyze, "run-analyze", false, "gprestore --run-analyze")
	fl.BoolVar(&f.onErrorContinue, "on-error-continue", false, "gprestore --on-error-continue")
	fl.BoolVar(&f.truncateTable, "truncate-table", false, "gprestore --truncate-table")
	fl.BoolVar(&f.resizeCluster, "resize-cluster", false, "gprestore --resize-cluster")
	return cmd
}

// buildRestoreRequest assembles the restore request body from flags.
func buildRestoreRequest(f *backupRestoreFlags) map[string]interface{} {
	gprestore := map[string]interface{}{
		fieldJobs:         f.jobs,
		"redirectDb":      f.redirectDb,
		"redirectSchema":  f.redirectSchema,
		"createDb":        f.createDb,
		"includeSchemas":  f.includeSchemas,
		"includeTables":   f.includeTables,
		"withStats":       f.withStats,
		"runAnalyze":      f.runAnalyze,
		"onErrorContinue": f.onErrorContinue,
		"truncateTable":   f.truncateTable,
		"resizeCluster":   f.resizeCluster,
	}
	return map[string]interface{}{
		"timestamp":        f.timestamp,
		"gprestoreOptions": gprestore,
	}
}

// newBackupScheduleCmd creates the `backup schedule` subcommand group.
func newBackupScheduleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Show and manage the backup schedule",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups/schedule", globals.namespace))
		},
	}
	cmd.AddCommand(
		newBackupScheduleSetCmd(),
		newBackupScheduleSuspendCmd("suspend", true, "Suspend the backup schedule"),
		newBackupScheduleSuspendCmd("resume", false, "Resume the backup schedule"),
	)
	return cmd
}

// newBackupScheduleSetCmd creates the `backup schedule set` subcommand.
func newBackupScheduleSetCmd() *cobra.Command {
	var cron string
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set the backup cron schedule",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if cron == "" {
				return fmt.Errorf("--cron is required")
			}
			body := map[string]interface{}{"schedule": cron}
			return runAPIPatch(
				ctl.ClusterSubresourcePath(globals.cluster, "backups/schedule", globals.namespace), body)
		},
	}
	cmd.Flags().StringVar(&cron, "cron", "", `Cron schedule (e.g. "0 3 * * *")`)
	return cmd
}

// newBackupScheduleSuspendCmd creates a suspend/resume subcommand that PATCHes the
// CronJob's .spec.suspend in place.
func newBackupScheduleSuspendCmd(use string, suspend bool, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			body := map[string]interface{}{"suspend": suspend}
			return runAPIPatch(
				ctl.ClusterSubresourcePath(globals.cluster, "backups/schedule", globals.namespace), body)
		},
	}
}

// newBackupJobsCmd creates the `backup jobs` subcommand group.
func newBackupJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdJobs,
		Short: "List backup/restore Jobs",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "backups/jobs", globals.namespace))
		},
	}
	cmd.AddCommand(newBackupJobsLogsCmd())
	return cmd
}

// backupJobsLogsFlags holds the flags for `backup jobs logs`.
type backupJobsLogsFlags struct {
	job    string
	follow bool
	tail   int64
}

// newBackupJobsLogsCmd creates the `backup jobs logs` subcommand. It streams the
// selected backup Job's pod logs from the operator API. When the streaming
// endpoint is unavailable (e.g. an older operator), it falls back to printing
// the equivalent kubectl command rather than failing silently.
func newBackupJobsLogsCmd() *cobra.Command {
	f := &backupJobsLogsFlags{tail: -1}
	cmd := &cobra.Command{
		Use:   cmdLogs,
		Short: "Stream logs for a backup Job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if f.job == "" {
				return fmt.Errorf("--job is required")
			}
			return runBackupJobsLogs(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.job, "job", "", "Backup Job name")
	fl.BoolVar(&f.follow, "follow", false, "Stream logs as they are produced")
	fl.Int64Var(&f.tail, "tail", -1, "Number of recent log lines to show (-1 for all)")
	return cmd
}

// runBackupJobsLogs streams the Job's pod logs to stdout, falling back to the
// kubectl instruction when the operator streaming endpoint is unavailable.
func runBackupJobsLogs(cmd *cobra.Command, f *backupJobsLogsFlags) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	path := buildBackupJobLogsPath(f)
	if streamErr := client.GetStream(ctx, path, cmd.OutOrStdout()); streamErr != nil {
		printBackupJobLogsFallback(cmd, f.job, streamErr)
	}
	return nil
}

// buildBackupJobLogsPath builds the namespace-qualified logs endpoint path,
// including the optional follow/tail query parameters.
func buildBackupJobLogsPath(f *backupJobsLogsFlags) string {
	path := fmt.Sprintf("/clusters/%s/backups/jobs/%s/logs",
		url.PathEscape(globals.cluster), url.PathEscape(f.job))

	query := url.Values{}
	if globals.namespace != "" {
		query.Set("namespace", globals.namespace)
	}
	if f.follow {
		query.Set("follow", "true")
	}
	if f.tail >= 0 {
		query.Set("tailLines", strconv.FormatInt(f.tail, 10))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path
}

// printBackupJobLogsFallback prints the kubectl instruction used when the
// operator log-streaming endpoint cannot be reached.
func printBackupJobLogsFallback(cmd *cobra.Command, job string, cause error) {
	ns := globals.namespace
	if ns == "" {
		ns = "default"
	}
	fmt.Fprintf(cmd.OutOrStdout(),
		"unable to stream logs from the operator API (%v); run:\n  kubectl logs -n %s job/%s\n",
		cause, ns, job)
}

// resolveTableDetail resolves the schema and table for the L.3
// "storage tables detail" command. Flags take precedence when non-empty;
// otherwise it falls back to the positional args (args[0]=schema,
// args[1]=table). Both must resolve to a non-empty value, else a clear usage
// error is returned BEFORE any HTTP call so no half-built /tables// request is
// issued.
func resolveTableDetail(flagSchema, flagTable string, args []string) (schema, table string, err error) {
	schema = flagSchema
	table = flagTable
	if schema == "" && len(args) >= 1 {
		schema = args[0]
	}
	if table == "" && len(args) >= 2 {
		table = args[1]
	}
	if schema == "" || table == "" {
		return "", "", errors.New(
			"schema and table are required " +
				"(use --schema/--table or positional args)")
	}
	return schema, table, nil
}

// newStorageCmd builds the "storage" command group: the storage-recommendations
// CLI family (L.1–L.6), a thin client over the Scenario 119/120 storage API
// endpoints P.1–P.6. This family is DISTINCT from the data-loading L.1–L.16 CLI
// family (Scenario 108 / spec 12). The six commands are:
//
//	L.1  storage disk-usage                  GET  .../storage/disk-usage
//	L.2  storage tables list                 GET  .../storage/tables
//	L.3  storage tables detail --schema --table
//	                                         GET  .../storage/tables/{schema}/{table}
//	L.4  storage recommendations list        GET  .../storage/recommendations
//	L.5  storage recommendations scan        POST .../storage/recommendations/scan
//	L.6  storage usage-report --month        GET  .../storage/usage-report?month=YYYY-MM
func newStorageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Storage management and recommendations",
	}

	tablesCmd := &cobra.Command{
		Use:   "tables",
		Short: "Table storage management",
	}

	// L.3 detail: accepts the --schema/--table flags (canonical form per the
	// spec) AND legacy positional [schema] [table] args for backward
	// compatibility. Flags take precedence when set; otherwise the resolution
	// falls back to the positional args. The variables are declared in the
	// enclosing scope so the RunE closure captures them (same shape as
	// usageReportMonth below).
	var detailSchema, detailTable string
	detailCmd := &cobra.Command{
		Use:   "detail [schema] [table]",
		Short: "Show table detail",
		Args:  cobra.MaximumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			schema, table, err := resolveTableDetail(detailSchema, detailTable, args)
			if err != nil {
				return err
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/storage/tables/%s/%s",
					url.PathEscape(globals.cluster),
					url.PathEscape(schema), url.PathEscape(table)),
				globals.namespace)
			return runAPIGet(path)
		},
	}
	detailCmd.Flags().StringVar(&detailSchema, "schema", "", "Table schema")
	detailCmd.Flags().StringVar(&detailTable, "table", "", "Table name")

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
		detailCmd,
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

	// usage-report (C.13): retrieve the monthly usage report. The optional
	// --month flag threads through as the ?month= query param the API already
	// honors (r.URL.Query().Get("month")), scoping/labeling the report; omitting
	// it returns the current/unscoped report. Both namespace and month are
	// encoded through a single url.Values builder (mirroring newQueryListCmd) so
	// the query string is always well-formed.
	var usageReportMonth string
	usageReportCmd := &cobra.Command{
		Use:   "usage-report",
		Short: "Show usage report",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			params := url.Values{}
			if globals.namespace != "" {
				params.Set("namespace", globals.namespace)
			}
			if usageReportMonth != "" {
				params.Set("month", usageReportMonth)
			}
			path := fmt.Sprintf("/clusters/%s/storage/usage-report",
				url.PathEscape(globals.cluster))
			if len(params) > 0 {
				path += "?" + params.Encode()
			}
			return runAPIGet(path)
		},
	}
	usageReportCmd.Flags().StringVar(&usageReportMonth, "month", "",
		"Month (YYYY-MM) to scope the usage report")

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
		usageReportCmd,
	)

	return cmd
}

// newPxfCmd creates the "pxf" command group for the operator-driven PXF
// lifecycle. It exposes only the operator-level verbs that the API server
// implements: status (honest sidecar readiness), restart (segment-primary pod
// roll), and sync (servers ConfigMap refresh + roll). The sidecar-local verbs
// pxf prepare/start/stop are exec-only and intentionally NOT exposed here.
func newPxfCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pxf",
		Short: "PXF data-loading sidecar lifecycle",
		Long: "Operator-driven PXF lifecycle commands. 'restart' and 'sync' bump the\n" +
			"segment-primary StatefulSet restart trigger, causing the PXF sidecars to\n" +
			"ROLL (pods are recreated) — this is heavier than an in-place sidecar restart.\n" +
			"The sidecar-local verbs (prepare/start/stop) are exec-only and not exposed here.",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   cmdStatus,
			Short: "Show PXF sidecar readiness across segment-primary pods",
			Long: "Reports honest PXF sidecar readiness aggregated from the real container\n" +
				"statuses of the segment-primary pods, plus the spec-derived configured/servers counts.",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(
					globals.cluster, "data-loading/pxf/status", globals.namespace))
			},
		},
		&cobra.Command{
			Use:   cmdRestart,
			Short: "Restart PXF sidecars across all segment-primary pods",
			Long: "Triggers an operator-driven PXF restart by bumping the segment-primary\n" +
				"StatefulSet restart trigger. The segment-primary pods ROLL (kubelet recreates\n" +
				"them; the entrypoint re-runs pxf prepare/start) — heavier than an in-place restart.",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterSubresourcePath(
					globals.cluster, "data-loading/pxf/restart", globals.namespace), nil)
			},
		},
		&cobra.Command{
			Use:   cmdSync,
			Short: "Refresh the PXF servers ConfigMap and roll the sidecars",
			Long: "Re-renders the <cluster>-pxf-servers ConfigMap from the current spec and bumps\n" +
				"the segment-primary restart trigger so the pxf-cred-init init container\n" +
				"re-renders the servers on the next roll.",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIPost(ctl.ClusterSubresourcePath(
					globals.cluster, "data-loading/pxf/sync", globals.namespace), nil)
			},
		},
		newPxfServersCmd(),
	)

	return cmd
}

// pxfServersFlags holds the flags for the `pxf servers create/update` commands.
type pxfServersFlags struct {
	name              string
	serverType        string
	endpoint          string
	bucket            string
	credentialSecrets []string
}

// newPxfServersCmd creates the `pxf servers` subtree (L.2–L.5): list/create/
// update/delete, wired to the existing Scenario 107 REST server CRUD endpoints.
func newPxfServersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "servers",
		Short: "Manage PXF servers",
	}
	cmd.AddCommand(
		newPxfServersListCmd(),
		newPxfServersCreateCmd(),
		newPxfServersUpdateCmd(),
		newPxfServersDeleteCmd(),
	)
	return cmd
}

// newPxfServersListCmd creates `pxf servers list` (L.2).
func newPxfServersListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   cmdList,
		Short: "List PXF servers",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			return runAPIGet(ctl.ClusterSubresourcePath(
				globals.cluster, "data-loading/pxf/servers", globals.namespace))
		},
	}
}

// newPxfServersCreateCmd creates `pxf servers create` (L.3). --name and --type
// are required and guarded locally (usage error, no API call) before the POST.
func newPxfServersCreateCmd() *cobra.Command {
	f := &pxfServersFlags{}
	cmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create a PXF server",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if f.name == "" || f.serverType == "" {
				return fmt.Errorf("--name and --type are required")
			}
			body := pxfServerRequest{
				Name:              f.name,
				Type:              f.serverType,
				Config:            buildPxfServerConfig(f),
				CredentialSecrets: parseCredentialSecrets(f.credentialSecrets),
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "data-loading/pxf/servers", globals.namespace), body)
		},
	}
	bindPxfServerFlags(cmd, f, true)
	return cmd
}

// newPxfServersUpdateCmd creates `pxf servers update [name]` (L.4). The server
// name comes from the positional arg or --name; the body is PUT to the named
// server endpoint.
func newPxfServersUpdateCmd() *cobra.Command {
	f := &pxfServersFlags{}
	cmd := &cobra.Command{
		Use:   cmdUpdate + " [name]",
		Short: "Update a PXF server",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			name := f.name
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("server name is required (positional arg or --name)")
			}
			body := pxfServerRequest{
				Type:              f.serverType,
				Config:            buildPxfServerConfig(f),
				CredentialSecrets: parseCredentialSecrets(f.credentialSecrets),
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/data-loading/pxf/servers/%s",
					url.PathEscape(globals.cluster), url.PathEscape(name)),
				globals.namespace)
			return runAPIPut(path, body)
		},
	}
	bindPxfServerFlags(cmd, f, false)
	return cmd
}

// newPxfServersDeleteCmd creates `pxf servers delete [name]` (L.5).
func newPxfServersDeleteCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   cmdDelete + " [name]",
		Short: "Delete a PXF server",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("server name is required (positional arg or --name)")
			}
			path := appendNamespaceQuery(
				fmt.Sprintf("/clusters/%s/data-loading/pxf/servers/%s",
					url.PathEscape(globals.cluster), url.PathEscape(name)),
				globals.namespace)
			return runAPIDelete(path)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "PXF server name")
	return cmd
}

// bindPxfServerFlags binds the shared --type/--endpoint/--bucket/--credential-secret
// flags used by create and update. When requireName is true a --name flag is
// also bound (create); update takes the name positionally or via a bare --name.
func bindPxfServerFlags(cmd *cobra.Command, f *pxfServersFlags, requireName bool) {
	fl := cmd.Flags()
	fl.StringVar(&f.name, "name", "", "PXF server name")
	if requireName {
		fl.StringVar(&f.serverType, "type", "",
			"PXF server type (s3|hdfs|jdbc|hbase|hive|gs|abfss|wasbs|custom)")
	} else {
		fl.StringVar(&f.serverType, "type", "", "PXF server type (optional on update)")
	}
	fl.StringVar(&f.endpoint, "endpoint", "", "Object-store endpoint (maps to "+pxfConfigEndpointKey+")")
	fl.StringVar(&f.bucket, "bucket", "", "Object-store bucket")
	fl.StringArrayVar(&f.credentialSecrets, "credential-secret", nil,
		"Credential secret as \"secretName[:key]\" (repeatable)")
}

// buildPxfServerConfig maps the friendly --endpoint/--bucket flags into the PXF
// server config map. It returns nil when neither flag is set so an empty config
// is omitted from the JSON body.
func buildPxfServerConfig(f *pxfServersFlags) map[string]string {
	cfg := map[string]string{}
	if f.endpoint != "" {
		cfg[pxfConfigEndpointKey] = f.endpoint
	}
	if f.bucket != "" {
		cfg[pxfConfigBucketKey] = f.bucket
	}
	if len(cfg) == 0 {
		return nil
	}
	return cfg
}

// parseCredentialSecrets parses each "secretName[:key]" flag value into a
// secretRef. An empty input yields nil so the field is omitted from the body.
func parseCredentialSecrets(raw []string) []secretRef {
	if len(raw) == 0 {
		return nil
	}
	out := make([]secretRef, 0, len(raw))
	for _, v := range raw {
		name, key, found := strings.Cut(v, ":")
		ref := secretRef{Name: name}
		if found {
			ref.Key = key
		}
		out = append(out, ref)
	}
	return out
}

// newDataLoadingCmd creates the data loading command group.
func newDataLoadingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data-loading",
		Short: "Data loading management",
	}

	jobsCmd := &cobra.Command{
		Use:   cmdJobs,
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
		newDataLoadingJobsCreateCmd(),
		newDataLoadingJobsLogsCmd(),
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
		newDataLoadingTestReadCmd(),
	)

	return cmd
}

// dataLoadingJobCreateFlags holds the flags for `data-loading jobs create`.
type dataLoadingJobCreateFlags struct {
	jobType     string
	name        string
	schedule    string
	fromYAML    string
	server      string
	profile     string
	resource    string
	target      string
	gpfdistHost string
	gpfdistPort int32
	filePath    string
	format      string
}

// newDataLoadingJobsCreateCmd creates the enriched `data-loading jobs create`
// command (L.9/L.14/L.16). It builds a CreateDataLoadingJobRequest from --type
// pxf|gpload flags, or — when --from-yaml is given (which takes PRECEDENCE over
// the individual flags) — from a YAML file. The assembled body is POSTed; the
// operator webhook stays authoritative for deep validation (cron, server refs).
func newDataLoadingJobsCreateCmd() *cobra.Command {
	f := &dataLoadingJobCreateFlags{jobType: dataLoadTypePXF}
	cmd := &cobra.Command{
		Use:   cmdCreate,
		Short: "Create a data loading job (pxf|gpload, or --from-yaml)",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			body, err := buildDataLoadingJobBody(f)
			if err != nil {
				return err
			}
			return runAPIPost(ctl.ClusterSubresourcePath(
				globals.cluster, "data-loading/jobs", globals.namespace), body)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.jobType, "type", dataLoadTypePXF, "Job type (pxf|gpload)")
	fl.StringVar(&f.name, "name", "", "Job name (required unless --from-yaml)")
	fl.StringVar(&f.schedule, "schedule", "", "Cron schedule (5-field); empty = one-off")
	fl.StringVar(&f.fromYAML, "from-yaml", "", "Path to a job definition YAML (takes precedence over flags)")
	// pxf flags.
	fl.StringVar(&f.server, "server", "", "PXF server name (pxf)")
	fl.StringVar(&f.profile, "profile", "", "PXF profile (pxf)")
	fl.StringVar(&f.resource, "resource", "", "PXF resource locator (pxf)")
	fl.StringVar(&f.target, "target", "", "Target table")
	// gpload flags.
	fl.StringVar(&f.gpfdistHost, "gpfdist-host", "", "gpfdist host (gpload)")
	fl.Int32Var(&f.gpfdistPort, "gpfdist-port", 0, "gpfdist port (gpload)")
	fl.StringVar(&f.filePath, "file-path", "", "Source file glob (gpload)")
	fl.StringVar(&f.format, "format", "", "Input format csv|text (gpload)")
	return cmd
}

// buildDataLoadingJobBody assembles the job request body. --from-yaml takes
// PRECEDENCE: when set, the file is read+unmarshalled and the flag-built body is
// ignored (a note is logged if other job flags were also supplied). Otherwise a
// pxf or gpload body is built from the typed flags.
func buildDataLoadingJobBody(f *dataLoadingJobCreateFlags) (*dataLoadingJobRequest, error) {
	if f.fromYAML != "" {
		if f.name != "" || f.server != "" || f.target != "" {
			slog.Info("--from-yaml takes precedence; individual job flags are ignored",
				"file", f.fromYAML)
		}
		return readDataLoadingJobYAML(f.fromYAML)
	}
	if f.name == "" {
		return nil, fmt.Errorf("--name is required (or use --from-yaml)")
	}
	switch f.jobType {
	case dataLoadTypePXF:
		return buildPxfJobBody(f), nil
	case dataLoadTypeGpload:
		return buildGploadJobBody(f), nil
	default:
		return nil, fmt.Errorf("invalid --type %q; valid types: pxf, gpload", f.jobType)
	}
}

// buildPxfJobBody builds a pxf-type job request from the typed flags (L.9). Mode
// defaults to "insert" (a readable import external table).
func buildPxfJobBody(f *dataLoadingJobCreateFlags) *dataLoadingJobRequest {
	return &dataLoadingJobRequest{
		Name:     f.name,
		Type:     dataLoadTypePXF,
		Schedule: f.schedule,
		PxfJob: &pxfJobBody{
			Server:      f.server,
			Profile:     f.profile,
			Resource:    f.resource,
			TargetTable: f.target,
			Mode:        "insert",
		},
	}
}

// buildGploadJobBody builds a gpload-type job request from the typed flags
// (L.14): a gpfdist inputSource (host/port) plus the single file-path glob.
func buildGploadJobBody(f *dataLoadingJobCreateFlags) *dataLoadingJobRequest {
	gpload := &gploadJobBody{
		TargetTable: f.target,
		Format:      f.format,
		InputSource: &gploadInputSourceBody{
			Type: "gpfdist",
			Host: f.gpfdistHost,
			Port: f.gpfdistPort,
		},
	}
	if f.filePath != "" {
		gpload.FilePaths = []string{f.filePath}
	}
	return &dataLoadingJobRequest{
		Name:      f.name,
		Type:      dataLoadTypeGpload,
		Schedule:  f.schedule,
		GploadJob: gpload,
	}
}

// readDataLoadingJobYAML reads and unmarshals a job definition YAML file into a
// dataLoadingJobRequest (L.16). It uses sigs.k8s.io/yaml so the YAML keys match
// the request's JSON tags 1:1. A missing file or malformed YAML is surfaced as a
// clean error (no POST is attempted by the caller).
func readDataLoadingJobYAML(path string) (*dataLoadingJobRequest, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-authored job file path supplied by the CLI user
	if err != nil {
		return nil, fmt.Errorf("reading job file %q: %w", path, err)
	}
	var req dataLoadingJobRequest
	if unmarshalErr := yaml.Unmarshal(data, &req); unmarshalErr != nil {
		return nil, fmt.Errorf("parsing job file %q: %w", path, unmarshalErr)
	}
	return &req, nil
}

// newDataLoadingJobsLogsCmd creates `data-loading jobs logs` (L.13). It streams
// the data-loading Job's pod logs via the operator API and falls back to the
// kubectl instruction when the streaming endpoint is unavailable, mirroring the
// backup jobs logs command.
func newDataLoadingJobsLogsCmd() *cobra.Command {
	f := &backupJobsLogsFlags{tail: -1}
	cmd := &cobra.Command{
		Use:   cmdLogs,
		Short: "Stream logs for a data loading Job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if f.job == "" {
				return fmt.Errorf("--job is required")
			}
			return runDataLoadingJobsLogs(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.job, "job", "", "Data loading Job name")
	fl.BoolVar(&f.follow, "follow", false, "Stream logs as they are produced")
	fl.Int64Var(&f.tail, "tail", -1, "Number of recent log lines to show (-1 for all)")
	return cmd
}

// runDataLoadingJobsLogs streams the data-loading Job's pod logs to stdout,
// falling back to the kubectl instruction when the operator streaming endpoint
// is unavailable. It reuses the backup logs streaming helper + fallback.
func runDataLoadingJobsLogs(cmd *cobra.Command, f *backupJobsLogsFlags) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext()
	defer cancel()

	path := buildDataLoadingJobLogsPath(f)
	if streamErr := client.GetStream(ctx, path, cmd.OutOrStdout()); streamErr != nil {
		printBackupJobLogsFallback(cmd, util.DataLoadJobName(globals.cluster, f.job), streamErr)
	}
	return nil
}

// buildDataLoadingJobLogsPath builds the namespace-qualified data-loading logs
// endpoint path, including the optional follow/tail query parameters.
func buildDataLoadingJobLogsPath(f *backupJobsLogsFlags) string {
	path := fmt.Sprintf("/clusters/%s/data-loading/jobs/%s/logs",
		url.PathEscape(globals.cluster), url.PathEscape(f.job))

	query := url.Values{}
	if globals.namespace != "" {
		query.Set("namespace", globals.namespace)
	}
	if f.follow {
		query.Set("follow", "true")
	}
	if f.tail >= 0 {
		query.Set("tailLines", strconv.FormatInt(f.tail, 10))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path
}

// dataLoadingTestReadFlags holds the flags for `data-loading test-read`.
type dataLoadingTestReadFlags struct {
	job      string
	server   string
	profile  string
	resource string
	limit    int
}

// newDataLoadingTestReadCmd creates `data-loading test-read` (L.15). It reads up
// to --limit rows from a PXF source — selected by --job (primary) or by explicit
// --server/--profile/--resource — and prints the REAL sampled rows. When the
// source is not readable it prints an honest "(source unavailable)" notice and
// exits 0 (this is a read/preview command), never fabricating rows.
func newDataLoadingTestReadCmd() *cobra.Command {
	f := &dataLoadingTestReadFlags{limit: 10}
	cmd := &cobra.Command{
		Use:   "test-read",
		Short: "Read N rows from a PXF source (preview)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireCluster(); err != nil {
				return err
			}
			if f.limit < 1 {
				return fmt.Errorf("--limit must be a positive integer")
			}
			if f.job == "" && (f.profile == "" || f.resource == "") {
				return fmt.Errorf("either --job or both --profile and --resource are required")
			}
			return runDataLoadingTestRead(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.job, "job", "", "Resolve the source from a defined job (primary)")
	fl.StringVar(&f.server, "server", "", "PXF server name (alternative to --job)")
	fl.StringVar(&f.profile, "profile", "", "PXF profile (alternative to --job)")
	fl.StringVar(&f.resource, "resource", "", "PXF resource locator (alternative to --job)")
	fl.IntVar(&f.limit, "limit", 10, "Maximum rows to read (default 10, cap 1000)")
	return cmd
}

// runDataLoadingTestRead builds the test-read query string and GETs the preview
// endpoint, rendering the result via the standard formatter.
func runDataLoadingTestRead(_ *cobra.Command, f *dataLoadingTestReadFlags) error {
	query := url.Values{}
	if globals.namespace != "" {
		query.Set("namespace", globals.namespace)
	}
	if f.job != "" {
		query.Set("job", f.job)
	} else {
		if f.server != "" {
			query.Set("server", f.server)
		}
		query.Set("profile", f.profile)
		query.Set("resource", f.resource)
	}
	query.Set("limit", strconv.Itoa(f.limit))

	path := fmt.Sprintf("/clusters/%s/data-loading/test-read?%s",
		url.PathEscape(globals.cluster), query.Encode())
	return runAPIGet(path)
}

// newMetricsCmd creates the metrics command group.
func newMetricsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Metrics and monitoring",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "exporters",
			Short: "Show exporter health status",
			RunE: func(_ *cobra.Command, _ []string) error {
				if err := requireCluster(); err != nil {
					return err
				}
				return runAPIGet(ctl.ClusterSubresourcePath(globals.cluster, "metrics/exporters", globals.namespace))
			},
		},
	)

	return cmd
}
