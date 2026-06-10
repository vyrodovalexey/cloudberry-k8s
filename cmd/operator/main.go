// Package main is the entry point for the cloudberry-operator.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/attribute"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/certmanager"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
)

// version is set via ldflags at build time (e.g. -X main.version=...).
//
//nolint:gochecknoglobals // set by ldflags
var version = "dev"

const (
	// shutdownTimeout is the maximum time to wait for graceful shutdown of
	// background components (tracer, API server, etc.).
	shutdownTimeout = 5 * time.Second

	// apiServerJoinTimeout bounds the join of the API server goroutine during
	// shutdown. startAPIServer terminates on context cancellation on every
	// path, so this is a defensive fallback that should never fire.
	apiServerJoinTimeout = 30 * time.Second
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cbv1alpha1.AddToScheme(scheme))
}

// The following package-level variables are testability seams (E-1). They are
// constant in production (assigned once, never mutated at runtime) and follow
// the existing seam pattern (internal/vault kubeTokenPath): tests substitute
// fakes so run()/setupWebhookCerts are exercisable without a live cluster.
//
//nolint:gochecknoglobals // test seams, constant in production
var (
	// getRestConfig resolves the Kubernetes REST config (panics without a
	// kubeconfig/in-cluster env, hence injectable for tests).
	getRestConfig = ctrl.GetConfigOrDie
	// newManager constructs the controller manager.
	newManager = ctrl.NewManager
	// newDirectClient constructs the uncached API client used for webhook
	// certificate management before the manager cache starts.
	newDirectClient = client.New
	// certRotationInterval is how often the background rotation loop checks
	// whether the webhook certificates need rotation.
	certRotationInterval = 12 * time.Hour
	// oidcStartupRetry supplies the OIDC discovery retry budget; tests shrink
	// the second-scale backoff so exhausted-budget paths stay fast.
	oidcStartupRetry = oidcStartupRetryOpts
	// metricsRegistry is the Prometheus registry the operator metrics are
	// registered with. Production uses the controller-runtime registry so
	// operator metrics share the /metrics endpoint; tests substitute a fresh
	// registry per run() invocation (MustRegister panics on re-registration).
	metricsRegistry prometheus.Registerer = ctrlmetrics.Registry
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return
		}
		slog.Error("operator failed", "error", err)
		cancel()
		os.Exit(1) //nolint:gocritic // intentional exit after cancel
	}
}

func run(ctx context.Context, args []string) error {
	// Derive a cancellable run context: essential components (the REST API
	// server, M-4) cancel it on failure so the manager stops and the operator
	// exits non-zero instead of running degraded.
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// Define and parse the operator command-line flags, then bind them into
	// the config loader so the documented precedence holds:
	// ENV > flag > config file > default.
	flags := config.OperatorFlagSet()
	if err := flags.Parse(args); err != nil {
		return err
	}

	// Load configuration.
	loader := config.NewLoaderWithFlags(os.Getenv("CLOUDBERRY_CONFIG_FILE"), flags)
	cfg, err := loader.Load()
	if err != nil {
		return err
	}

	// Initialize logger.
	logger := util.NewLogger(cfg.LogLevel, util.LogFormat(cfg.LogFormat), os.Stdout)
	slog.SetDefault(logger)
	ctrl.SetLogger(util.SlogToLogr(logger))

	logger.Info("starting cloudberry operator", "version", version)

	// Initialize telemetry. ServiceVersion carries the build-time version so
	// exported spans identify the exact operator build (D-7).
	shutdownTracer, err := telemetry.InitTracer(ctx, buildTelemetryConfig(cfg))
	if err != nil {
		logger.Warn("failed to initialize telemetry", "error", err)
	} else {
		//nolint:contextcheck // fresh ctx needed; parent may be canceled
		defer func() {
			// Use a fresh context with timeout for shutdown, since the parent
			// context may already be canceled when this deferred function runs.
			shutdownCtx, shutdownCancel := context.WithTimeout(
				context.Background(), shutdownTimeout,
			)
			defer shutdownCancel()
			if shutdownErr := shutdownTracer(shutdownCtx); shutdownErr != nil {
				logger.Error("failed to shutdown tracer", "error", shutdownErr)
			}
		}()
	}

	// Initialize metrics using controller-runtime's registry so they are
	// exposed on the same /metrics endpoint as the controller-runtime metrics.
	metricsRecorder := metrics.NewPrometheusRecorder(metricsRegistry)

	// Create controller manager.
	mgr, err := newManager(getRestConfig(), buildManagerOptions(cfg))
	if err != nil {
		return err
	}

	// Create an optional Vault client for the admin controller so it can source
	// backup S3 credentials from a Vault path (spec.backup.destination.s3.vaultSecret).
	// When Vault is disabled the client is omitted and the vaultSecret path is skipped.
	adminVaultClient, err := newAdminVaultClient(ctx, cfg, metricsRecorder, logger)
	if err != nil {
		return fmt.Errorf("creating vault client for admin controller: %w", err)
	}
	if closer, ok := adminVaultClient.(vault.Closer); ok {
		// Stop the background Vault token lifetime watcher on shutdown.
		defer closer.Close()
	}

	// Register all controllers and health checks with the manager.
	if err := registerControllers(mgr, cfg, metricsRecorder, logger, adminVaultClient); err != nil {
		return err
	}

	// backgroundWg tracks background goroutines (e.g. cert rotation) to ensure
	// they complete before the process exits.
	var backgroundWg sync.WaitGroup

	// Register admission webhooks when enabled. The shared admin Vault client
	// is passed in so the vault-pki cert source reuses ONE client (single
	// token lifecycle watcher; no leaked second client, L-2).
	if cfg.WebhookEnabled {
		if err := setupWebhookCerts(ctx, mgr, cfg, logger, &backgroundWg, metricsRecorder,
			adminVaultClient); err != nil {
			return fmt.Errorf("setting up webhook certificates: %w", err)
		}
		if err := registerWebhooks(mgr, logger, metricsRecorder); err != nil {
			return fmt.Errorf("registering webhooks: %w", err)
		}
	} else {
		logger.Info("admission webhooks disabled")
	}

	// Start the REST API server in a background goroutine. The API is an
	// essential component: a startup failure is logged immediately and
	// cancels the run context so the operator shuts down (and run() returns
	// the API error from apiErrCh) instead of running without its API.
	apiErrCh := make(chan error, 1)
	go func() {
		apiErr := startAPIServer(ctx, cfg, mgr, metricsRecorder, logger)
		handleAPIServerExit(apiErr, cancelRun, logger)
		apiErrCh <- apiErr
	}()

	logger.Info("starting cloudberry-operator",
		"metricsAddress", cfg.MetricsAddress,
		"healthProbeAddress", cfg.HealthProbeAddress,
		"apiAddress", cfg.APIAddress,
		"leaderElection", cfg.LeaderElection,
		"webhookEnabled", cfg.WebhookEnabled,
	)

	// Start the controller manager; blocks until the context is canceled.
	mgrErr := mgr.Start(ctx)

	// mgr.Start has returned: either the run context is already canceled or
	// the manager failed on its own. Cancel explicitly (the deferred
	// cancelRun would only fire after run() returns) so the API server and
	// background goroutines observe shutdown before they are joined below.
	cancelRun()

	// Wait for background goroutines (e.g. cert rotation) to finish before
	// returning, so they are not leaked on shutdown.
	backgroundWg.Wait()

	// ALWAYS join the API server goroutine before returning. A non-blocking
	// check here would race the goroutine's send on apiErrCh: a terminal API
	// error could be silently lost and the goroutine would outlive run().
	apiErr := awaitAPIServer(apiErrCh, logger)
	if apiErr != nil && !errors.Is(apiErr, http.ErrServerClosed) {
		apiErr = fmt.Errorf("API server error: %w", apiErr)
	} else {
		apiErr = nil
	}

	// Surface both failures when the manager and the API server both errored;
	// errors.Join drops nils, so single-failure cases keep their error and a
	// clean shutdown returns nil.
	return errors.Join(mgrErr, apiErr)
}

// awaitAPIServer joins the API server goroutine after the run context has
// been canceled. Every wait inside startAPIServer honors context
// cancellation (the manager cache's WaitForCacheSync takes the run ctx, OIDC
// discovery retries select on ctx.Done, and api.StartServer shuts the HTTP
// server down with a bounded timeout once ctx is canceled), so the receive
// completes promptly. The generous bounded fallback only guards against a
// future regression introducing a non-ctx-aware wait, turning a potential
// shutdown deadlock into a loud ERROR instead.
func awaitAPIServer(apiErrCh <-chan error, logger *slog.Logger) error {
	select {
	case apiErr := <-apiErrCh:
		return apiErr
	case <-time.After(apiServerJoinTimeout):
		logger.Error("API server goroutine did not terminate after context cancellation; " +
			"continuing shutdown without joining it")
		return nil
	}
}

// buildTelemetryConfig maps the operator configuration to the telemetry
// initialization config, injecting the build-time version as ServiceVersion.
func buildTelemetryConfig(cfg *config.OperatorConfig) telemetry.Config {
	return telemetry.Config{
		Enabled:        cfg.Telemetry.Enabled,
		OTLPEndpoint:   cfg.Telemetry.OTLPEndpoint,
		OTLPProtocol:   cfg.Telemetry.OTLPProtocol,
		OTLPInsecure:   cfg.Telemetry.OTLPInsecure,
		SamplingRate:   cfg.Telemetry.SamplingRate,
		ServiceName:    cfg.Telemetry.ServiceName,
		ServiceVersion: version,
	}
}

// buildManagerOptions maps the loaded operator configuration to the
// controller-manager options (B-2/M-2):
//   - WebhookPort configures the webhook server (default cert dir is kept:
//     controller-runtime serves from /tmp/k8s-webhook-server/serving-certs).
//   - Namespace restricts the cache to a single namespace; empty keeps the
//     historical cluster-wide watch.
func buildManagerOptions(cfg *config.OperatorConfig) ctrl.Options {
	opts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddress,
		},
		HealthProbeBindAddress: cfg.HealthProbeAddress,
		LeaderElection:         cfg.LeaderElection,
		LeaderElectionID:       "cloudberry-operator-leader-election",
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Port: cfg.WebhookPort,
		}),
	}
	if cfg.Namespace != "" {
		opts.Cache = cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				cfg.Namespace: {},
			},
		}
	}
	return opts
}

// operationTimeoutOverride returns the configured long-operation timeout when
// it was EXPLICITLY changed from the config default, and zero otherwise so
// the reconcilers keep their per-operation hardcoded deadlines (documented
// B-2 contract: defaults preserve current behavior exactly).
func operationTimeoutOverride(cfg *config.OperatorConfig) time.Duration {
	if cfg.OperationTimeout != config.DefaultOperationTimeout {
		return cfg.OperationTimeout
	}
	return 0
}

// registerControllers creates and registers all reconcilers and health checks
// with the controller manager. The optional adminVaultClient (nil when Vault
// is disabled) is created by the caller so its lifecycle (Close on shutdown)
// is owned in one place.
func registerControllers(
	mgr ctrl.Manager,
	cfg *config.OperatorConfig,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
	adminVaultClient vault.Client,
) error {
	// Create resource builder.
	resourceBuilder := builder.NewBuilder()

	// Create database client factory with the metrics recorder so query-history
	// metrics are recorded by created clients.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger, metricsRecorder)

	// Create event recorder.
	//nolint:staticcheck // v1 events API needed for record.EventRecorder
	eventRecorder := mgr.GetEventRecorderFor("cloudberry-operator")

	// The configured reconcile interval / operation timeout are injected into
	// every reconciler; zero values keep the built-in defaults (B-2/M-2).
	opTimeout := operationTimeoutOverride(cfg)

	// Register cluster controller.
	clusterReconciler := controller.NewClusterReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		eventRecorder,
		resourceBuilder,
		metricsRecorder,
		logger,
		dbFactory,
	)
	clusterReconciler.SetIntervals(cfg.ReconcileInterval, opTimeout)
	// Wire the optional Vault client + operator PKI settings so the cluster
	// controller can auto-issue cluster server certificates (spec.auth.ssl)
	// from the SAME Vault PKI mount/role used for webhook certificates. A nil
	// client (Vault disabled) leaves auto-issuance off.
	clusterReconciler.SetClusterTLS(adminVaultClient, cfg.VaultPKIMountPath, cfg.VaultPKIRole)
	if err := clusterReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up cluster controller: %w", err)
	}

	// Register HA controller.
	haReconciler := controller.NewHAReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		eventRecorder,
		dbFactory,
		resourceBuilder,
		metricsRecorder,
		logger,
	)
	haReconciler.SetIntervals(cfg.ReconcileInterval, opTimeout)
	if err := haReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up HA controller: %w", err)
	}

	// Register auth controller.
	authReconciler := controller.NewAuthReconciler(
		mgr.GetClient(),
		eventRecorder,
		resourceBuilder,
		metricsRecorder,
		logger,
	)
	if err := authReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up auth controller: %w", err)
	}

	// Register admin controller.
	adminReconciler := controller.NewAdminReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		eventRecorder,
		resourceBuilder,
		dbFactory,
		metricsRecorder,
		logger,
		adminVaultClient,
	)
	adminReconciler.SetIntervals(cfg.ReconcileInterval, opTimeout)
	if err := adminReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up admin controller: %w", err)
	}

	// Add health checks.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("adding healthz check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("adding readyz check: %w", err)
	}

	return nil
}

// buildVaultClientConfig maps the loaded operator Vault configuration to the
// vault client config. It is the single place where config.VaultConfig is
// translated to vault.Config so every consumer (admin controller, webhook PKI
// cert manager) wires the same fields, including the AppRole credentials
// (RoleID/SecretID, bound to CLOUDBERRY_VAULT_ROLE_ID and
// CLOUDBERRY_VAULT_SECRET_ID). The vault client itself implements the
// backward-compatible fallback to Role/Token when they are empty.
func buildVaultClientConfig(cfg *config.OperatorConfig) vault.Config {
	return vault.Config{
		Enabled:    true,
		Address:    cfg.Vault.Address,
		AuthMethod: cfg.Vault.AuthMethod,
		AuthPath:   cfg.Vault.AuthPath,
		Role:       cfg.Vault.Role,
		Token:      cfg.Vault.Token.Value(),
		RoleID:     cfg.Vault.RoleID,
		SecretID:   cfg.Vault.SecretID.Value(),
		SecretPath: cfg.Vault.SecretPath,
	}
}

// newAdminVaultClient builds the optional Vault client used by the admin
// controller to source backup S3 credentials. It returns (nil, nil) when Vault
// is disabled so the reconciler runs without Vault support and the vaultSecret
// credential path is skipped with a warning.
func newAdminVaultClient(
	ctx context.Context,
	cfg *config.OperatorConfig,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
) (vault.Client, error) {
	if !cfg.Vault.Enabled {
		return nil, nil
	}
	vc, err := vault.NewClient(ctx, buildVaultClientConfig(cfg), logger, metricsRecorder)
	if err != nil {
		return nil, err
	}
	logger.Info("vault client created for admin controller backup S3 credentials",
		"address", cfg.Vault.Address,
		"authMethod", cfg.Vault.AuthMethod,
	)
	return vc, nil
}

// startAPIServer creates and starts the REST API server.
// It sets up basic authentication with an in-memory credential store and
// returns when the server shuts down or encounters an error.
func startAPIServer(
	ctx context.Context,
	cfg *config.OperatorConfig,
	mgr ctrl.Manager,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
) error {
	// Wait for the manager's cache to sync before using the cached client.
	// The API server goroutine is launched before mgr.Start(), so the cache
	// may not be ready yet. Without this wait, the cached client returns
	// ErrCacheNotStarted and the API server silently fails to start.
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return fmt.Errorf("timed out waiting for manager cache to sync")
	}
	logger.Info("manager cache synced, initializing API server")

	// Create an in-memory credential store with an admin user.
	credStore := auth.NewInMemoryCredentialStore()
	adminPassword, err := resolveAdminPassword(ctx, mgr.GetClient(), logger)
	if err != nil {
		return fmt.Errorf("resolving admin password: %w", err)
	}
	credStore.SetCredentials("admin", adminPassword, auth.PermissionAdmin)

	// Optionally seed well-known TEST users (opt-in via
	// CLOUDBERRY_ENABLE_TEST_USERS=true; default off — see A-1/C-1).
	seedTestUsers(cfg, credStore, logger)

	// Create the basic auth provider.
	basicProvider := auth.NewBasicAuthProvider(credStore, logger)

	// Create the OIDC provider when enabled.
	oidcProvider := buildOIDCProvider(ctx, cfg, logger, metricsRecorder)

	// Create the auth middleware with both providers.
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, logger, metricsRecorder)

	// Create database client factory for session operations, propagating the
	// metrics recorder so query-history metrics are recorded.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger, metricsRecorder)

	// Create and start the API server.
	apiServer := api.NewServer(mgr.GetClient(), authMW, dbFactory, metricsRecorder, logger, cfg.APIRateLimit, credStore)
	defer apiServer.Close()

	// Inject a typed Kubernetes clientset so endpoints that stream pod logs
	// (e.g. backup Job logs) work; the controller-runtime client cannot stream.
	if clientset, csErr := kubernetes.NewForConfig(mgr.GetConfig()); csErr != nil {
		logger.Warn("failed to build Kubernetes clientset; Job log streaming will be unavailable",
			"error", csErr)
	} else {
		apiServer.WithClientset(clientset)
	}

	logger.Info("starting REST API server", "address", cfg.APIAddress, "rateLimit", cfg.APIRateLimit)
	return api.StartServer(ctx, cfg.APIAddress, apiServer.Handler(), logger)
}

// handleAPIServerExit reacts to the API server goroutine terminating (M-4).
// The REST API is an essential operator component: any startup or runtime
// failure is logged immediately and cancels the run context so the manager
// stops and the operator exits non-zero, instead of silently running without
// its API. A nil error or http.ErrServerClosed (clean shutdown) is a no-op.
func handleAPIServerExit(err error, cancel context.CancelFunc, logger *slog.Logger) {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return
	}
	logger.Error("REST API server failed; shutting down operator", "error", err)
	cancel()
}

// seedTestUsers registers the well-known test users (one per permission
// level) in the credential store, ONLY when explicitly enabled via
// CLOUDBERRY_ENABLE_TEST_USERS=true (Config.EnableTestUsers, default false).
//
// SECURITY: these credentials are publicly known (they live in this source
// file) and grant up to Operator-level access. They exist exclusively for
// e2e/access-control test suites and must never be enabled in production —
// hence the loud warning when the gate is open.
func seedTestUsers(
	cfg *config.OperatorConfig,
	credStore *auth.InMemoryCredentialStore,
	logger *slog.Logger,
) {
	if !cfg.EnableTestUsers {
		return
	}

	logger.Warn("TEST USERS ENABLED: registering publicly known test credentials " +
		"(basic_user, opbasic_user, operator_user); NEVER enable CLOUDBERRY_ENABLE_TEST_USERS " +
		"in production environments")

	credStore.SetCredentials("basic_user", "basic_pass", auth.PermissionBasic)
	credStore.SetCredentials("opbasic_user", "opbasic_pass", auth.PermissionOperatorBasic)
	credStore.SetCredentials("operator_user", "operator_pass", auth.PermissionOperator)
}

// oidcStartupRetryOpts is the bounded retry budget for OIDC discovery at
// startup: long enough to ride out a brief IdP hiccup, short enough not to
// stall API server startup (lazy re-init covers longer outages).
func oidcStartupRetryOpts() util.RetryOptions {
	return util.RetryOptions{
		MaxRetries:     3,
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
}

// buildOIDCProvider constructs the lazily initialized OIDC provider when OIDC
// is enabled (nil otherwise). Discovery is attempted eagerly with a bounded
// exponential-backoff budget; when it still fails, Bearer auth is NOT
// permanently disabled — the lazy provider retries discovery on the first
// Bearer request (with a cooldown), so auth recovers without a pod restart.
func buildOIDCProvider(
	ctx context.Context,
	cfg *config.OperatorConfig,
	logger *slog.Logger,
	metricsRecorder metrics.Recorder,
) auth.Provider {
	if !cfg.OIDC.Enabled {
		return nil
	}

	roleMapping := cfg.OIDC.RoleMapping
	if len(roleMapping) == 0 {
		roleMapping = map[string]string{
			"admin":          "Admin",
			"operator":       "Operator",
			"operator-basic": "Operator Basic",
			"user":           "Basic",
			"reader":         "Self Only",
		}
	}
	oidcCfg := auth.OIDCConfig{
		IssuerURL:       cfg.OIDC.IssuerURL,
		ClientID:        cfg.OIDC.ClientID,
		ClientSecret:    cfg.OIDC.ClientSecret.Value(),
		RoleClaimPath:   cfg.OIDC.RoleClaimPath,
		RoleClaimSource: cfg.OIDC.RoleClaimSource,
		RoleMatchMode:   cfg.OIDC.RoleMatchMode,
		RoleMapping:     roleMapping,
	}

	lazyProvider := auth.NewLazyOIDCProvider(oidcCfg, logger, metricsRecorder)
	if err := util.RetryWithBackoff(ctx, oidcStartupRetry(), func(retryCtx context.Context) error {
		return lazyProvider.Init(retryCtx)
	}); err != nil {
		logger.Warn("OIDC discovery failed at startup; Bearer auth will lazily retry "+
			"initialization on the first Bearer request",
			"error", err,
			"issuerURL", cfg.OIDC.IssuerURL,
			"clientID", cfg.OIDC.ClientID,
		)
	} else {
		logger.Info("OIDC authentication enabled",
			"issuerURL", cfg.OIDC.IssuerURL,
			"clientID", cfg.OIDC.ClientID,
		)
	}
	return lazyProvider
}

// resolveAdminPassword determines the API admin password using the following
// priority order:
//  1. CLOUDBERRY_API_ADMIN_PASSWORD environment variable (highest priority).
//  2. Existing Kubernetes Secret (survives pod restarts).
//  3. Generate a new random password and persist it to a Secret.
func resolveAdminPassword(ctx context.Context, k8sClient client.Client, logger *slog.Logger) (string, error) {
	// 1. Check environment variable first (highest priority).
	if envPassword := os.Getenv("CLOUDBERRY_API_ADMIN_PASSWORD"); envPassword != "" {
		return envPassword, nil
	}

	// Determine the operator namespace.
	operatorNS := os.Getenv("POD_NAMESPACE")
	if operatorNS == "" {
		operatorNS = util.OperatorNamespace
	}

	// 2. Check if a persisted Secret already exists.
	existing := &corev1.Secret{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      util.OperatorAdminPasswordSecretName,
		Namespace: operatorNS,
	}, existing)

	if err == nil {
		// Secret exists — use the stored password.
		if pw, ok := existing.Data[util.PasswordSecretKey]; ok && len(pw) > 0 {
			logger.Info("using admin password from existing Secret",
				"secret", util.OperatorAdminPasswordSecretName, "namespace", operatorNS)
			return string(pw), nil
		}
		// Secret exists but has no password key — fall through to generate.
		logger.Warn("admin password Secret exists but is empty, generating new password",
			"secret", util.OperatorAdminPasswordSecretName)
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("checking admin password secret: %w", err)
	}

	// 3. Generate a new random password and persist it.
	generated, genErr := util.GenerateRandomPassword()
	if genErr != nil {
		return "", fmt.Errorf("generating admin password: %w", genErr)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.OperatorAdminPasswordSecretName,
			Namespace: operatorNS,
			Labels: map[string]string{
				util.LabelManagedBy: util.LabelManagedByValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			util.PasswordSecretKey: []byte(generated),
		},
	}

	if createErr := k8sClient.Create(ctx, secret); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			// Race condition: another replica created it first — re-read.
			if getErr := k8sClient.Get(ctx, types.NamespacedName{
				Name:      util.OperatorAdminPasswordSecretName,
				Namespace: operatorNS,
			}, existing); getErr != nil {
				return "", fmt.Errorf("re-reading admin password secret after conflict: %w", getErr)
			}
			if pw, ok := existing.Data[util.PasswordSecretKey]; ok && len(pw) > 0 {
				return string(pw), nil
			}
		}
		return "", fmt.Errorf("creating admin password secret: %w", createErr)
	}

	logger.Warn("CLOUDBERRY_API_ADMIN_PASSWORD not set, generated and persisted password to Secret",
		"secret", util.OperatorAdminPasswordSecretName, "namespace", operatorNS,
		"hint", "set CLOUDBERRY_API_ADMIN_PASSWORD environment variable for production use")

	return generated, nil
}

// operatorTracerName is the tracer name for operator startup spans.
const operatorTracerName = "cloudberry-operator"

// setupWebhookCerts creates and manages webhook TLS certificates. The
// optional adminVaultClient (nil when operator-level Vault is disabled) is
// REUSED for the vault-pki cert source so only one Vault client (and one
// token lifecycle watcher) exists; a dedicated client is created — and closed
// when rotation stops — only when no shared client is available (L-2).
func setupWebhookCerts(
	ctx context.Context,
	mgr ctrl.Manager,
	cfg *config.OperatorConfig,
	logger *slog.Logger,
	wg *sync.WaitGroup,
	metricsRecorder metrics.Recorder,
	adminVaultClient vault.Client,
) (err error) {
	// Startup span for certificate provisioning (D-7); records the error
	// status on failure. No-op when telemetry is disabled.
	ctx, span := telemetry.StartSpan(ctx, operatorTracerName, "operator.setupWebhookCerts")
	defer func() {
		telemetry.SetSpanError(span, err)
		span.End()
	}()
	// Determine the operator namespace from the POD_NAMESPACE env var (set by
	// the Helm deployment via the downward API), falling back to the configured
	// watch namespace, and finally to the compile-time default.
	operatorNS := os.Getenv("POD_NAMESPACE")
	if operatorNS == "" {
		operatorNS = cfg.Namespace
	}
	if operatorNS == "" {
		operatorNS = util.OperatorNamespace
	}

	certCfg := certmanager.Config{
		ServiceName:       cfg.WebhookServiceName,
		ServiceNamespace:  operatorNS,
		SecretName:        cfg.WebhookCertSecretName,
		SecretNamespace:   operatorNS,
		CertSource:        cfg.WebhookCertSource,
		VaultPKIMountPath: cfg.VaultPKIMountPath,
		VaultPKIRole:      cfg.VaultPKIRole,
	}

	// Use a direct API client (not the cached manager client) because the
	// manager cache has not been started yet at this point in the lifecycle.
	directClient, err := newDirectClient(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return fmt.Errorf("creating direct API client for cert management: %w", err)
	}

	// Resolve the vault client when the cert source is vault-pki so that the
	// certmanager can issue certificates via the Vault PKI engine. ownedVault
	// is non-nil only when a DEDICATED client was created here; it is closed
	// when cert rotation stops (or on an error return below).
	var vaultClient vault.Client
	var ownedVault vault.Closer
	if cfg.WebhookCertSource == certmanager.CertSourceVaultPKI {
		vaultClient, ownedVault, err = resolveWebhookPKIVaultClient(
			ctx, cfg, adminVaultClient, logger, metricsRecorder)
		if err != nil {
			return fmt.Errorf("creating vault client for webhook cert management: %w", err)
		}
	}
	defer func() {
		// Error exit before the rotation goroutine took ownership: close the
		// dedicated client so its token lifecycle watcher is not leaked.
		if err != nil && ownedVault != nil {
			ownedVault.Close()
		}
	}()

	cm := certmanager.New(directClient, vaultClient, certCfg, logger, metricsRecorder)

	caBundle, err := cm.EnsureCertificates(ctx)
	if err != nil {
		return fmt.Errorf("ensuring webhook certificates: %w", err)
	}

	logger.Info("webhook certificates ready",
		"caBundle_len", len(caBundle),
		"certSource", cfg.WebhookCertSource,
	)

	// Inject the CA bundle into webhook configurations so the API server
	// can verify the self-signed certificate used by the webhook server.
	// Retry with exponential backoff to handle transient API server errors
	// during startup or network instability.
	retryOpts := util.RetryOptions{
		MaxRetries:     5,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
	attempt := 0
	if err := util.RetryWithBackoff(ctx, retryOpts, func(retryCtx context.Context) error {
		// Each retry is recorded as a span event (not a separate span) so a
		// flaky API server during startup is visible on the cert span (D-7).
		attempt++
		telemetry.AddSpanEvent(span, "injectCABundle.attempt", attribute.Int("attempt", attempt))
		return injectCABundle(retryCtx, directClient, caBundle, logger)
	}); err != nil {
		return fmt.Errorf("injecting CA bundle into webhook configurations: %w", err)
	}

	// Start background goroutine for certificate rotation. It owns the
	// dedicated Vault client (if any) and closes it when rotation stops.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if ownedVault != nil {
			defer ownedVault.Close()
		}
		runCertRotation(ctx, cm, logger)
	}()

	return nil
}

// resolveWebhookPKIVaultClient returns the Vault client used for webhook PKI
// certificate management (certSource=vault-pki). It reuses the shared admin
// Vault client when one is wired (operator-level Vault enabled), so a single
// client instance — and a single token lifecycle watcher — serves both the
// admin controller and the cert manager. Only when operator-level Vault is
// disabled (adminVaultClient nil) is a dedicated client constructed; the
// returned Closer is non-nil exactly in that case and the caller must close
// it when cert rotation stops.
func resolveWebhookPKIVaultClient(
	ctx context.Context,
	cfg *config.OperatorConfig,
	adminVaultClient vault.Client,
	logger *slog.Logger,
	metricsRecorder metrics.Recorder,
) (vault.Client, vault.Closer, error) {
	if adminVaultClient != nil && adminVaultClient.IsEnabled() {
		logger.Info("reusing shared vault client for webhook PKI certificate management",
			"address", cfg.Vault.Address,
			"authMethod", cfg.Vault.AuthMethod,
		)
		return adminVaultClient, nil, nil
	}

	vc, err := vault.NewClient(ctx, buildVaultClientConfig(cfg), logger, metricsRecorder)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("vault client created for webhook PKI certificate management",
		"address", cfg.Vault.Address,
		"authMethod", cfg.Vault.AuthMethod,
	)
	closer, _ := vc.(vault.Closer)
	return vc, closer, nil
}

// injectCABundle patches ValidatingWebhookConfiguration and MutatingWebhookConfiguration
// resources managed by the operator to include the given CA bundle.
func injectCABundle(
	ctx context.Context,
	k8sClient client.Client,
	caBundle []byte,
	logger *slog.Logger,
) (err error) {
	ctx, span := telemetry.StartSpan(ctx, operatorTracerName, "operator.injectCABundle")
	defer func() {
		telemetry.SetSpanError(span, err)
		span.End()
	}()
	// Patch validating webhook configurations.
	vwcList := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
	if err := k8sClient.List(ctx, vwcList, client.MatchingLabels{
		util.LabelPartOf: util.LabelPartOfValue,
	}); err != nil {
		return fmt.Errorf("listing validating webhook configurations: %w", err)
	}

	for i := range vwcList.Items {
		vwc := &vwcList.Items[i]
		patched := false
		for j := range vwc.Webhooks {
			vwc.Webhooks[j].ClientConfig.CABundle = caBundle
			patched = true
		}
		if patched {
			if err := k8sClient.Update(ctx, vwc); err != nil {
				return fmt.Errorf("updating validating webhook configuration %s: %w", vwc.Name, err)
			}
			logger.Info("injected CA bundle into validating webhook configuration", "name", vwc.Name)
		}
	}

	// Patch mutating webhook configurations.
	mwcList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
	if err := k8sClient.List(ctx, mwcList, client.MatchingLabels{
		util.LabelPartOf: util.LabelPartOfValue,
	}); err != nil {
		return fmt.Errorf("listing mutating webhook configurations: %w", err)
	}

	for i := range mwcList.Items {
		mwc := &mwcList.Items[i]
		patched := false
		for j := range mwc.Webhooks {
			mwc.Webhooks[j].ClientConfig.CABundle = caBundle
			patched = true
		}
		if patched {
			if err := k8sClient.Update(ctx, mwc); err != nil {
				return fmt.Errorf("updating mutating webhook configuration %s: %w", mwc.Name, err)
			}
			logger.Info("injected CA bundle into mutating webhook configuration", "name", mwc.Name)
		}
	}

	return nil
}

// runCertRotation periodically checks and rotates webhook certificates.
func runCertRotation(ctx context.Context, cm certmanager.CertManager, logger *slog.Logger) {
	ticker := time.NewTicker(certRotationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			needs, err := cm.NeedsRotation(ctx)
			if err != nil {
				logger.Error("failed to check certificate rotation", "error", err)
				continue
			}
			if needs {
				logger.Info("rotating webhook certificates")
				if _, rotErr := cm.EnsureCertificates(ctx); rotErr != nil {
					logger.Error("failed to rotate certificates", "error", rotErr)
				} else {
					logger.Info("webhook certificates rotated successfully")
				}
			}
		}
	}
}

// registerWebhooks registers the validating and mutating admission webhooks
// for CloudberryCluster resources with the controller manager.
func registerWebhooks(mgr ctrl.Manager, logger *slog.Logger, metricsRecorder metrics.Recorder) error {
	// Register the validating webhook.
	if err := ctrl.NewWebhookManagedBy(mgr, &cbv1alpha1.CloudberryCluster{}).
		WithValidator(webhook.NewCloudberryClusterValidator(mgr.GetClient(), metricsRecorder)).
		Complete(); err != nil {
		return fmt.Errorf("creating validating webhook: %w", err)
	}
	logger.Info("registered validating webhook for CloudberryCluster")

	// Register the mutating webhook.
	if err := ctrl.NewWebhookManagedBy(mgr, &cbv1alpha1.CloudberryCluster{}).
		WithDefaulter(webhook.NewCloudberryClusterDefaulter(metricsRecorder)).
		Complete(); err != nil {
		return fmt.Errorf("creating mutating webhook: %w", err)
	}
	logger.Info("registered mutating webhook for CloudberryCluster")

	return nil
}
