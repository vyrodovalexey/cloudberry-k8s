// Package main is the entry point for the cloudberry-operator.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cbv1alpha1.AddToScheme(scheme))
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("operator failed", "error", err)
		cancel()
		os.Exit(1) //nolint:gocritic // intentional exit after cancel
	}
}

func run(ctx context.Context) error {
	// Load configuration.
	loader := config.NewLoader(os.Getenv("CLOUDBERRY_CONFIG_FILE"))
	cfg, err := loader.Load()
	if err != nil {
		return err
	}

	// Initialize logger.
	logger := util.NewLogger(cfg.LogLevel, util.LogFormat(cfg.LogFormat), os.Stdout)
	slog.SetDefault(logger)
	ctrl.SetLogger(util.SlogToLogr(logger))

	logger.Info("starting cloudberry operator", "version", version)

	// Initialize telemetry.
	shutdownTracer, err := telemetry.InitTracer(ctx, telemetry.Config{
		Enabled:        cfg.Telemetry.Enabled,
		OTLPEndpoint:   cfg.Telemetry.OTLPEndpoint,
		OTLPProtocol:   cfg.Telemetry.OTLPProtocol,
		OTLPInsecure:   cfg.Telemetry.OTLPInsecure,
		SamplingRate:   cfg.Telemetry.SamplingRate,
		ServiceName:    cfg.Telemetry.ServiceName,
		ServiceVersion: "1.0.0",
	})
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
	metricsRecorder := metrics.NewPrometheusRecorder(ctrlmetrics.Registry)

	// Create controller manager.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddress,
		},
		HealthProbeBindAddress: cfg.HealthProbeAddress,
		LeaderElection:         cfg.LeaderElection,
		LeaderElectionID:       "cloudberry-operator-leader-election",
	})
	if err != nil {
		return err
	}

	// Register all controllers and health checks with the manager.
	if err := registerControllers(ctx, mgr, cfg, metricsRecorder, logger); err != nil {
		return err
	}

	// backgroundWg tracks background goroutines (e.g. cert rotation) to ensure
	// they complete before the process exits.
	var backgroundWg sync.WaitGroup

	// Register admission webhooks when enabled.
	if cfg.WebhookEnabled {
		if err := setupWebhookCerts(ctx, mgr, cfg, logger, &backgroundWg, metricsRecorder); err != nil {
			return fmt.Errorf("setting up webhook certificates: %w", err)
		}
		if err := registerWebhooks(mgr, logger, metricsRecorder); err != nil {
			return fmt.Errorf("registering webhooks: %w", err)
		}
	} else {
		logger.Info("admission webhooks disabled")
	}

	// Start the REST API server in a background goroutine.
	apiErrCh := make(chan error, 1)
	go func() {
		apiErrCh <- startAPIServer(ctx, cfg, mgr, metricsRecorder, logger)
	}()

	logger.Info("starting cloudberry-operator",
		"metricsAddress", cfg.MetricsAddress,
		"healthProbeAddress", cfg.HealthProbeAddress,
		"apiAddress", cfg.APIAddress,
		"leaderElection", cfg.LeaderElection,
		"webhookEnabled", cfg.WebhookEnabled,
	)

	// Start the controller manager; blocks until the context is canceled.
	if err := mgr.Start(ctx); err != nil {
		return err
	}

	// Wait for background goroutines (e.g. cert rotation) to finish before
	// returning, so they are not leaked on shutdown.
	backgroundWg.Wait()

	// Check if the API server returned an error before the manager stopped.
	select {
	case apiErr := <-apiErrCh:
		if apiErr != nil {
			return fmt.Errorf("API server error: %w", apiErr)
		}
	default:
		// API server is still shutting down; no error yet.
	}

	return nil
}

// registerControllers creates and registers all reconcilers and health checks
// with the controller manager.
func registerControllers(
	ctx context.Context,
	mgr ctrl.Manager,
	cfg *config.OperatorConfig,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
) error {
	// Create resource builder.
	resourceBuilder := builder.NewBuilder()

	// Create database client factory with the metrics recorder so query-history
	// metrics are recorded by created clients.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger, metricsRecorder)

	// Create event recorder.
	//nolint:staticcheck // v1 events API needed for record.EventRecorder
	eventRecorder := mgr.GetEventRecorderFor("cloudberry-operator")

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

	// Create an optional Vault client for the admin controller so it can source
	// backup S3 credentials from a Vault path (spec.backup.destination.s3.vaultSecret).
	// When Vault is disabled the client is omitted and the vaultSecret path is skipped.
	adminVaultClient, err := newAdminVaultClient(ctx, cfg, metricsRecorder, logger)
	if err != nil {
		return fmt.Errorf("creating vault client for admin controller: %w", err)
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
	vaultCfg := vault.Config{
		Enabled:    true,
		Address:    cfg.Vault.Address,
		AuthMethod: cfg.Vault.AuthMethod,
		AuthPath:   cfg.Vault.AuthPath,
		Role:       cfg.Vault.Role,
		Token:      cfg.Vault.Token.Value(),
		SecretPath: cfg.Vault.SecretPath,
	}
	vc, err := vault.NewClient(ctx, vaultCfg, logger, metricsRecorder)
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

	// Register additional test users for different permission levels.
	// These are useful for testing access control scenarios.
	credStore.SetCredentials("basic_user", "basic_pass", auth.PermissionBasic)
	credStore.SetCredentials("opbasic_user", "opbasic_pass", auth.PermissionOperatorBasic)
	credStore.SetCredentials("operator_user", "operator_pass", auth.PermissionOperator)

	// Create the basic auth provider.
	basicProvider := auth.NewBasicAuthProvider(credStore, logger)

	// Create the OIDC provider when enabled.
	var oidcProvider auth.Provider
	if cfg.OIDC.Enabled {
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
		provider, oidcErr := auth.NewOIDCProvider(ctx, oidcCfg, logger)
		if oidcErr != nil {
			logger.Warn("failed to initialize OIDC provider, Bearer token auth will be unavailable",
				"error", oidcErr,
				"issuerURL", cfg.OIDC.IssuerURL,
				"clientID", cfg.OIDC.ClientID,
			)
		} else {
			oidcProvider = provider
			logger.Info("OIDC authentication enabled",
				"issuerURL", cfg.OIDC.IssuerURL,
				"clientID", cfg.OIDC.ClientID,
			)
		}
	}

	// Create the auth middleware with both providers.
	authMW := auth.NewAuthMiddleware(basicProvider, oidcProvider, logger, metricsRecorder)

	// Create database client factory for session operations, propagating the
	// metrics recorder so query-history metrics are recorded.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger, metricsRecorder)

	// Create and start the API server.
	apiServer := api.NewServer(mgr.GetClient(), authMW, dbFactory, metricsRecorder, logger, cfg.APIRateLimit, credStore)
	defer apiServer.Close()

	logger.Info("starting REST API server", "address", cfg.APIAddress, "rateLimit", cfg.APIRateLimit)
	return api.StartServer(ctx, cfg.APIAddress, apiServer.Handler(), logger)
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

// setupWebhookCerts creates and manages webhook TLS certificates.
func setupWebhookCerts(
	ctx context.Context,
	mgr ctrl.Manager,
	cfg *config.OperatorConfig,
	logger *slog.Logger,
	wg *sync.WaitGroup,
	metricsRecorder metrics.Recorder,
) error {
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
	directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return fmt.Errorf("creating direct API client for cert management: %w", err)
	}

	// Create a vault client when the cert source is vault-pki so that the
	// certmanager can issue certificates via the Vault PKI engine.
	var vaultClient vault.Client
	if cfg.WebhookCertSource == certmanager.CertSourceVaultPKI {
		vaultCfg := vault.Config{
			Enabled:    true,
			Address:    cfg.Vault.Address,
			AuthMethod: cfg.Vault.AuthMethod,
			AuthPath:   cfg.Vault.AuthPath,
			Role:       cfg.Vault.Role,
			Token:      cfg.Vault.Token.Value(),
			SecretPath: cfg.Vault.SecretPath,
		}
		vc, vaultErr := vault.NewClient(ctx, vaultCfg, logger, metricsRecorder)
		if vaultErr != nil {
			return fmt.Errorf("creating vault client for webhook cert management: %w", vaultErr)
		}
		vaultClient = vc
		logger.Info("vault client created for webhook PKI certificate management",
			"address", cfg.Vault.Address,
			"authMethod", cfg.Vault.AuthMethod,
		)
	}

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
	if err := util.RetryWithBackoff(ctx, retryOpts, func(retryCtx context.Context) error {
		return injectCABundle(retryCtx, directClient, caBundle, logger)
	}); err != nil {
		return fmt.Errorf("injecting CA bundle into webhook configurations: %w", err)
	}

	// Start background goroutine for certificate rotation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runCertRotation(ctx, cm, logger)
	}()

	return nil
}

// injectCABundle patches ValidatingWebhookConfiguration and MutatingWebhookConfiguration
// resources managed by the operator to include the given CA bundle.
func injectCABundle(ctx context.Context, k8sClient client.Client, caBundle []byte, logger *slog.Logger) error {
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
	ticker := time.NewTicker(12 * time.Hour)
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
