// Package main is the entry point for the cloudberry-operator.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
)

// version is set via ldflags at build time (e.g. -X main.version=...).
//
//nolint:gochecknoglobals // set by ldflags
var version = "dev"

// Version returns the build version for use by other packages.
func Version() string { return version }

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
				context.Background(), 5*time.Second,
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
	if err := registerControllers(mgr, metricsRecorder, logger); err != nil {
		return err
	}

	// Register admission webhooks when enabled.
	if cfg.WebhookEnabled {
		if err := setupWebhookCerts(ctx, mgr, cfg, logger); err != nil {
			return fmt.Errorf("setting up webhook certificates: %w", err)
		}
		if err := registerWebhooks(mgr, logger); err != nil {
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
	mgr ctrl.Manager,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
) error {
	// Create resource builder.
	resourceBuilder := builder.NewBuilder()

	// Create database client factory.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger)

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
		metricsRecorder,
		logger,
	)
	if err := haReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up HA controller: %w", err)
	}

	// Register auth controller.
	authReconciler := controller.NewAuthReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
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
	// Create an in-memory credential store with an admin user.
	credStore := auth.NewInMemoryCredentialStore()
	adminPassword := os.Getenv("CLOUDBERRY_API_ADMIN_PASSWORD")
	if adminPassword == "" {
		generated, genErr := util.GenerateRandomPassword()
		if genErr != nil {
			return fmt.Errorf("generating admin password: %w", genErr)
		}
		adminPassword = generated
		logger.Warn("CLOUDBERRY_API_ADMIN_PASSWORD not set, using generated password",
			"hint", "set CLOUDBERRY_API_ADMIN_PASSWORD environment variable for production use")
	}
	credStore.SetCredentials("admin", adminPassword, auth.PermissionAdmin)

	// Create the basic auth provider and middleware.
	basicProvider := auth.NewBasicAuthProvider(credStore, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, metricsRecorder)

	// Create database client factory for session operations.
	dbFactory := db.NewClientFactory(mgr.GetClient(), logger)

	// Create and start the API server.
	apiServer := api.NewServer(mgr.GetClient(), authMW, dbFactory, metricsRecorder, logger)

	logger.Info("starting REST API server", "address", cfg.APIAddress)
	return api.StartServer(ctx, cfg.APIAddress, apiServer.Handler(), logger)
}

// setupWebhookCerts creates and manages webhook TLS certificates.
func setupWebhookCerts(
	ctx context.Context,
	mgr ctrl.Manager,
	cfg *config.OperatorConfig,
	logger *slog.Logger,
) error {
	operatorNS := cfg.Namespace
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

	cm := certmanager.New(mgr.GetClient(), nil, certCfg, logger)

	caBundle, err := cm.EnsureCertificates(ctx)
	if err != nil {
		return fmt.Errorf("ensuring webhook certificates: %w", err)
	}

	logger.Info("webhook certificates ready",
		"caBundle_len", len(caBundle),
		"certSource", cfg.WebhookCertSource,
	)

	// Start background goroutine for certificate rotation.
	go runCertRotation(ctx, cm, logger)

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
func registerWebhooks(mgr ctrl.Manager, logger *slog.Logger) error {
	// Register the validating webhook.
	if err := ctrl.NewWebhookManagedBy(mgr, &cbv1alpha1.CloudberryCluster{}).
		WithValidator(webhook.NewCloudberryClusterValidator(mgr.GetClient())).
		Complete(); err != nil {
		return fmt.Errorf("creating validating webhook: %w", err)
	}
	logger.Info("registered validating webhook for CloudberryCluster")

	// Register the mutating webhook.
	if err := ctrl.NewWebhookManagedBy(mgr, &cbv1alpha1.CloudberryCluster{}).
		WithDefaulter(webhook.NewCloudberryClusterDefaulter()).
		Complete(); err != nil {
		return fmt.Errorf("creating mutating webhook: %w", err)
	}
	logger.Info("registered mutating webhook for CloudberryCluster")

	return nil
}
