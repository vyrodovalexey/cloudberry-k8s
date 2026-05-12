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
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/prometheus/client_golang/prometheus"
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

	// Initialize metrics.
	metricsRecorder := metrics.NewPrometheusRecorder(prometheus.DefaultRegisterer)

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
	// Create an in-memory credential store with a default admin user.
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "admin", auth.PermissionAdmin)

	// Create the basic auth provider and middleware.
	basicProvider := auth.NewBasicAuthProvider(credStore, logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, logger, metricsRecorder)

	// Create and start the API server.
	apiServer := api.NewServer(mgr.GetClient(), authMW, metricsRecorder, logger)

	logger.Info("starting REST API server", "address", cfg.APIAddress)
	return api.StartServer(ctx, cfg.APIAddress, apiServer.Handler(), logger)
}

// registerWebhooks registers the validating and mutating admission webhooks
// for CloudberryCluster resources with the controller manager.
func registerWebhooks(mgr ctrl.Manager, logger *slog.Logger) error {
	// Register the validating webhook.
	if err := ctrl.NewWebhookManagedBy(mgr, &cbv1alpha1.CloudberryCluster{}).
		WithValidator(webhook.NewCloudberryClusterValidator()).
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
