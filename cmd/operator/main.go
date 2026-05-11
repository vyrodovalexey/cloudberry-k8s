// Package main is the entry point for the cloudberry-operator.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/prometheus/client_golang/prometheus"
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

	// Initialize telemetry.
	shutdownTracer, err := telemetry.InitTracer(ctx, telemetry.Config{
		Enabled:        cfg.Telemetry.Enabled,
		OTLPEndpoint:   cfg.Telemetry.OTLPEndpoint,
		OTLPProtocol:   cfg.Telemetry.OTLPProtocol,
		SamplingRate:   cfg.Telemetry.SamplingRate,
		ServiceName:    cfg.Telemetry.ServiceName,
		ServiceVersion: "1.0.0",
	})
	if err != nil {
		logger.Warn("failed to initialize telemetry", "error", err)
	} else {
		defer func() {
			if shutdownErr := shutdownTracer(ctx); shutdownErr != nil {
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

	// Create resource builder.
	resourceBuilder := builder.NewBuilder()

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
		return err
	}

	// Register HA controller.
	haReconciler := controller.NewHAReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		eventRecorder,
		nil, // DB factory will be set up when cluster is running.
		metricsRecorder,
		logger,
	)
	if err := haReconciler.SetupWithManager(mgr); err != nil {
		return err
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
		return err
	}

	// Register admin controller.
	adminReconciler := controller.NewAdminReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		eventRecorder,
		resourceBuilder,
		nil, // DB factory will be set up when cluster is running.
		metricsRecorder,
		logger,
	)
	if err := adminReconciler.SetupWithManager(mgr); err != nil {
		return err
	}

	// Add health checks.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	logger.Info("starting cloudberry-operator",
		"metricsAddress", cfg.MetricsAddress,
		"healthProbeAddress", cfg.HealthProbeAddress,
		"leaderElection", cfg.LeaderElection,
	)

	return mgr.Start(ctx)
}
