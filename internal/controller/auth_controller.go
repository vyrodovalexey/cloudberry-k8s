package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const authControllerName = "auth-controller"

// AuthReconciler reconciles the authentication aspects of a CloudberryCluster.
type AuthReconciler struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
	builder  builder.ResourceBuilder
	metrics  metrics.Recorder
	logger   *slog.Logger
}

// NewAuthReconciler creates a new AuthReconciler.
func NewAuthReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	b builder.ResourceBuilder,
	m metrics.Recorder,
	logger *slog.Logger,
) *AuthReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuthReconciler{
		client:   c,
		scheme:   scheme,
		recorder: recorder,
		builder:  b,
		metrics:  m,
		logger:   logger.With("controller", authControllerName),
	}
}

// Reconcile handles the auth reconciliation for CloudberryCluster resources.
func (r *AuthReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.logger.With("cluster", req.Name, "namespace", req.Namespace)
	ctx = util.WithLogger(ctx, logger)

	ctx, span := telemetry.StartSpan(ctx, authControllerName, "Reconcile")
	defer span.End()

	// Fetch the CloudberryCluster resource.
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching cluster: %w", err)
	}

	// Skip if cluster is not running or initializing.
	if cluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning &&
		cluster.Status.Phase != cbv1alpha1.ClusterPhaseInitializing {
		return ctrl.Result{RequeueAfter: requeueAfterDefault}, nil
	}

	// Skip full reconciliation if only status changed (ObservedGeneration matches).
	if cluster.Status.ObservedGeneration == cluster.Generation {
		logger.Info("skipping auth reconciliation, generation unchanged")
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Reconcile pg_hba.conf.
	if err := r.reconcileHBA(ctx, cluster); err != nil {
		logger.Error("failed to reconcile HBA", "error", err)
		cluster.Status.Conditions = util.SetCondition(
			cluster.Status.Conditions,
			string(cbv1alpha1.ConditionAuthConfigured),
			metav1.ConditionFalse,
			"HBAReconcileFailed",
			fmt.Sprintf("Failed to reconcile pg_hba.conf: %v", err),
		)
		if statusErr := patchStatus(ctx, r.client, cluster); statusErr != nil {
			logger.Error("failed to update status", "error", statusErr)
		}
		return ctrl.Result{RequeueAfter: requeueAfterError}, err
	}

	// Record event for auth configuration changes.
	r.recorder.Event(cluster, "Normal", "AuthReconciled", "Authentication configuration reconciled")

	// Validate OIDC configuration if enabled.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.OIDC != nil && cluster.Spec.Auth.OIDC.Enabled {
		if err := r.validateOIDCConfig(ctx, cluster); err != nil {
			logger.Warn("OIDC validation failed", "error", err)
			r.recorder.Event(cluster, "Warning", "OIDCValidationFailed",
				fmt.Sprintf("OIDC validation failed: %v", err))
		} else {
			r.recorder.Event(cluster, "Normal", "OIDCConfigured",
				"OIDC authentication is properly configured")
		}
	}

	// Update auth condition.
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionAuthConfigured),
		metav1.ConditionTrue,
		"AuthConfigured",
		"Authentication is properly configured",
	)

	// Use Status().Patch() with MergePatch to prevent clobbering status changes
	// from other controllers.
	if err := patchStatus(ctx, r.client, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating auth status: %w", err)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// reconcileHBA ensures the pg_hba.conf ConfigMap is in the desired state.
func (r *AuthReconciler) reconcileHBA(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) error {
	desired := r.builder.BuildPgHBAConfConfigMap(cluster)

	existing := desired.DeepCopy()
	err := r.client.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("creating pg_hba.conf configmap: %w", createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting pg_hba.conf configmap: %w", err)
	}

	// Check if content changed.
	existingHash := existing.Annotations[util.AnnotationConfigHash]
	desiredHash := desired.Annotations[util.AnnotationConfigHash]

	if existingHash != desiredHash {
		existing.Data = desired.Data
		existing.Annotations = desired.Annotations
		if updateErr := r.client.Update(ctx, existing); updateErr != nil {
			return fmt.Errorf("updating pg_hba.conf configmap: %w", updateErr)
		}
		util.LoggerFromContext(ctx).Info("pg_hba.conf updated")
	}

	return nil
}

// validateOIDCConfig validates the OIDC configuration.
func (r *AuthReconciler) validateOIDCConfig(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	oidc := cluster.Spec.Auth.OIDC

	if oidc.IssuerURL == "" {
		return fmt.Errorf("OIDC issuer URL is required when OIDC is enabled")
	}
	if oidc.ClientID == "" {
		return fmt.Errorf("OIDC client ID is required when OIDC is enabled")
	}

	util.LoggerFromContext(ctx).Info("OIDC configuration validated",
		"issuerURL", oidc.IssuerURL,
		"clientID", oidc.ClientID,
	)

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AuthReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cbv1alpha1.CloudberryCluster{}).
		Named(authControllerName).
		Complete(r)
}
