package controller

// Cycle-2 fix tests (T14):
//   M-1: backup/restore metric counters are transition-gated (no inflation
//        across no-op reconciles of the same Job).
//   M-2: ensureBackupS3VaultCredentials failures surface a Warning Event and
//        flip the reconcile outcome metric to result="error".
//   L-5: cluster pods roll exactly once per TLS certificate rotation via the
//        pod-template checksum annotation (stable across no-op reconciles).
//   L-7: deterministic deletion-backup Job naming from deletionTimestamp.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// countingBackupRestoreRecorder counts RecordBackup / RecordRestore calls and
// keeps the latest label tuples, embedding NoopRecorder for the rest of the
// Recorder surface (Recorder convention).
type countingBackupRestoreRecorder struct {
	metrics.NoopRecorder
	backupCalls   int
	restoreCalls  int
	lastBackup    recordedCall
	lastRestore   recordedCall
	gaugeStatuses []float64
}

func (m *countingBackupRestoreRecorder) RecordBackup(cluster, ns, backType, result string) {
	m.backupCalls++
	m.lastBackup = recordedCall{cluster: cluster, namespace: ns, backType: backType, result: result}
}

func (m *countingBackupRestoreRecorder) RecordRestore(cluster, ns, result string) {
	m.restoreCalls++
	m.lastRestore = recordedCall{cluster: cluster, namespace: ns, result: result}
}

func (m *countingBackupRestoreRecorder) SetBackupLastStatus(_, _ string, status float64) {
	m.gaugeStatuses = append(m.gaugeStatuses, status)
}

// metricsAdminReconciler builds an AdminReconciler with the counting recorder.
func metricsAdminReconciler(t *testing.T) (*AdminReconciler, *countingBackupRestoreRecorder, *record.FakeRecorder) {
	t.Helper()
	scheme := newTestScheme()
	rec := &countingBackupRestoreRecorder{}
	events := record.NewFakeRecorder(20)
	r := NewAdminReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, events, builder.NewBuilder(), nil, rec, nil,
	)
	return r, rec, events
}

func TestRecordBackup_TransitionGated_SameJobSameStatus(t *testing.T) {
	r, rec, _ := metricsAdminReconciler(t)
	cluster := newTestCluster()

	job := pinningBackupJob("test-cluster-backup-20260201120000", util.BackupOperationBackup, 1, 0)
	for i := 0; i < 3; i++ {
		r.applyBackupJobToStatus(cluster, job, backupStatusSuccess)
	}

	assert.Equal(t, 1, rec.backupCalls,
		"the backup counter must increment once for repeated reconciles of the same Job+status")
	assert.Equal(t, "success", rec.lastBackup.result)
	// The last-status gauge stays recorded on every reconcile (idempotent).
	assert.Len(t, rec.gaugeStatuses, 3)
}

func TestRecordBackup_TransitionGated_StatusChangeIncrements(t *testing.T) {
	r, rec, _ := metricsAdminReconciler(t)
	cluster := newTestCluster()

	running := pinningBackupJob("test-cluster-backup-20260201120000", util.BackupOperationBackup, 0, 0)
	r.applyBackupJobToStatus(cluster, running, backupStatusInProgress)
	r.applyBackupJobToStatus(cluster, running, backupStatusInProgress) // no-op repeat

	done := pinningBackupJob("test-cluster-backup-20260201120000", util.BackupOperationBackup, 1, 0)
	r.applyBackupJobToStatus(cluster, done, backupStatusSuccess)

	assert.Equal(t, 2, rec.backupCalls,
		"one increment per observed status transition (InProgress, then Success)")
}

func TestRecordBackup_TransitionGated_NewJobNameIncrements(t *testing.T) {
	r, rec, _ := metricsAdminReconciler(t)
	cluster := newTestCluster()

	first := pinningBackupJob("test-cluster-backup-20260201120000", util.BackupOperationBackup, 1, 0)
	r.applyBackupJobToStatus(cluster, first, backupStatusSuccess)
	second := pinningBackupJob("test-cluster-backup-20260202120000", util.BackupOperationBackup, 1, 0)
	r.applyBackupJobToStatus(cluster, second, backupStatusSuccess)

	assert.Equal(t, 2, rec.backupCalls, "a new Job name is a transition and increments")
}

func TestRecordRestore_TransitionGated(t *testing.T) {
	r, rec, _ := metricsAdminReconciler(t)
	cluster := newTestCluster()

	job := pinningBackupJob("test-cluster-restore-20260201120000", util.BackupOperationRestore, 0, 1)
	for i := 0; i < 3; i++ {
		r.applyBackupJobToStatus(cluster, job, backupStatusFailed)
	}
	assert.Equal(t, 1, rec.restoreCalls,
		"the restore counter must increment once for repeated reconciles of the same failed Job")
}

func TestRecordBackup_EventsUnchangedByMetricGating(t *testing.T) {
	r, _, events := metricsAdminReconciler(t)
	cluster := newTestCluster()

	failed := pinningBackupJob("test-cluster-backup-20260201130000", util.BackupOperationBackup, 0, 1)
	for i := 0; i < 3; i++ {
		r.applyBackupJobToStatus(cluster, failed, backupStatusFailed)
	}
	assert.Equal(t, 1, countEventsWithReason(drainEvents(events), cbv1alpha1.EventReasonBackupFailed),
		"event de-duplication semantics must be unchanged by the metric gating")
}

// ----------------------------------------------------------------------------
// M-2: Vault credential failures surfaced (event + reconcile outcome metric)
// ----------------------------------------------------------------------------

func TestEnsureBackupS3VaultCredentials_ReadError_EmitsWarningEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	events := record.NewFakeRecorder(10)

	vc := &fakeVaultClient{enabled: true, readErr: errors.New("vault down")}
	r := NewAdminReconciler(k8sClient, scheme, events,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	err := r.ensureBackupS3VaultCredentials(context.Background(), cluster)
	require.Error(t, err)
	assert.Equal(t, 1, countEventsWithReason(drainEvents(events),
		cbv1alpha1.EventReasonBackupVaultCredentialsFailed))
}

func TestEnsureBackupS3VaultCredentials_NilClientSkip_EmitsWarningEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	events := record.NewFakeRecorder(10)

	// No vault client wired: skip must not panic, must warn via Event.
	r := NewAdminReconciler(k8sClient, scheme, events,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Equal(t, 1, countEventsWithReason(drainEvents(events),
		cbv1alpha1.EventReasonBackupVaultCredentialsFailed))
}

func TestEnsureBackupS3VaultCredentials_NoVaultSecret_NoEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster() // credentialSecret, no vaultSecret
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	events := record.NewFakeRecorder(10)

	r := NewAdminReconciler(k8sClient, scheme, events,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Zero(t, countEventsWithReason(drainEvents(events),
		cbv1alpha1.EventReasonBackupVaultCredentialsFailed),
		"no event when no vault credentials are requested (negative case)")
}

func TestEnsureBackupS3VaultCredentials_Success_NoEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	events := record.NewFakeRecorder(10)

	vc := &fakeVaultClient{enabled: true, readData: map[string]interface{}{
		"aws_access_key_id":     "id",
		"aws_secret_access_key": "secret",
	}}
	r := NewAdminReconciler(k8sClient, scheme, events,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Zero(t, countEventsWithReason(drainEvents(events),
		cbv1alpha1.EventReasonBackupVaultCredentialsFailed),
		"success path must stay event-free")
}

func TestAdminReconcile_SubComponentFailure_RecordsErrorOutcome(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	// Force the full reconcile path (generation changed).
	cluster.Generation = 2
	cluster.Status.ObservedGeneration = 1
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}

	vc := &fakeVaultClient{enabled: true, readErr: errors.New("vault down")}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), nil, rec, nil, vc)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err,
		"sub-component failures must not abort the reconcile (existing aggregation style)")

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result,
		"the swallowed sub-component failure must surface as result=error on the outcome metric")
}

// ----------------------------------------------------------------------------
// L-5: TLS cert checksum pod-template annotation
// ----------------------------------------------------------------------------

// tlsChecksumFixture creates a reconciler + cluster with an existing
// operator-managed TLS secret and returns both.
func tlsChecksumFixture(t *testing.T) (*ClusterReconciler, *cbv1alpha1.CloudberryCluster) {
	t.Helper()
	r, _ := tlsTestReconciler(pkiVaultClient())
	cluster := tlsTestCluster()
	certPEM := selfSignedPEM(t, time.Now().Add(-time.Hour), time.Now().Add(365*24*time.Hour))
	require.NoError(t, r.client.Create(context.Background(), operatorManagedTLSSecret(certPEM)))
	return r, cluster
}

func TestApplyClusterTLSChecksum_StableAcrossNoOpReconciles(t *testing.T) {
	r, cluster := tlsChecksumFixture(t)

	build := func() *appsv1.StatefulSet {
		sts, err := builder.NewBuilder().BuildCoordinatorStatefulSet(cluster)
		require.NoError(t, err)
		require.NoError(t, r.applyClusterTLSChecksum(context.Background(), cluster, sts))
		return sts
	}

	first := build()
	checksum := first.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]
	require.NotEmpty(t, checksum, "the checksum annotation must be stamped")

	for i := 0; i < 3; i++ {
		assert.Equal(t, checksum,
			build().Spec.Template.Annotations[util.AnnotationTLSCertChecksum],
			"unchanged secret data must yield a byte-identical annotation (no rollout)")
	}
}

func TestApplyClusterTLSChecksum_ChangesExactlyOncePerRotation(t *testing.T) {
	r, cluster := tlsChecksumFixture(t)

	sts, err := builder.NewBuilder().BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	require.NoError(t, r.applyClusterTLSChecksum(context.Background(), cluster, sts))
	before := sts.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]

	// Rotate: replace the certificate data in the Secret.
	secret := getTLSSecret(t, r)
	secret.Data[secretKeyTLSCert] = []byte("ROTATED-CERT-PEM")
	require.NoError(t, r.client.Update(context.Background(), secret))

	var after string
	for i := 0; i < 3; i++ {
		sts2, err := builder.NewBuilder().BuildCoordinatorStatefulSet(cluster)
		require.NoError(t, err)
		require.NoError(t, r.applyClusterTLSChecksum(context.Background(), cluster, sts2))
		got := sts2.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]
		if after == "" {
			after = got
		}
		assert.Equal(t, after, got, "post-rotation checksum must be stable")
	}
	assert.NotEqual(t, before, after, "rotation must change the annotation exactly once")
}

func TestApplyClusterTLSChecksum_NoSSL_NoAnnotation(t *testing.T) {
	r, _ := tlsTestReconciler(pkiVaultClient())
	cluster := newTestCluster() // no auth.ssl

	sts, err := builder.NewBuilder().BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	require.NoError(t, r.applyClusterTLSChecksum(context.Background(), cluster, sts))
	_, ok := sts.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]
	assert.False(t, ok, "no annotation when SSL is disabled")
}

func TestApplyClusterTLSChecksum_SecretMissing_NoAnnotationNoError(t *testing.T) {
	r, _ := tlsTestReconciler(pkiVaultClient())
	cluster := tlsTestCluster() // secret not created

	sts, err := builder.NewBuilder().BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	require.NoError(t, r.applyClusterTLSChecksum(context.Background(), cluster, sts))
	_, ok := sts.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]
	assert.False(t, ok, "no annotation while the secret has not been issued yet")
}

func TestPreserveTLSChecksumAnnotation_CarriedOverWhenUnstamped(t *testing.T) {
	existing := &appsv1.StatefulSet{}
	existing.Spec.Template.Annotations = map[string]string{
		util.AnnotationTLSCertChecksum: "prev-checksum",
	}
	desired := &appsv1.StatefulSet{} // built by a path that does not stamp

	preserveTLSChecksumAnnotation(existing, desired)
	assert.Equal(t, "prev-checksum",
		desired.Spec.Template.Annotations[util.AnnotationTLSCertChecksum],
		"scale/mirroring paths must not strip the annotation (no pod churn)")

	// A freshly stamped desired value always wins.
	desired2 := &appsv1.StatefulSet{}
	desired2.Spec.Template.Annotations = map[string]string{
		util.AnnotationTLSCertChecksum: "new-checksum",
	}
	preserveTLSChecksumAnnotation(existing, desired2)
	assert.Equal(t, "new-checksum",
		desired2.Spec.Template.Annotations[util.AnnotationTLSCertChecksum])
}

func TestReconcileCoordinator_StampsChecksumAndStaysStable(t *testing.T) {
	r, cluster := tlsChecksumFixture(t)
	require.NoError(t, r.client.Create(context.Background(), cluster))

	require.NoError(t, r.reconcileCoordinator(context.Background(), cluster))

	sts := &appsv1.StatefulSet{}
	require.NoError(t, r.client.Get(context.Background(), types.NamespacedName{
		Name: util.CoordinatorName(cluster.Name), Namespace: cluster.Namespace,
	}, sts))
	checksum := sts.Spec.Template.Annotations[util.AnnotationTLSCertChecksum]
	require.NotEmpty(t, checksum)
	rv := sts.ResourceVersion

	// No-op reconcile: same secret → no StatefulSet update (no rollout).
	require.NoError(t, r.reconcileCoordinator(context.Background(), cluster))
	require.NoError(t, r.client.Get(context.Background(), types.NamespacedName{
		Name: util.CoordinatorName(cluster.Name), Namespace: cluster.Namespace,
	}, sts))
	assert.Equal(t, rv, sts.ResourceVersion,
		"a no-op reconcile must not update the StatefulSet")
	assert.Equal(t, checksum, sts.Spec.Template.Annotations[util.AnnotationTLSCertChecksum])
}

// ----------------------------------------------------------------------------
// L-7: deterministic deletion-backup Job naming
// ----------------------------------------------------------------------------

func TestDeletionBackupTimestamp_DerivedFromDeletionTimestamp(t *testing.T) {
	cluster := newTestCluster()
	ts := metav1.NewTime(time.Date(2026, 3, 1, 10, 30, 0, 0, time.UTC))
	cluster.DeletionTimestamp = &ts

	got := deletionBackupTimestamp(cluster)
	assert.Equal(t, "20260301-103000", got)
	// Deterministic: repeated derivations yield the same value.
	assert.Equal(t, got, deletionBackupTimestamp(cluster))
}

func TestDeletionBackupTimestamp_NilFallsBackToNow(t *testing.T) {
	cluster := newTestCluster()
	got := deletionBackupTimestamp(cluster)
	assert.NotEmpty(t, got)
	_, err := time.Parse(util.BackupTimestampLayout, got)
	assert.NoError(t, err)
}

func TestStartDeletionBackup_IdempotentAcrossAnnotationPatchFailure(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete
	now := metav1.NewTime(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{util.FinalizerName}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	// First attempt creates the Job (the annotation patch outcome does not
	// matter for naming determinism).
	_, _, err := r.startDeletionBackup(context.Background(), cluster)
	require.NoError(t, err)

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	require.Len(t, jobs.Items, 1)
	firstName := jobs.Items[0].Name

	// Simulate a re-reconcile after a FAILED annotation patch: the tracking
	// annotation is absent, so startDeletionBackup runs again. The derived
	// Job name must be IDENTICAL and Create must tolerate AlreadyExists.
	cluster.Annotations = nil
	_, _, err = r.startDeletionBackup(context.Background(), cluster)
	require.NoError(t, err)

	require.NoError(t, k8sClient.List(context.Background(), jobs))
	require.Len(t, jobs.Items, 1, "no duplicate deletion-backup Job may be created")
	assert.Equal(t, firstName, jobs.Items[0].Name)
	assert.Contains(t, firstName, "20260302-090000",
		"the Job name must embed the deletionTimestamp-derived timestamp")
}

// Compile-time guard: the corev1 import is used by the fixtures above.
var _ = corev1.EventTypeWarning
