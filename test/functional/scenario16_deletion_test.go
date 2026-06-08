//go:build functional

package functional

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario16ClusterName = "scenario16-cluster"
	scenario16Namespace   = "cloudberry-test"
	scenario16SegCount    = int32(2)
)

// Scenario16DeletionSuite tests cluster deletion with both Retain and Delete policies,
// including backup-on-delete support.
type Scenario16DeletionSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario16_Deletion(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario16DeletionSuite))
}

func (s *Scenario16DeletionSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario16DeletionSuite) TearDownTest() {
	s.cancel()
}

// scenario16Req creates a reconcile request for the scenario 16 cluster.
func scenario16Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario16ClusterName,
			Namespace: scenario16Namespace,
		},
	}
}

// newScenario16Reconciler creates a ClusterReconciler for scenario 16 tests.
func newScenario16Reconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	logger *slog.Logger,
) *controller.ClusterReconciler {
	return controller.NewClusterReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, env.Metrics, logger,
	)
}

// createPVCsForCluster creates PVCs with cluster labels for testing deletion policies.
func createPVCsForCluster(
	ctx context.Context,
	t *testing.T,
	env *testutil.TestK8sEnv,
	clusterName, namespace string,
	count int,
) {
	t.Helper()
	for i := 0; i < count; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName(clusterName, i),
				Namespace: namespace,
				Labels: map[string]string{
					util.LabelCluster: clusterName,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		}
		err := env.Client.Create(ctx, pvc)
		require.NoError(t, err, "creating PVC %s should succeed", pvc.Name)
	}
}

// pvcName generates a PVC name for testing.
func pvcName(clusterName string, index int) string {
	return util.SanitizeK8sName(fmt.Sprintf("%s-data-%d", clusterName, index))
}

// setDeletionTimestamp simulates kubectl delete by setting the deletion timestamp.
// Since the fake client does not support setting DeletionTimestamp directly,
// we create the cluster with a finalizer and then issue a Delete call which
// sets the DeletionTimestamp while the finalizer prevents actual removal.
func setDeletionTimestamp(
	ctx context.Context,
	t *testing.T,
	env *testutil.TestK8sEnv,
	clusterName, namespace string,
) {
	t.Helper()
	cluster := &cbv1alpha1.CloudberryCluster{}
	err := env.Client.Get(ctx, types.NamespacedName{
		Name:      clusterName,
		Namespace: namespace,
	}, cluster)
	require.NoError(t, err, "getting cluster for deletion should succeed")

	err = env.Client.Delete(ctx, cluster)
	require.NoError(t, err, "deleting cluster should succeed")
}

// collectScenario16Events reads all pending events from the FakeRecorder channel.
func collectScenario16Events(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// hasEvent checks if any event contains the given substring.
func hasEvent(events []string, substr string) bool {
	for _, event := range events {
		if strings.Contains(event, substr) {
			return true
		}
	}
	return false
}

// --- Test: Delete with Retain Policy and BackupOnDelete ---

func (s *Scenario16DeletionSuite) TestScenario16a_DeleteWithRetainPolicy() {
	t := s.T()

	// Arrange: create a Running cluster with Retain policy and backupOnDelete=true.
	cluster := testutil.NewClusterBuilder(scenario16ClusterName, scenario16Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario16SegCount).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithBackupOnDelete(true).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation since the fake client's status subresource
	// does not persist status fields set during object creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario16SegCount
	current.Status.SegmentsTotal = scenario16SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(t, err)

	// Create PVCs with cluster labels.
	createPVCsForCluster(s.ctx, t, s.env, cluster.Name, cluster.Namespace, 3)

	// Simulate kubectl delete (sets DeletionTimestamp via finalizer).
	setDeletionTimestamp(s.ctx, t, s.env, cluster.Name, cluster.Namespace)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario16Reconciler(s.env, fakeRecorder, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario16Req())
	require.NoError(t, err, "deletion reconciliation should succeed")

	// After deletion completes (finalizer removed), the fake client deletes
	// the object because DeletionTimestamp was set. Verify the cluster is gone,
	// which confirms the finalizer was removed successfully.
	_, getErr := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	assert.Error(t, getErr,
		"cluster should be deleted after finalizer removal")

	// Verify: PVCs still exist (Retain policy).
	pvcList := &corev1.PersistentVolumeClaimList{}
	err = s.env.Client.List(s.ctx, pvcList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, len(pvcList.Items),
		"PVCs should still exist with Retain policy")

	// Verify events.
	events := collectScenario16Events(fakeRecorder)
	assert.True(t, hasEvent(events, "Deleting"),
		"Deleting event should be emitted; events: %v", events)
	assert.True(t, hasEvent(events, "BackupOnDelete"),
		"BackupOnDelete event should be emitted; events: %v", events)
	assert.True(t, hasEvent(events, "PVCsRetained"),
		"PVCsRetained event should be emitted; events: %v", events)
	assert.True(t, hasEvent(events, "Deleted"),
		"Deleted event should be emitted; events: %v", events)
	assert.False(t, hasEvent(events, "PVCsDeleted"),
		"PVCsDeleted event should NOT be emitted with Retain policy; events: %v", events)
}

// --- Test: Delete with Delete Policy and no BackupOnDelete ---

func (s *Scenario16DeletionSuite) TestScenario16b_DeleteWithDeletePolicy() {
	t := s.T()

	// Arrange: create a Running cluster with Delete policy and backupOnDelete=false.
	cluster := testutil.NewClusterBuilder(scenario16ClusterName, scenario16Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario16SegCount).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyDelete).
		WithBackupOnDelete(false).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario16SegCount
	current.Status.SegmentsTotal = scenario16SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(t, err)

	// Create PVCs with cluster labels.
	createPVCsForCluster(s.ctx, t, s.env, cluster.Name, cluster.Namespace, 3)

	// Simulate kubectl delete.
	setDeletionTimestamp(s.ctx, t, s.env, cluster.Name, cluster.Namespace)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario16Reconciler(s.env, fakeRecorder, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario16Req())
	require.NoError(t, err, "deletion reconciliation should succeed")

	// After deletion completes (finalizer removed), the fake client deletes
	// the object because DeletionTimestamp was set. Verify the cluster is gone.
	_, getErr := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	assert.Error(t, getErr,
		"cluster should be deleted after finalizer removal")

	// Verify: PVCs deleted (Delete policy).
	pvcList := &corev1.PersistentVolumeClaimList{}
	err = s.env.Client.List(s.ctx, pvcList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, len(pvcList.Items),
		"PVCs should be deleted with Delete policy")

	// Verify events.
	events := collectScenario16Events(fakeRecorder)
	assert.True(t, hasEvent(events, "Deleting"),
		"Deleting event should be emitted; events: %v", events)
	assert.True(t, hasEvent(events, "PVCsDeleted"),
		"PVCsDeleted event should be emitted; events: %v", events)
	assert.True(t, hasEvent(events, "Deleted"),
		"Deleted event should be emitted; events: %v", events)
	assert.False(t, hasEvent(events, "BackupOnDelete"),
		"BackupOnDelete event should NOT be emitted when backupOnDelete=false; events: %v", events)
	assert.False(t, hasEvent(events, "PVCsRetained"),
		"PVCsRetained event should NOT be emitted with Delete policy; events: %v", events)
}

// --- Test: No Finalizer Skips Deletion ---

func (s *Scenario16DeletionSuite) TestScenario16_NoFinalizerSkipsDeletion() {
	t := s.T()

	// Arrange: create a cluster WITHOUT finalizer, with deletion timestamp.
	cluster := testutil.NewClusterBuilder(scenario16ClusterName, scenario16Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario16SegCount).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	// Note: no WithFinalizer() — cluster has no finalizer.

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(t, err)

	// Simulate kubectl delete — since there's no finalizer, the fake client
	// will actually delete the object. We need to verify the reconciler
	// handles the "not found" case gracefully.
	err = s.env.Client.Delete(s.ctx, current)
	require.NoError(t, err)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario16Reconciler(s.env, fakeRecorder, s.logger)

	// Act: reconcile — cluster is gone, should return immediately.
	result, err := reconciler.Reconcile(s.ctx, scenario16Req())
	require.NoError(t, err, "reconciliation should succeed when cluster is not found")
	assert.False(t, result.Requeue, "should not requeue when cluster is not found")

	// Verify: no events emitted.
	events := collectScenario16Events(fakeRecorder)
	assert.Empty(t, events,
		"no events should be emitted when cluster is not found; events: %v", events)
}

// --- Test: Backup Job Created ---

func (s *Scenario16DeletionSuite) TestScenario16_BackupJobCreated() {
	t := s.T()

	// Arrange: create a cluster with backupOnDelete=true and finalizer.
	cluster := testutil.NewClusterBuilder(scenario16ClusterName, scenario16Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario16SegCount).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithBackupOnDelete(true).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(t, err)

	// Simulate kubectl delete.
	setDeletionTimestamp(s.ctx, t, s.env, cluster.Name, cluster.Namespace)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario16Reconciler(s.env, fakeRecorder, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario16Req())
	require.NoError(t, err, "deletion reconciliation should succeed")

	// Verify: a Job with "backup-on-delete" in the name is created.
	jobList := &batchv1.JobList{}
	err = s.env.Client.List(s.ctx, jobList,
		client.InNamespace(cluster.Namespace),
	)
	require.NoError(t, err)

	backupJobFound := false
	for _, job := range jobList.Items {
		if strings.Contains(job.Name, "backup-on-delete") {
			backupJobFound = true
			// Verify the job has the correct labels.
			assert.Equal(t, cluster.Name, job.Labels[util.LabelCluster],
				"backup job should have cluster label")
			assert.Equal(t, util.MaintenanceBackupOnDelete, job.Labels[util.LabelOperation],
				"backup job should have operation label")
			break
		}
	}
	assert.True(t, backupJobFound,
		"a Job with 'backup-on-delete' in the name should be created; jobs: %v",
		jobNames(jobList))
}

// --- Test: Deletion Phase Transition ---

func (s *Scenario16DeletionSuite) TestScenario16_DeletionPhaseTransition() {
	t := s.T()

	// Arrange: create a cluster in Running phase with finalizer.
	cluster := testutil.NewClusterBuilder(scenario16ClusterName, scenario16Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario16SegCount).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status to Running.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario16SegCount
	current.Status.SegmentsTotal = scenario16SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(t, err)

	// Verify initial phase is Running.
	before, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(t, err)
	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, before.Status.Phase,
		"initial phase should be Running")

	// Simulate kubectl delete.
	setDeletionTimestamp(s.ctx, t, s.env, cluster.Name, cluster.Namespace)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario16Reconciler(s.env, fakeRecorder, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario16Req())
	require.NoError(t, err, "deletion reconciliation should succeed")

	// After deletion completes (finalizer removed), the fake client deletes
	// the object. The phase transition to Deleting happened during reconciliation
	// (verified by the Deleting event). The cluster is now gone because the
	// finalizer was removed and DeletionTimestamp was set.
	_, getErr := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	assert.Error(t, getErr,
		"cluster should be deleted after finalizer removal, confirming phase transition completed")

	// Verify the Deleting event was emitted, confirming the phase transition occurred.
	events := collectScenario16Events(fakeRecorder)
	assert.True(t, hasEvent(events, "Deleting"),
		"Deleting event should be emitted, confirming Running -> Deleting transition; events: %v", events)
	assert.True(t, hasEvent(events, "Deleted"),
		"Deleted event should be emitted, confirming deletion completed; events: %v", events)
}
