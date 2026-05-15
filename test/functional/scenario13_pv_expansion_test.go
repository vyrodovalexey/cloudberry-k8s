//go:build functional

package functional

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario13ClusterName = "scenario13-cluster"
	scenario13Namespace   = "cloudberry-test"
	scenario13InitSize    = "5Gi"
	scenario13ExpandSize  = "10Gi"
	scenario13ShrinkSize  = "3Gi"
	scenario13SegCount    = int32(3)
)

// Scenario13PVExpansionSuite tests PV expansion across coordinator, standby, and segments.
type Scenario13PVExpansionSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario13_PVExpansion(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario13PVExpansionSuite))
}

func (s *Scenario13PVExpansionSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario13PVExpansionSuite) TearDownTest() {
	s.cancel()
}

// newScenario13Reconciler creates a ClusterReconciler for scenario 13 tests.
func newScenario13Reconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	metricsRec metrics.Recorder,
	logger *slog.Logger,
) *controller.ClusterReconciler {
	return controller.NewClusterReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, metricsRec, logger,
	)
}

// createPVC creates a PVC with the given name, namespace, size, and labels.
func (s *Scenario13PVExpansionSuite) createPVC(
	name, namespace, size string, labels map[string]string,
) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
	err := s.env.Client.Create(s.ctx, pvc)
	require.NoError(s.T(), err, "creating PVC %s should succeed", name)
}

// getPVCSize returns the storage size of a PVC.
func (s *Scenario13PVExpansionSuite) getPVCSize(name, namespace string) string {
	pvc := &corev1.PersistentVolumeClaim{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, pvc)
	if err != nil {
		return ""
	}
	qty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	return qty.String()
}

// --- Test: Coordinator Storage Expansion ---

func (s *Scenario13PVExpansionSuite) TestScenario13a_CoordinatorStorageExpansion() {
	// Arrange: create a Running cluster with coordinator storage 5Gi.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13InitSize).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create coordinator PVC at 5Gi.
	coordPVCName := fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name))
	s.createPVC(coordPVCName, scenario13Namespace, scenario13InitSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentCoordinator,
	})

	// Create segment PVCs at 5Gi (should remain unchanged).
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		s.createPVC(primaryPVC, scenario13Namespace, scenario13InitSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentPrimary,
		})
	}

	// Act: patch coordinator.storage.size to 10Gi.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Coordinator.Storage.Size = scenario13ExpandSize
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Run reconciliation via the reconciler.
	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	_, err = reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify coordinator PVC expanded to 10Gi.
	assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(coordPVCName, scenario13Namespace),
		"coordinator PVC should be expanded to 10Gi")

	// Verify StorageExpanded condition set.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	storageCond := util.FindCondition(updated.Status.Conditions, "StorageExpanded")
	require.NotNil(s.T(), storageCond, "StorageExpanded condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, storageCond.Status)
	assert.Equal(s.T(), "PVCsExpanded", storageCond.Reason)

	// Verify StorageExpanded event.
	events := collectEvents(fakeRecorder)
	storageExpandedFound := false
	for _, event := range events {
		if containsSubstring(event, "StorageExpanded") {
			storageExpandedFound = true
			break
		}
	}
	assert.True(s.T(), storageExpandedFound,
		"StorageExpanded event should be emitted; events: %v", events)

	// Verify segment PVCs unchanged at 5Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(primaryPVC, scenario13Namespace),
			"segment PVC %s should remain at 5Gi", primaryPVC)
	}
}

// --- Test: Standby Storage Expansion ---

func (s *Scenario13PVExpansionSuite) TestScenario13b_StandbyStorageExpansion() {
	// Arrange: create a cluster with standby enabled, storage 5Gi.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13InitSize).
		WithStandbyStorage(scenario13InitSize).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create coordinator PVC at 5Gi.
	coordPVCName := fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name))
	s.createPVC(coordPVCName, scenario13Namespace, scenario13InitSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentCoordinator,
	})

	// Create standby PVC at 5Gi.
	standbyPVCName := fmt.Sprintf("data-%s-0", util.StandbyName(cluster.Name))
	s.createPVC(standbyPVCName, scenario13Namespace, scenario13InitSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentStandby,
	})

	// Create segment PVCs at 5Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		s.createPVC(primaryPVC, scenario13Namespace, scenario13InitSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentPrimary,
		})
	}

	// Act: patch standby.storage.size to 10Gi.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Standby.Storage.Size = scenario13ExpandSize
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	_, err = reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify only standby PVC expanded to 10Gi.
	assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(standbyPVCName, scenario13Namespace),
		"standby PVC should be expanded to 10Gi")

	// Verify coordinator PVC unchanged at 5Gi.
	assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(coordPVCName, scenario13Namespace),
		"coordinator PVC should remain at 5Gi")

	// Verify segment PVCs unchanged at 5Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(primaryPVC, scenario13Namespace),
			"segment PVC %s should remain at 5Gi", primaryPVC)
	}
}

// --- Test: Segment Storage Expansion (3 primaries + 3 mirrors) ---

func (s *Scenario13PVExpansionSuite) TestScenario13c_SegmentStorageExpansion() {
	// Arrange: create a cluster with 3 segments + mirroring, storage 5Gi.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13InitSize).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create coordinator PVC at 5Gi.
	coordPVCName := fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name))
	s.createPVC(coordPVCName, scenario13Namespace, scenario13InitSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentCoordinator,
	})

	// Create 3 primary PVCs + 3 mirror PVCs at 5Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		s.createPVC(primaryPVC, scenario13Namespace, scenario13InitSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentPrimary,
		})
		mirrorPVC := fmt.Sprintf("data-%s-%d", util.SegmentMirrorName(cluster.Name), i)
		s.createPVC(mirrorPVC, scenario13Namespace, scenario13InitSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentMirror,
		})
	}

	// Act: patch segments.storage.size to 10Gi.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Storage.Size = scenario13ExpandSize
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	_, err = reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify ALL 6 PVCs (3 primary + 3 mirror) expanded to 10Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(primaryPVC, scenario13Namespace),
			"primary PVC %s should be expanded to 10Gi", primaryPVC)

		mirrorPVC := fmt.Sprintf("data-%s-%d", util.SegmentMirrorName(cluster.Name), i)
		assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(mirrorPVC, scenario13Namespace),
			"mirror PVC %s should be expanded to 10Gi", mirrorPVC)
	}

	// Verify coordinator PVC unchanged at 5Gi.
	assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(coordPVCName, scenario13Namespace),
		"coordinator PVC should remain at 5Gi")

	// Verify StorageExpanded event.
	events := collectEvents(fakeRecorder)
	storageExpandedFound := false
	for _, event := range events {
		if containsSubstring(event, "StorageExpanded") {
			storageExpandedFound = true
			break
		}
	}
	assert.True(s.T(), storageExpandedFound,
		"StorageExpanded event should be emitted; events: %v", events)
}

// --- Test: No Expansion When Size Unchanged ---

func (s *Scenario13PVExpansionSuite) TestScenario13_NoExpansionWhenSizeUnchanged() {
	// Arrange: create a cluster with 5Gi storage.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13InitSize).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create coordinator PVC at 5Gi.
	coordPVCName := fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name))
	s.createPVC(coordPVCName, scenario13Namespace, scenario13InitSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentCoordinator,
	})

	// Create segment PVCs at 5Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		s.createPVC(primaryPVC, scenario13Namespace, scenario13InitSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentPrimary,
		})
	}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile without changing storage sizes.
	_, err := reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify no PVCs modified.
	assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(coordPVCName, scenario13Namespace),
		"coordinator PVC should remain at 5Gi")
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		assert.Equal(s.T(), scenario13InitSize, s.getPVCSize(primaryPVC, scenario13Namespace),
			"segment PVC %s should remain at 5Gi", primaryPVC)
	}

	// Verify no StorageExpanded event.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "StorageExpanded"),
			"StorageExpanded event should NOT be emitted when size is unchanged; events: %v", events)
	}
}

// --- Test: No Shrink Allowed ---

func (s *Scenario13PVExpansionSuite) TestScenario13_NoShrinkAllowed() {
	// Arrange: create a cluster with 10Gi storage.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13ExpandSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13ExpandSize).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create coordinator PVC at 10Gi.
	coordPVCName := fmt.Sprintf("data-%s-0", util.CoordinatorName(cluster.Name))
	s.createPVC(coordPVCName, scenario13Namespace, scenario13ExpandSize, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentCoordinator,
	})

	// Create segment PVCs at 10Gi.
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		s.createPVC(primaryPVC, scenario13Namespace, scenario13ExpandSize, map[string]string{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: util.ComponentSegmentPrimary,
		})
	}

	// Act: patch to 3Gi (shrink).
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Coordinator.Storage.Size = scenario13ShrinkSize
	current.Spec.Segments.Storage.Size = scenario13ShrinkSize
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	_, err = reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify PVCs NOT modified (shrink not supported).
	assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(coordPVCName, scenario13Namespace),
		"coordinator PVC should remain at 10Gi (shrink not supported)")
	for i := int32(0); i < scenario13SegCount; i++ {
		primaryPVC := fmt.Sprintf("data-%s-%d", util.SegmentPrimaryName(cluster.Name), i)
		assert.Equal(s.T(), scenario13ExpandSize, s.getPVCSize(primaryPVC, scenario13Namespace),
			"segment PVC %s should remain at 10Gi (shrink not supported)", primaryPVC)
	}

	// Verify no StorageExpanded event.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "StorageExpanded"),
			"StorageExpanded event should NOT be emitted for shrink; events: %v", events)
	}
}

// --- Test: PVC Not Found Skipped ---

func (s *Scenario13PVExpansionSuite) TestScenario13_PVCNotFoundSkipped() {
	// Arrange: create a cluster but don't create PVCs.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13ExpandSize).
		WithSegments(scenario13SegCount).
		WithSegmentStorage(scenario13ExpandSize).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile — should not error even though PVCs don't exist.
	_, err := reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed even when PVCs don't exist")

	// Verify no StorageExpanded event (nothing to expand).
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "StorageExpanded"),
			"StorageExpanded event should NOT be emitted when PVCs don't exist; events: %v", events)
	}
}

// TestScenario13_BlockedByStorageClass verifies that PVC expansion is blocked
// when the StorageClass has allowVolumeExpansion=false, and that a warning is
// logged instead of an error.
func (s *Scenario13PVExpansionSuite) TestScenario13_BlockedByStorageClass() {
	s.env = testutil.NewTestK8sEnv()

	// Create a StorageClass with allowVolumeExpansion=false.
	expansionFalse := false
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "no-expand-sc"},
		Provisioner:          "kubernetes.io/no-provisioner",
		AllowVolumeExpansion: &expansionFalse,
	}
	require.NoError(s.T(), s.env.Client.Create(s.ctx, sc))

	// Create cluster with coordinator storage 5Gi, already initializing.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(1).
		WithFinalizer().
		WithPendingGeneration().
		Build()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	require.NoError(s.T(), s.env.Client.Create(s.ctx, cluster))

	// Create coordinator PVC at 5Gi referencing the non-expandable StorageClass.
	scName := "no-expand-sc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.CoordinatorName(scenario13ClusterName)),
			Namespace: scenario13Namespace,
			Labels:    util.CommonLabels(scenario13ClusterName, util.ComponentCoordinator),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(scenario13InitSize),
				},
			},
		},
	}
	require.NoError(s.T(), s.env.Client.Create(s.ctx, pvc))

	// Patch cluster to request larger storage.
	cluster.Spec.Coordinator.Storage.Size = scenario13ExpandSize
	cluster.Generation = 2
	require.NoError(s.T(), s.env.Client.Update(s.ctx, cluster))

	// Reconcile.
	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, &metrics.NoopRecorder{}, s.logger)
	_, err := reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err, "reconciliation should succeed even when expansion is blocked")

	// Verify PVC was NOT expanded (still 5Gi).
	updatedPVC := &corev1.PersistentVolumeClaim{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name: pvc.Name, Namespace: scenario13Namespace,
	}, updatedPVC))
	currentSize := updatedPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(s.T(), scenario13InitSize, currentSize.String(),
		"PVC should NOT be expanded when StorageClass doesn't support it")

	// Verify no StorageExpanded event.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "StorageExpanded"),
			"StorageExpanded event should NOT be emitted when blocked by StorageClass")
	}
}

// TestScenario13_AllowedByStorageClass verifies that PVC expansion proceeds
// when the StorageClass has allowVolumeExpansion=true.
func (s *Scenario13PVExpansionSuite) TestScenario13_AllowedByStorageClass() {
	s.env = testutil.NewTestK8sEnv()

	// Create a StorageClass with allowVolumeExpansion=true.
	expansionTrue := true
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "expand-sc"},
		Provisioner:          "kubernetes.io/no-provisioner",
		AllowVolumeExpansion: &expansionTrue,
	}
	require.NoError(s.T(), s.env.Client.Create(s.ctx, sc))

	// Create cluster with coordinator storage 5Gi, already in Running state.
	cluster := testutil.NewClusterBuilder(scenario13ClusterName, scenario13Namespace).
		WithCoordinatorStorage(scenario13InitSize).
		WithSegments(1).
		WithFinalizer().
		WithPendingGeneration().
		Build()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	require.NoError(s.T(), s.env.Client.Create(s.ctx, cluster))

	// Create coordinator PVC at 5Gi referencing the expandable StorageClass.
	scName := "expand-sc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("data-%s-0", util.CoordinatorName(scenario13ClusterName)),
			Namespace: scenario13Namespace,
			Labels:    util.CommonLabels(scenario13ClusterName, util.ComponentCoordinator),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(scenario13InitSize),
				},
			},
		},
	}
	require.NoError(s.T(), s.env.Client.Create(s.ctx, pvc))

	// Patch cluster to request larger storage.
	cluster.Spec.Coordinator.Storage.Size = scenario13ExpandSize
	cluster.Generation = 2
	require.NoError(s.T(), s.env.Client.Update(s.ctx, cluster))

	// Reconcile.
	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario13Reconciler(s.env, fakeRecorder, &metrics.NoopRecorder{}, s.logger)
	_, err := reconciler.Reconcile(s.ctx, scenario13Req())
	require.NoError(s.T(), err)

	// Verify PVC WAS expanded to 10Gi.
	updatedPVC := &corev1.PersistentVolumeClaim{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name: pvc.Name, Namespace: scenario13Namespace,
	}, updatedPVC))
	currentSize := updatedPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(s.T(), scenario13ExpandSize, currentSize.String(),
		"PVC should be expanded when StorageClass supports it")

	// Verify StorageExpanded event.
	events := collectEvents(fakeRecorder)
	hasExpanded := false
	for _, event := range events {
		if containsSubstring(event, "StorageExpanded") {
			hasExpanded = true
			break
		}
	}
	assert.True(s.T(), hasExpanded, "StorageExpanded event should be emitted")
}

// scenario13Req creates a reconcile request for the scenario 13 cluster.
func scenario13Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario13ClusterName,
			Namespace: scenario13Namespace,
		},
	}
}
