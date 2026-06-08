//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// Scenario44HBACustomRulesE2ESuite tests Scenario 44: pg_hba.conf custom rules end-to-end.
type Scenario44HBACustomRulesE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario44(t *testing.T) {
	suite.Run(t, new(Scenario44HBACustomRulesE2ESuite))
}

// scenario44Rules returns the four custom HBA rules from the scenario44 example.
func scenario44Rules() []cbv1alpha1.HBARule {
	return []cbv1alpha1.HBARule{
		{
			Type:     cbv1alpha1.HBATypeLocal,
			Database: "all",
			User:     "gpadmin",
			Method:   cbv1alpha1.AuthMethodTrust,
		},
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "10.0.0.0/8",
			Method:   cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type:     cbv1alpha1.HBATypeHostSSL,
			Database: "all",
			User:     "all",
			Address:  "192.168.0.0/16",
			Method:   cbv1alpha1.AuthMethodScramSHA256,
		},
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "0.0.0.0/0",
			Method:   cbv1alpha1.AuthMethodReject,
		},
	}
}

// reconcileAuthCustom creates an AuthReconciler with the given K8s env and
// reconciles the specified cluster, returning the pg_hba.conf content.
func (s *Scenario44HBACustomRulesE2ESuite) reconcileAuthCustom(
	env *testutil.TestK8sEnv,
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	s.T().Helper()

	reconciler := controller.NewAuthReconciler(
		env.Client, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	cm, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	content, ok := cm.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")
	return content
}

// countE2ERuleLines counts non-empty, non-comment lines in the pg_hba.conf content.
func countE2ERuleLines(content string) int {
	count := 0
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		count++
	}
	return count
}

// TestE2E_Scenario44_CustomRules_ConfigMap creates a cluster with custom rules
// and verifies the ConfigMap is created correctly.
func (s *Scenario44HBACustomRulesE2ESuite) TestE2E_Scenario44_CustomRules_ConfigMap() {
	s.logger.Info("starting scenario 44 E2E: custom rules ConfigMap creation")

	cluster := testutil.NewClusterBuilder("e2e-s44-cm", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(scenario44Rules()).
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuthCustom(env, cluster)

	// Verify ConfigMap was created with correct content.
	assert.Contains(s.T(), content, "local\tall\tgpadmin\ttrust")
	assert.Equal(s.T(), 4, countE2ERuleLines(content),
		"pg_hba.conf should contain exactly 4 custom rules")

	// Verify ConfigMap has hash annotation.
	cm, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), cm.Annotations[util.AnnotationConfigHash],
		"ConfigMap should have a config hash annotation")

	s.logger.Info("scenario 44 E2E: custom rules ConfigMap creation completed")
}

// TestE2E_Scenario44_CustomRules_AllRulesPresent verifies all 4 rules are in the ConfigMap.
func (s *Scenario44HBACustomRulesE2ESuite) TestE2E_Scenario44_CustomRules_AllRulesPresent() {
	s.logger.Info("starting scenario 44 E2E: all custom rules present")

	cluster := testutil.NewClusterBuilder("e2e-s44-all-rules", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(scenario44Rules()).
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuthCustom(env, cluster)

	expectedLines := []string{
		"local\tall\tgpadmin\ttrust",
		"host\tall\tall\t10.0.0.0/8\tscram-sha-256",
		"hostssl\tall\tall\t192.168.0.0/16\tscram-sha-256",
		"host\tall\tall\t0.0.0.0/0\treject",
	}

	for _, expected := range expectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain custom rule: %s", expected)
	}

	// Verify default-only rules are NOT present.
	assert.NotContains(s.T(), content, "local\tall\tall\tscram-sha-256",
		"default local all scram rule should NOT be present")
	assert.NotContains(s.T(), content, "host\tall\tgpadmin\t127.0.0.1/32\ttrust",
		"default host gpadmin localhost rule should NOT be present")
	assert.NotContains(s.T(), content, "host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
		"default replication rule should NOT be present")

	s.logger.Info("scenario 44 E2E: all custom rules present completed")
}

// TestE2E_Scenario44_UpdateRules updates rules and verifies ConfigMap is updated.
func (s *Scenario44HBACustomRulesE2ESuite) TestE2E_Scenario44_UpdateRules() {
	s.logger.Info("starting scenario 44 E2E: update rules")

	cluster := testutil.NewClusterBuilder("e2e-s44-update", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(scenario44Rules()).
		Build()

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		env.Client, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// First reconcile: creates the ConfigMap.
	_, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	cmBefore, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)
	hashBefore := cmBefore.Annotations[util.AnnotationConfigHash]

	// Re-fetch the cluster to get the latest resource version after reconcile.
	cluster, err = env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Update the cluster with new HBA rules.
	if cluster.Spec.Auth == nil {
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	cluster.Spec.Auth.HBARules = []cbv1alpha1.HBARule{
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "172.16.0.0/12",
			Method:   cbv1alpha1.AuthMethodMD5,
		},
		{
			Type:     cbv1alpha1.HBATypeHost,
			Database: "all",
			User:     "all",
			Address:  "0.0.0.0/0",
			Method:   cbv1alpha1.AuthMethodReject,
		},
	}
	err = env.Client.Update(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Second reconcile: updates the ConfigMap.
	_, err = reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	cmAfter, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	content, ok := cmAfter.Data["pg_hba.conf"]
	require.True(s.T(), ok)

	// Verify new rules are present.
	assert.Contains(s.T(), content, "host\tall\tall\t172.16.0.0/12\tmd5",
		"updated rule must be present")
	assert.Contains(s.T(), content, "host\tall\tall\t0.0.0.0/0\treject",
		"reject rule must be present")

	// Verify old rules are gone.
	assert.NotContains(s.T(), content, "10.0.0.0/8",
		"old rule must NOT be present after update")
	assert.NotContains(s.T(), content, "192.168.0.0/16",
		"old hostssl rule must NOT be present after update")

	// Verify hash changed.
	hashAfter := cmAfter.Annotations[util.AnnotationConfigHash]
	assert.NotEqual(s.T(), hashBefore, hashAfter,
		"config hash must change after HBA rules update")

	assert.Equal(s.T(), 2, countE2ERuleLines(content),
		"pg_hba.conf should contain exactly 2 rules after update")

	s.logger.Info("scenario 44 E2E: update rules completed")
}

// TestE2E_Scenario44_ClusterCRAccepted verifies the cluster CR with custom HBA
// rules is accepted by the reconciler.
func (s *Scenario44HBACustomRulesE2ESuite) TestE2E_Scenario44_ClusterCRAccepted() {
	s.logger.Info("starting scenario 44 E2E: cluster CR accepted")

	cluster := testutil.NewClusterBuilder("e2e-s44-accepted", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(scenario44Rules()).
		Build()

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		env.Client, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err, "reconcile should succeed for cluster with custom HBA rules")
	assert.NotZero(s.T(), result.RequeueAfter,
		"reconcile should return a requeue interval")

	// Verify the cluster can be retrieved.
	retrieved, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cluster.Name, retrieved.Name)

	// Verify the HBA rules are preserved in the spec.
	require.NotNil(s.T(), retrieved.Spec.Auth)
	assert.Len(s.T(), retrieved.Spec.Auth.HBARules, 4,
		"cluster spec should have 4 HBA rules")

	s.logger.Info("scenario 44 E2E: cluster CR accepted completed")
}

// TestE2E_Scenario44_HBACustomRuleCases runs the cases catalog.
func (s *Scenario44HBACustomRulesE2ESuite) TestE2E_Scenario44_HBACustomRuleCases() {
	s.logger.Info("starting scenario 44 E2E: HBA custom rule cases catalog")

	for _, tc := range cases.HBACustomRuleCases() {
		s.Run(tc.Name, func() {
			cluster := testutil.NewClusterBuilder("e2e-s44-"+tc.Name, s.namespace).
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				WithPendingGeneration().
				WithHBARules(tc.Rules).
				Build()

			env := testutil.NewTestK8sEnv(cluster)

			reconciler := controller.NewAuthReconciler(
				env.Client, env.Recorder,
				env.Builder, env.Metrics, env.Logger,
			)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      cluster.Name,
					Namespace: cluster.Namespace,
				},
			}

			result, err := reconciler.Reconcile(s.ctx, req)
			require.NoError(s.T(), err,
				"reconcile should succeed for case: %s", tc.Description)
			assert.NotZero(s.T(), result.RequeueAfter)

			cm, err := env.GetConfigMap(
				s.ctx,
				util.PgHBAConfConfigMapName(cluster.Name),
				cluster.Namespace,
			)
			require.NoError(s.T(), err)

			content, ok := cm.Data["pg_hba.conf"]
			require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")

			// Verify expected lines are present.
			for _, expected := range tc.ExpectedLines {
				assert.Contains(s.T(), content, expected,
					"pg_hba.conf should contain line: %s (case: %s)",
					expected, tc.Description)
			}

			// Verify expected rule count.
			if tc.ExpectedCount > 0 {
				assert.Equal(s.T(), tc.ExpectedCount, countE2ERuleLines(content),
					"pg_hba.conf should contain exactly %d rules (case: %s)",
					tc.ExpectedCount, tc.Description)
			}

			// Verify hash annotation presence.
			if tc.HasHashAnnotation {
				hashValue, hasHash := cm.Annotations[util.AnnotationConfigHash]
				assert.True(s.T(), hasHash,
					"ConfigMap must have %s annotation (case: %s)",
					util.AnnotationConfigHash, tc.Description)
				assert.NotEmpty(s.T(), hashValue,
					"config hash annotation must not be empty (case: %s)",
					tc.Description)
			}
		})
	}

	s.logger.Info("scenario 44 E2E: HBA custom rule cases catalog completed")
}

// Ensure the builder and metrics packages are used (compile-time check).
var (
	_ builder.ResourceBuilder = (*builder.DefaultBuilder)(nil)
	_ metrics.Recorder        = (*metrics.NoopRecorder)(nil)
)
