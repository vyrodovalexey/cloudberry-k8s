//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// scenario44CustomRules returns the four custom HBA rules from the scenario44 example.
func scenario44CustomRules() []cbv1alpha1.HBARule {
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

// HBACustomRulesSuite tests Scenario 44: pg_hba.conf custom rules.
type HBACustomRulesSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario44(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(HBACustomRulesSuite))
}

func (s *HBACustomRulesSuite) SetupTest() {
	s.ctx = context.Background()
}

// reqFor builds a reconcile request for the given cluster.
func (s *HBACustomRulesSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// buildCustomRulesCluster creates a running cluster with the scenario44 custom HBA rules.
func buildCustomRulesCluster(name string) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(scenario44CustomRules()).
		Build()
}

// reconcileAndGetHBACustom is a helper that creates a cluster, reconciles auth,
// and returns the pg_hba.conf content from the generated ConfigMap.
func (s *HBACustomRulesSuite) reconcileAndGetHBACustom(
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	s.T().Helper()

	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	cm, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	content, ok := cm.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")
	return content
}

// countRuleLines counts non-empty, non-comment lines in the pg_hba.conf content.
func countRuleLines(content string) int {
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

// TestFunctional_Scenario44_CustomRules_ConfigMapCreated deploys a cluster with
// 4 custom HBA rules and verifies the ConfigMap is created with all 4 rules.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_ConfigMapCreated() {
	cluster := buildCustomRulesCluster("s44-cm-created")
	content := s.reconcileAndGetHBACustom(cluster)

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

	assert.Equal(s.T(), 4, countRuleLines(content),
		"pg_hba.conf should contain exactly 4 custom rules")
}

// TestFunctional_Scenario44_CustomRules_RuleOrder verifies that rules appear
// in the same order as specified in the CRD.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_RuleOrder() {
	cluster := buildCustomRulesCluster("s44-rule-order")
	content := s.reconcileAndGetHBACustom(cluster)

	// Verify ordering: local trust < host scram 10.0 < hostssl scram 192.168 < host reject 0.0.
	localIdx := strings.Index(content, "local\tall\tgpadmin\ttrust")
	hostScramIdx := strings.Index(content, "host\tall\tall\t10.0.0.0/8\tscram-sha-256")
	hostsslIdx := strings.Index(content, "hostssl\tall\tall\t192.168.0.0/16\tscram-sha-256")
	hostRejectIdx := strings.Index(content, "host\tall\tall\t0.0.0.0/0\treject")

	require.NotEqual(s.T(), -1, localIdx, "local trust rule must be present")
	require.NotEqual(s.T(), -1, hostScramIdx, "host scram rule must be present")
	require.NotEqual(s.T(), -1, hostsslIdx, "hostssl scram rule must be present")
	require.NotEqual(s.T(), -1, hostRejectIdx, "host reject rule must be present")

	assert.Less(s.T(), localIdx, hostScramIdx,
		"local trust must appear before host scram")
	assert.Less(s.T(), hostScramIdx, hostsslIdx,
		"host scram must appear before hostssl scram")
	assert.Less(s.T(), hostsslIdx, hostRejectIdx,
		"hostssl scram must appear before host reject")
}

// TestFunctional_Scenario44_CustomRules_HashAnnotation verifies the ConfigMap
// has the avsoft.io/config-hash annotation.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_HashAnnotation() {
	cluster := buildCustomRulesCluster("s44-hash-ann")

	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cm, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	hashValue, ok := cm.Annotations[util.AnnotationConfigHash]
	assert.True(s.T(), ok, "ConfigMap must have %s annotation", util.AnnotationConfigHash)
	assert.NotEmpty(s.T(), hashValue, "config hash annotation must not be empty")
}

// TestFunctional_Scenario44_CustomRules_NoDefaults verifies that default rules
// are NOT present when custom rules are specified.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_NoDefaults() {
	cluster := buildCustomRulesCluster("s44-no-defaults")
	content := s.reconcileAndGetHBACustom(cluster)

	// Default rules that should NOT be present.
	defaultOnlyLines := []string{
		"local\tall\tall\tscram-sha-256",
		"host\tall\tgpadmin\t127.0.0.1/32\ttrust",
		"host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
	}

	for _, excluded := range defaultOnlyLines {
		assert.NotContains(s.T(), content, excluded,
			"pg_hba.conf should NOT contain default rule when custom rules are set: %s", excluded)
	}
}

// TestFunctional_Scenario44_CustomRules_LocalTrust verifies the
// "local all gpadmin trust" rule is present.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_LocalTrust() {
	cluster := buildCustomRulesCluster("s44-local-trust")
	content := s.reconcileAndGetHBACustom(cluster)

	assert.Contains(s.T(), content, "local\tall\tgpadmin\ttrust",
		"local all gpadmin trust rule must be present")
}

// TestFunctional_Scenario44_CustomRules_HostScram verifies the
// "host all all 10.0.0.0/8 scram-sha-256" rule.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_HostScram() {
	cluster := buildCustomRulesCluster("s44-host-scram")
	content := s.reconcileAndGetHBACustom(cluster)

	assert.Contains(s.T(), content, "host\tall\tall\t10.0.0.0/8\tscram-sha-256",
		"host all all 10.0.0.0/8 scram-sha-256 rule must be present")
}

// TestFunctional_Scenario44_CustomRules_HostSSL verifies the
// "hostssl all all 192.168.0.0/16 scram-sha-256" rule.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_HostSSL() {
	cluster := buildCustomRulesCluster("s44-hostssl")
	content := s.reconcileAndGetHBACustom(cluster)

	assert.Contains(s.T(), content, "hostssl\tall\tall\t192.168.0.0/16\tscram-sha-256",
		"hostssl all all 192.168.0.0/16 scram-sha-256 rule must be present")
}

// TestFunctional_Scenario44_CustomRules_HostReject verifies the
// "host all all 0.0.0.0/0 reject" rule.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_CustomRules_HostReject() {
	cluster := buildCustomRulesCluster("s44-host-reject")
	content := s.reconcileAndGetHBACustom(cluster)

	assert.Contains(s.T(), content, "host\tall\tall\t0.0.0.0/0\treject",
		"host all all 0.0.0.0/0 reject rule must be present")
}

// TestFunctional_Scenario44_UpdateRules_ConfigMapUpdated updates HBA rules and
// verifies the ConfigMap is updated with the new content.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_UpdateRules_ConfigMapUpdated() {
	// Start with the original custom rules.
	cluster := buildCustomRulesCluster("s44-update-rules")

	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// First reconcile: creates the ConfigMap.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Fetch the original ConfigMap and its hash.
	cmBefore, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)
	originalHash := cmBefore.Annotations[util.AnnotationConfigHash]

	// Re-fetch the cluster to get the latest resource version after reconcile.
	cluster, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
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
	}
	err = s.env.Client.Update(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Second reconcile: updates the ConfigMap.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	// Verify the ConfigMap was updated.
	cmAfter, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	content, ok := cmAfter.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")

	assert.Contains(s.T(), content, "host\tall\tall\t172.16.0.0/12\tmd5",
		"updated rule must be present in pg_hba.conf")
	assert.NotContains(s.T(), content, "10.0.0.0/8",
		"old rule must NOT be present after update")

	// Verify hash changed.
	newHash := cmAfter.Annotations[util.AnnotationConfigHash]
	assert.NotEqual(s.T(), originalHash, newHash,
		"config hash must change after HBA rules update")
}

// TestFunctional_Scenario44_UpdateRules_HashChanged verifies the hash annotation
// changes after updating HBA rules.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_UpdateRules_HashChanged() {
	cluster := buildCustomRulesCluster("s44-hash-changed")

	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	// First reconcile.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cmBefore, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)
	hashBefore := cmBefore.Annotations[util.AnnotationConfigHash]
	require.NotEmpty(s.T(), hashBefore, "initial hash must not be empty")

	// Re-fetch the cluster to get the latest resource version after reconcile.
	cluster, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Update rules.
	if cluster.Spec.Auth == nil {
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{}
	}
	cluster.Spec.Auth.HBARules = []cbv1alpha1.HBARule{
		{
			Type:     cbv1alpha1.HBATypeLocal,
			Database: "all",
			User:     "all",
			Method:   cbv1alpha1.AuthMethodPeer,
		},
	}
	err = s.env.Client.Update(s.ctx, cluster)
	require.NoError(s.T(), err)

	// Second reconcile.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cmAfter, err := s.env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)
	hashAfter := cmAfter.Annotations[util.AnnotationConfigHash]

	assert.NotEqual(s.T(), hashBefore, hashAfter,
		"hash annotation must change when HBA rules are updated")
}

// TestFunctional_Scenario44_HBACustomRuleCases runs the cases catalog.
func (s *HBACustomRulesSuite) TestFunctional_Scenario44_HBACustomRuleCases() {
	for _, tc := range cases.HBACustomRuleCases() {
		s.Run(tc.Name, func() {
			cluster := testutil.NewClusterBuilder("s44-case-"+tc.Name, "default").
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				WithPendingGeneration().
				WithHBARules(tc.Rules).
				Build()

			s.env = testutil.NewTestK8sEnv(cluster)

			reconciler := controller.NewAuthReconciler(
				s.env.Client, s.env.Recorder,
				s.env.Builder, s.env.Metrics, s.env.Logger,
			)

			result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err, "reconcile should succeed for case: %s", tc.Description)
			assert.NotZero(s.T(), result.RequeueAfter)

			cm, err := s.env.GetConfigMap(
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
					"pg_hba.conf should contain line: %s (case: %s)", expected, tc.Description)
			}

			// Verify expected rule count.
			if tc.ExpectedCount > 0 {
				assert.Equal(s.T(), tc.ExpectedCount, countRuleLines(content),
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
					"config hash annotation must not be empty (case: %s)", tc.Description)
			}
		})
	}
}
