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

// HBADefaultsSuite tests Scenario 45: pg_hba.conf default rules.
type HBADefaultsSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario45(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(HBADefaultsSuite))
}

func (s *HBADefaultsSuite) SetupTest() {
	s.ctx = context.Background()
}

// reqFor builds a reconcile request for the given cluster.
func (s *HBADefaultsSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// reconcileAndGetHBA is a helper that creates a cluster, reconciles auth, and
// returns the pg_hba.conf content from the generated ConfigMap.
func (s *HBADefaultsSuite) reconcileAndGetHBA(cluster *cbv1alpha1.CloudberryCluster) string {
	s.T().Helper()

	s.env = testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		s.env.Client, s.env.Recorder,
		s.env.Builder, s.env.Metrics, s.env.Logger,
	)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	cm, err := s.env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)

	content, ok := cm.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")
	return content
}

// buildClusterNoHBA creates a minimal running cluster with NO hbaRules.
func buildClusterNoHBA(name string) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// buildClusterWithHBA creates a running cluster with the given hbaRules.
func buildClusterWithHBA(name string, rules []cbv1alpha1.HBARule) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(rules).
		Build()
}

// TestFunctional_Scenario45_NoHBARules_GeneratesDefaults verifies that deploying
// a cluster with NO hbaRules generates the correct default pg_hba.conf.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_NoHBARules_GeneratesDefaults() {
	cluster := buildClusterNoHBA("s45-no-rules")
	content := s.reconcileAndGetHBA(cluster)

	tc := cases.HBADefaultRuleCases()[0] // no_hba_rules_generates_defaults
	for _, expected := range tc.ExpectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain default line: %s", expected)
	}
}

// TestFunctional_Scenario45_DefaultRuleOrder verifies that local rules come
// before host rules in the generated pg_hba.conf.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_DefaultRuleOrder() {
	cluster := buildClusterNoHBA("s45-rule-order")
	content := s.reconcileAndGetHBA(cluster)

	// Find positions of the first local and first host rule.
	localIdx := strings.Index(content, "local\tall\tgpadmin\ttrust")
	hostIdx := strings.Index(content, "host\tall\tgpadmin\t127.0.0.1/32\ttrust")

	require.NotEqual(s.T(), -1, localIdx, "local gpadmin rule must be present")
	require.NotEqual(s.T(), -1, hostIdx, "host gpadmin rule must be present")
	assert.Less(s.T(), localIdx, hostIdx,
		"local rules must appear before host rules in pg_hba.conf")

	// Also verify the second local rule comes before the first host rule.
	localAllIdx := strings.Index(content, "local\tall\tall\tscram-sha-256")
	require.NotEqual(s.T(), -1, localAllIdx, "local all rule must be present")
	assert.Less(s.T(), localAllIdx, hostIdx,
		"all local rules must appear before host rules")
}

// TestFunctional_Scenario45_ReplicationRulePresent verifies the replication
// rule is present in the default pg_hba.conf.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_ReplicationRulePresent() {
	cluster := buildClusterNoHBA("s45-repl-rule")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
		"replication rule must be present in default pg_hba.conf")
}

// TestFunctional_Scenario45_GpadminTrustLocal verifies that local gpadmin
// connections use the trust authentication method.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_GpadminTrustLocal() {
	cluster := buildClusterNoHBA("s45-gpadmin-local")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "local\tall\tgpadmin\ttrust",
		"local gpadmin must use trust authentication")
}

// TestFunctional_Scenario45_AllUsersScramLocal verifies that local connections
// for all users use scram-sha-256 authentication.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_AllUsersScramLocal() {
	cluster := buildClusterNoHBA("s45-all-local")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "local\tall\tall\tscram-sha-256",
		"local all users must use scram-sha-256 authentication")
}

// TestFunctional_Scenario45_GpadminTrustLocalhost verifies that host gpadmin
// connections from 127.0.0.1/32 use trust authentication.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_GpadminTrustLocalhost() {
	cluster := buildClusterNoHBA("s45-gpadmin-host")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "host\tall\tgpadmin\t127.0.0.1/32\ttrust",
		"host gpadmin from 127.0.0.1/32 must use trust authentication")
}

// TestFunctional_Scenario45_AllUsersScramRemote verifies that host connections
// for all users from 0.0.0.0/0 use scram-sha-256 authentication.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_AllUsersScramRemote() {
	cluster := buildClusterNoHBA("s45-all-remote")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "host\tall\tall\t0.0.0.0/0\tscram-sha-256",
		"host all users from 0.0.0.0/0 must use scram-sha-256 authentication")
}

// TestFunctional_Scenario45_ReplicationScram verifies that host replication
// connections from 0.0.0.0/0 use scram-sha-256 authentication.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_ReplicationScram() {
	cluster := buildClusterNoHBA("s45-repl-scram")
	content := s.reconcileAndGetHBA(cluster)

	assert.Contains(s.T(), content, "host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
		"host replication from 0.0.0.0/0 must use scram-sha-256 authentication")
}

// TestFunctional_Scenario45_CustomRulesOverrideDefaults verifies that when
// custom HBA rules are provided, the defaults are NOT present.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_CustomRulesOverrideDefaults() {
	tc := cases.HBADefaultRuleCases()[2] // custom_rules_override_defaults
	cluster := buildClusterWithHBA("s45-custom-override", tc.HBARules)
	content := s.reconcileAndGetHBA(cluster)

	// Verify custom rules are present.
	for _, expected := range tc.ExpectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain custom line: %s", expected)
	}

	// Verify default rules are excluded.
	for _, excluded := range tc.ExcludedLines {
		assert.NotContains(s.T(), content, excluded,
			"pg_hba.conf should NOT contain default line when custom rules are set: %s", excluded)
	}
}

// TestFunctional_Scenario45_BehavioralVerification simulates behavioral checks
// by verifying the ConfigMap content matches what each connection type would use.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_BehavioralVerification() {
	cluster := buildClusterNoHBA("s45-behavioral")
	content := s.reconcileAndGetHBA(cluster)

	// Define the expected connection-type-to-auth-method mapping.
	behavioralChecks := []struct {
		description  string
		expectedLine string
	}{
		{
			description:  "local gpadmin -> trust (no password needed)",
			expectedLine: "local\tall\tgpadmin\ttrust",
		},
		{
			description:  "local other user -> scram-sha-256 (password required)",
			expectedLine: "local\tall\tall\tscram-sha-256",
		},
		{
			description:  "host gpadmin from 127.0.0.1 -> trust",
			expectedLine: "host\tall\tgpadmin\t127.0.0.1/32\ttrust",
		},
		{
			description:  "host any user from 0.0.0.0/0 -> scram-sha-256",
			expectedLine: "host\tall\tall\t0.0.0.0/0\tscram-sha-256",
		},
		{
			description:  "host replication from 0.0.0.0/0 -> scram-sha-256",
			expectedLine: "host\treplication\tall\t0.0.0.0/0\tscram-sha-256",
		},
	}

	for _, check := range behavioralChecks {
		assert.Contains(s.T(), content, check.expectedLine,
			"behavioral verification failed: %s", check.description)
	}

	// Verify the ConfigMap has exactly 5 rule lines (excluding comments and blanks).
	lines := strings.Split(content, "\n")
	ruleCount := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		ruleCount++
	}
	assert.Equal(s.T(), 5, ruleCount,
		"default pg_hba.conf should contain exactly 5 rules")
}

// TestFunctional_Scenario45_EmptyHBARules_GeneratesDefaults verifies that an
// empty HBARules slice also triggers default rule generation.
func (s *HBADefaultsSuite) TestFunctional_Scenario45_EmptyHBARules_GeneratesDefaults() {
	tc := cases.HBADefaultRuleCases()[1] // empty_hba_rules_generates_defaults
	cluster := buildClusterWithHBA("s45-empty-rules", tc.HBARules)
	content := s.reconcileAndGetHBA(cluster)

	for _, expected := range tc.ExpectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain default line even with empty HBARules: %s", expected)
	}
}
