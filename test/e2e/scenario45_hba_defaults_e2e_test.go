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

// Scenario45HBADefaultsE2ESuite tests Scenario 45: pg_hba.conf default rules end-to-end.
type Scenario45HBADefaultsE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario45(t *testing.T) {
	suite.Run(t, new(Scenario45HBADefaultsE2ESuite))
}

// reconcileAuth creates an AuthReconciler with the given K8s env and reconciles
// the specified cluster, returning the pg_hba.conf content from the ConfigMap.
func (s *Scenario45HBADefaultsE2ESuite) reconcileAuth(
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

	cm, err := env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)

	content, ok := cm.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")
	return content
}

// TestE2E_Scenario45_HBADefaults_NoRulesGeneratesDefaults creates a cluster
// without hbaRules, reconciles, and verifies the ConfigMap contains all 5
// expected default lines.
func (s *Scenario45HBADefaultsE2ESuite) TestE2E_Scenario45_HBADefaults_NoRulesGeneratesDefaults() {
	s.logger.Info("starting scenario 45 E2E: no HBA rules generates defaults")

	cluster := testutil.NewClusterBuilder("e2e-s45-no-rules", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuth(env, cluster)

	tc := cases.HBADefaultRuleCases()[0] // no_hba_rules_generates_defaults
	for _, expected := range tc.ExpectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain default line: %s", expected)
	}

	// Verify exactly 5 rule lines.
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

	s.logger.Info("scenario 45 E2E: no HBA rules generates defaults completed")
}

// TestE2E_Scenario45_HBADefaults_BehavioralVerification verifies each
// connection type maps to the correct auth method by inspecting the generated
// ConfigMap content.
func (s *Scenario45HBADefaultsE2ESuite) TestE2E_Scenario45_HBADefaults_BehavioralVerification() {
	s.logger.Info("starting scenario 45 E2E: behavioral verification")

	cluster := testutil.NewClusterBuilder("e2e-s45-behavioral", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuth(env, cluster)

	// Behavioral verification: each connection type -> expected auth method.
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

	// Verify rule ordering: local rules before host rules.
	localIdx := strings.Index(content, "local\tall\tgpadmin\ttrust")
	hostIdx := strings.Index(content, "host\tall\tgpadmin\t127.0.0.1/32\ttrust")
	require.NotEqual(s.T(), -1, localIdx, "local gpadmin rule must be present")
	require.NotEqual(s.T(), -1, hostIdx, "host gpadmin rule must be present")
	assert.Less(s.T(), localIdx, hostIdx,
		"local rules must appear before host rules")

	s.logger.Info("scenario 45 E2E: behavioral verification completed")
}

// TestE2E_Scenario45_HBADefaults_CustomRulesOverride verifies that when custom
// HBA rules are provided, the defaults are replaced entirely.
func (s *Scenario45HBADefaultsE2ESuite) TestE2E_Scenario45_HBADefaults_CustomRulesOverride() {
	s.logger.Info("starting scenario 45 E2E: custom rules override defaults")

	tc := cases.HBADefaultRuleCases()[2] // custom_rules_override_defaults

	cluster := testutil.NewClusterBuilder("e2e-s45-custom", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(tc.HBARules).
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuth(env, cluster)

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

	s.logger.Info("scenario 45 E2E: custom rules override defaults completed")
}

// TestE2E_Scenario45_HBADefaults_EmptyRulesGeneratesDefaults verifies that an
// empty HBARules slice also triggers default rule generation.
func (s *Scenario45HBADefaultsE2ESuite) TestE2E_Scenario45_HBADefaults_EmptyRulesGeneratesDefaults() {
	s.logger.Info("starting scenario 45 E2E: empty rules generates defaults")

	tc := cases.HBADefaultRuleCases()[1] // empty_hba_rules_generates_defaults

	cluster := testutil.NewClusterBuilder("e2e-s45-empty", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithHBARules(tc.HBARules).
		Build()

	env := testutil.NewTestK8sEnv(cluster)
	content := s.reconcileAuth(env, cluster)

	for _, expected := range tc.ExpectedLines {
		assert.Contains(s.T(), content, expected,
			"pg_hba.conf should contain default line even with empty HBARules: %s", expected)
	}

	s.logger.Info("scenario 45 E2E: empty rules generates defaults completed")
}

// TestE2E_Scenario45_HBADefaults_ConfigMapOwnership verifies that the generated
// ConfigMap has proper labels and annotations set by the builder.
func (s *Scenario45HBADefaultsE2ESuite) TestE2E_Scenario45_HBADefaults_ConfigMapOwnership() {
	s.logger.Info("starting scenario 45 E2E: ConfigMap ownership verification")

	cluster := testutil.NewClusterBuilder("e2e-s45-ownership", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
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

	_, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)

	cm, err := env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)

	// Verify the ConfigMap has the expected name.
	assert.Equal(s.T(), util.PgHBAConfConfigMapName(cluster.Name), cm.Name)

	// Verify the ConfigMap has a config hash annotation.
	assert.NotEmpty(s.T(), cm.Annotations[util.AnnotationConfigHash],
		"ConfigMap should have a config hash annotation")

	// Verify the ConfigMap has common labels.
	assert.NotEmpty(s.T(), cm.Labels, "ConfigMap should have labels")

	s.logger.Info("scenario 45 E2E: ConfigMap ownership verification completed")
}

// Ensure the builder and metrics packages are used (compile-time check).
var (
	_ builder.ResourceBuilder = (*builder.DefaultBuilder)(nil)
	_ metrics.Recorder        = (*metrics.NoopRecorder)(nil)
)
