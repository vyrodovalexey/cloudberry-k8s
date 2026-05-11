//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// WebhookSuite tests validating and mutating webhooks.
type WebhookSuite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestFunctional_Webhook(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WebhookSuite))
}

func (s *WebhookSuite) SetupTest() {
	s.ctx = context.Background()
	s.validator = webhook.NewCloudberryClusterValidator()
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// --- Validating Webhook Tests ---

func (s *WebhookSuite) TestFunctional_Validate_ValidMinimalCluster() {
	cluster := testutil.MinimalCluster("test-valid", "default")

	warnings, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), warnings)
}

func (s *WebhookSuite) TestFunctional_Validate_ValidFullCluster() {
	cluster := testutil.FullCluster("test-full", "default")

	warnings, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
	// May have warnings about spread mirroring
	_ = warnings
}

func (s *WebhookSuite) TestFunctional_Validate_InvalidSegmentCount() {
	cluster := testutil.NewClusterBuilder("test-invalid-seg", "default").
		WithSegments(0).
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "segments.count")
}

func (s *WebhookSuite) TestFunctional_Validate_OIDCEnabled_MissingIssuer() {
	cluster := testutil.NewClusterBuilder("test-oidc-no-issuer", "default").
		WithOIDC(true, "", "client-id").
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "issuerURL")
}

func (s *WebhookSuite) TestFunctional_Validate_OIDCEnabled_MissingClientID() {
	cluster := testutil.NewClusterBuilder("test-oidc-no-client", "default").
		WithOIDC(true, "http://keycloak/realms/test", "").
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "clientID")
}

func (s *WebhookSuite) TestFunctional_Validate_OIDCDisabled_NoValidation() {
	cluster := testutil.NewClusterBuilder("test-oidc-disabled", "default").
		WithOIDC(false, "", "").
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

func (s *WebhookSuite) TestFunctional_Validate_VaultEnabled_MissingAddress() {
	cluster := testutil.NewClusterBuilder("test-vault-no-addr", "default").
		WithVault(true, "", "token").
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "vault.address")
}

func (s *WebhookSuite) TestFunctional_Validate_VaultDisabled_NoValidation() {
	cluster := testutil.NewClusterBuilder("test-vault-disabled", "default").
		WithVault(false, "", "").
		Build()

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

func (s *WebhookSuite) TestFunctional_Validate_InvalidDeletionPolicy() {
	cluster := testutil.NewClusterBuilder("test-invalid-dp", "default").Build()
	cluster.Spec.DeletionPolicy = "InvalidPolicy"

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "deletionPolicy")
}

func (s *WebhookSuite) TestFunctional_Validate_ValidDeletionPolicies() {
	for _, policy := range []cbv1alpha1.DeletionPolicy{
		cbv1alpha1.DeletionPolicyRetain,
		cbv1alpha1.DeletionPolicyDelete,
	} {
		cluster := testutil.NewClusterBuilder("test-dp-"+string(policy), "default").
			WithDeletionPolicy(policy).
			Build()

		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err, "policy %s should be valid", policy)
	}
}

func (s *WebhookSuite) TestFunctional_Validate_MissingCoordinatorStorage() {
	cluster := testutil.NewClusterBuilder("test-no-coord-storage", "default").Build()
	cluster.Spec.Coordinator.Storage.Size = ""

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "coordinator.storage.size")
}

func (s *WebhookSuite) TestFunctional_Validate_MissingSegmentStorage() {
	cluster := testutil.NewClusterBuilder("test-no-seg-storage", "default").Build()
	cluster.Spec.Segments.Storage.Size = ""

	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "segments.storage.size")
}

func (s *WebhookSuite) TestFunctional_Validate_SpreadMirroring_Warning() {
	cluster := testutil.NewClusterBuilder("test-spread-warn", "default").
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutSpread).
		Build()

	warnings, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), warnings, "should warn about spread mirroring with few segments")
}

func (s *WebhookSuite) TestFunctional_Validate_Update_Valid() {
	oldCluster := testutil.MinimalCluster("test-update", "default")
	newCluster := testutil.NewClusterBuilder("test-update", "default").
		WithSegments(8).
		Build()

	warnings, err := s.validator.ValidateUpdate(s.ctx, oldCluster, newCluster)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), warnings)
}

func (s *WebhookSuite) TestFunctional_Validate_Delete_AlwaysSucceeds() {
	cluster := testutil.MinimalCluster("test-delete", "default")

	warnings, err := s.validator.ValidateDelete(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), warnings)
}

// --- Mutating Webhook Tests ---

func (s *WebhookSuite) TestFunctional_Default_SetsVersion() {
	cluster := testutil.NewClusterBuilder("test-default-version", "default").Build()
	cluster.Spec.Version = ""

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), cluster.Spec.Version)
}

func (s *WebhookSuite) TestFunctional_Default_SetsImage() {
	cluster := testutil.NewClusterBuilder("test-default-image", "default").Build()
	cluster.Spec.Image = ""

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), cluster.Spec.Image)
}

func (s *WebhookSuite) TestFunctional_Default_SetsCoordinatorDefaults() {
	cluster := testutil.NewClusterBuilder("test-default-coord", "default").Build()
	cluster.Spec.Coordinator.Replicas = nil
	cluster.Spec.Coordinator.Port = 0

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), cluster.Spec.Coordinator.Replicas)
	assert.Equal(s.T(), int32(1), *cluster.Spec.Coordinator.Replicas)
	assert.Equal(s.T(), int32(5432), cluster.Spec.Coordinator.Port)
}

func (s *WebhookSuite) TestFunctional_Default_SetsSegmentDefaults() {
	cluster := testutil.NewClusterBuilder("test-default-seg", "default").Build()
	cluster.Spec.Segments.PrimariesPerHost = 0
	cluster.Spec.Segments.Mirroring = nil

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(2), cluster.Spec.Segments.PrimariesPerHost)
	assert.NotNil(s.T(), cluster.Spec.Segments.Mirroring)
	assert.True(s.T(), cluster.Spec.Segments.Mirroring.Enabled)
}

func (s *WebhookSuite) TestFunctional_Default_SetsAuthDefaults() {
	cluster := testutil.NewClusterBuilder("test-default-auth", "default").Build()
	cluster.Spec.Auth = nil

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), cluster.Spec.Auth)
	assert.NotNil(s.T(), cluster.Spec.Auth.Basic)
	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled)
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser)
}

func (s *WebhookSuite) TestFunctional_Default_SetsHADefaults() {
	cluster := testutil.NewClusterBuilder("test-default-ha", "default").Build()
	cluster.Spec.HA = nil

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), cluster.Spec.HA)
	assert.Equal(s.T(), int32(60), cluster.Spec.HA.FTSProbeInterval)
	assert.Equal(s.T(), int32(20), cluster.Spec.HA.FTSProbeTimeout)
	assert.Equal(s.T(), int32(5), cluster.Spec.HA.FTSProbeRetries)
}

func (s *WebhookSuite) TestFunctional_Default_SetsMonitoringDefaults() {
	cluster := testutil.NewClusterBuilder("test-default-mon", "default").Build()
	cluster.Spec.Monitoring = nil

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), cluster.Spec.Monitoring)
	assert.True(s.T(), cluster.Spec.Monitoring.Enabled)
	assert.Equal(s.T(), int32(9187), cluster.Spec.Monitoring.MetricsPort)
}

func (s *WebhookSuite) TestFunctional_Default_SetsDeletionPolicy() {
	cluster := testutil.NewClusterBuilder("test-default-dp", "default").Build()
	cluster.Spec.DeletionPolicy = ""

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.DeletionPolicyRetain, cluster.Spec.DeletionPolicy)
}

func (s *WebhookSuite) TestFunctional_Default_OIDCEnabled_SetsScopes() {
	cluster := testutil.NewClusterBuilder("test-default-oidc", "default").
		WithOIDC(true, "http://keycloak/realms/test", "client-id").
		Build()
	cluster.Spec.Auth.OIDC.Scopes = nil

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), cluster.Spec.Auth.OIDC.Scopes)
	assert.Contains(s.T(), cluster.Spec.Auth.OIDC.Scopes, "openid")
}

func (s *WebhookSuite) TestFunctional_Default_PreservesExistingValues() {
	cluster := testutil.NewClusterBuilder("test-default-preserve", "default").
		WithVersion("8.0").
		WithSegments(16).
		WithHA(30, 10, 3).
		Build()

	err := s.defaulter.Default(s.ctx, cluster)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "8.0", cluster.Spec.Version)
	assert.Equal(s.T(), int32(16), cluster.Spec.Segments.Count)
	assert.Equal(s.T(), int32(30), cluster.Spec.HA.FTSProbeInterval)
}
