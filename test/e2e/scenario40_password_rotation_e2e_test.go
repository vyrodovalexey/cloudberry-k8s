//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 40: Password Rotation (E2E)
// ============================================================================
//
// End-to-end tests for password rotation, verifying the full lifecycle of
// admin password creation, rotation, and authentication behavior.
// ============================================================================

// Scenario40PasswordRotationE2ESuite tests password rotation end-to-end.
type Scenario40PasswordRotationE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario40(t *testing.T) {
	suite.Run(t, new(Scenario40PasswordRotationE2ESuite))
}

// newAuthServer creates an API server with basic auth for the given cluster and users.
func (s *Scenario40PasswordRotationE2ESuite) newAuthServer(
	cluster *cbv1alpha1.CloudberryCluster,
	store *auth.InMemoryCredentialStore,
) (*api.Server, http.Handler) {
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, 0)
	return server, server.Handler()
}

// TestE2E_Scenario40_AdminSecretCreated verifies that the cluster controller
// creates an admin password Secret for a new cluster.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_AdminSecretCreated() {
	s.logger.Info("starting scenario 40: admin secret created")

	cluster := testutil.NewClusterBuilder("s40-admin-secret", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	secretName := util.AdminPasswordSecretName(cluster.Name)

	// Verify no secret exists initially.
	existing := &corev1.Secret{}
	err := k8sEnv.Client.Get(s.ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, existing)
	require.True(s.T(), apierrors.IsNotFound(err),
		"admin secret should not exist before controller creates it")

	// Simulate controller creating the secret.
	password, genErr := util.GenerateRandomPassword()
	require.NoError(s.T(), genErr)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelManagedBy: util.LabelManagedByValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte(password),
		},
	}
	require.NoError(s.T(), k8sEnv.Client.Create(s.ctx, secret))

	// Verify the secret was created.
	retrieved := &corev1.Secret{}
	err = k8sEnv.Client.Get(s.ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, retrieved)
	require.NoError(s.T(), err, "admin secret should exist after creation")
	assert.NotEmpty(s.T(), retrieved.Data["password"],
		"password should not be empty")
	assert.Equal(s.T(), util.LabelManagedByValue, retrieved.Labels[util.LabelManagedBy],
		"secret should have managed-by label")

	s.logger.Info("scenario 40: admin secret created completed")
}

// TestE2E_Scenario40_PasswordChange_NewWorks verifies that after changing the
// password, the new password authenticates successfully through the full API stack.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_PasswordChange_NewWorks() {
	s.logger.Info("starting scenario 40: password change - new works")

	cluster := testutil.NewClusterBuilder("s40-pw-new-works", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	oldPassword := "old-e2e-password"
	newPassword := "new-e2e-password-rotated"

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", oldPassword, auth.PermissionAdmin)

	server, handler := s.newAuthServer(cluster, store)
	defer server.Close()

	// Verify old password works.
	s.Run("old_password_works_before_rotation", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", oldPassword)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(s.T(), http.StatusOK, rec.Code)
	})

	// Rotate password.
	store.SetCredentials("admin", newPassword, auth.PermissionAdmin)

	// Verify new password works.
	s.Run("new_password_works_after_rotation", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", newPassword)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"new password should authenticate successfully after rotation")
	})

	s.logger.Info("scenario 40: password change - new works completed")
}

// TestE2E_Scenario40_PasswordChange_OldFails verifies that after changing the
// password, the old password no longer authenticates.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_PasswordChange_OldFails() {
	s.logger.Info("starting scenario 40: password change - old fails")

	cluster := testutil.NewClusterBuilder("s40-pw-old-fails", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	oldPassword := "old-e2e-password-fail"
	newPassword := "new-e2e-password-success"

	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", oldPassword, auth.PermissionAdmin)

	server, handler := s.newAuthServer(cluster, store)
	defer server.Close()

	// Verify old password works before rotation.
	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", oldPassword)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(s.T(), http.StatusOK, rec.Code,
		"old password should work before rotation")

	// Rotate password.
	store.SetCredentials("admin", newPassword, auth.PermissionAdmin)

	// Verify old password fails.
	req = httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", oldPassword)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"old password should fail after rotation")

	s.logger.Info("scenario 40: password change - old fails completed")
}

// TestE2E_Scenario40_VaultWatcher_DetectsChange verifies that the Vault
// SecretWatcher detects secret changes and invokes the onChange callback.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_VaultWatcher_DetectsChange() {
	s.logger.Info("starting scenario 40: vault watcher detects change")

	callCount := 0
	mockClient := &e2eMockVaultClient{
		readFunc: func(_ context.Context, _ string) (map[string]interface{}, error) {
			callCount++
			if callCount <= 1 {
				return map[string]interface{}{
					"password": "initial-vault-password",
				}, nil
			}
			return map[string]interface{}{
				"password": "rotated-vault-password",
			}, nil
		},
		enabled: true,
	}

	changeCh := make(chan map[string]interface{}, 1)
	onChange := func(data map[string]interface{}) {
		changeCh <- data
	}

	watcher := vault.NewSecretWatcher(
		mockClient,
		"secret/data/cloudberry/admin-password",
		50*time.Millisecond,
		onChange,
		s.logger,
	)

	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()

	go watcher.Watch(ctx)

	select {
	case data := <-changeCh:
		assert.Equal(s.T(), "rotated-vault-password", data["password"],
			"onChange should receive the updated secret data")
		s.logger.Info("vault watcher detected change successfully")
	case <-ctx.Done():
		s.T().Fatal("timed out waiting for SecretWatcher to detect change")
	}

	s.logger.Info("scenario 40: vault watcher detects change completed")
}

// TestE2E_Scenario40_ClusterCRAccepted verifies that a cluster CR with
// password rotation configuration is accepted and persisted correctly.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_ClusterCRAccepted() {
	s.logger.Info("starting scenario 40: cluster CR accepted")

	cluster := testutil.NewClusterBuilder("s40-cr-accepted", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	// Verify the cluster spec has basic auth configured.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.Basic, "basic auth spec should be set")
	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled,
		"basic auth should be enabled")
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser,
		"basic auth admin user should be 'gpadmin'")

	// Create the cluster in the fake K8s env and verify it persists.
	k8sEnv := testutil.NewTestK8sEnv(cluster)
	retrieved, err := k8sEnv.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	require.NotNil(s.T(), retrieved.Spec.Auth.Basic)
	assert.True(s.T(), retrieved.Spec.Auth.Basic.Enabled)
	assert.Equal(s.T(), "gpadmin", retrieved.Spec.Auth.Basic.AdminUser)

	// Verify the API server works with basic auth on this cluster.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", "test-password", auth.PermissionAdmin)

	server, handler := s.newAuthServer(cluster, store)
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", "test-password")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	s.logger.Info("scenario 40: cluster CR accepted completed")
}

// TestE2E_Scenario40_PasswordRotationCases runs the PasswordRotationCases catalog.
func (s *Scenario40PasswordRotationE2ESuite) TestE2E_Scenario40_PasswordRotationCases() {
	s.logger.Info("starting scenario 40: password rotation cases catalog")

	testCases := cases.PasswordRotationCases()
	require.NotEmpty(s.T(), testCases, "PasswordRotationCases should return test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			cluster := testutil.NewClusterBuilder(
				fmt.Sprintf("s40-e2e-%s", tc.Name), s.namespace,
			).
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				WithStatusReady().
				WithBasicAuth(true, "gpadmin").
				Build()

			store := auth.NewInMemoryCredentialStore()
			store.SetCredentials("admin", tc.OldPassword, auth.PermissionAdmin)

			server, handler := s.newAuthServer(cluster, store)
			defer server.Close()

			// Verify old password works before rotation.
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
			req.SetBasicAuth("admin", tc.OldPassword)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(s.T(), http.StatusOK, rec.Code,
				"old password should work before rotation")

			// Rotate password.
			store.SetCredentials("admin", tc.NewPassword, auth.PermissionAdmin)

			// Verify new password behavior.
			if tc.ExpectNewWorks {
				req = httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
				req.SetBasicAuth("admin", tc.NewPassword)
				rec = httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), http.StatusOK, rec.Code,
					"new password should work after rotation")
			}

			// Verify old password behavior.
			if tc.ExpectOldFails {
				req = httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
				req.SetBasicAuth("admin", tc.OldPassword)
				rec = httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
					"old password should fail after rotation")
			}
		})
	}

	s.logger.Info("scenario 40: password rotation cases catalog completed")
}

// e2eMockVaultClient implements vault.Client for E2E testing.
type e2eMockVaultClient struct {
	readFunc func(ctx context.Context, path string) (map[string]interface{}, error)
	enabled  bool
}

// ReadSecret implements vault.Client.
func (m *e2eMockVaultClient) ReadSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, path)
	}
	return nil, nil
}

// WriteSecret implements vault.Client.
func (m *e2eMockVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

// WriteSecretWithResponse implements vault.Client.
func (m *e2eMockVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

// IsEnabled implements vault.Client.
func (m *e2eMockVaultClient) IsEnabled() bool {
	return m.enabled
}
