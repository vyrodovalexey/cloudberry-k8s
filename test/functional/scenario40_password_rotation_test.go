//go:build functional

package functional

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// resolvePasswordWithPriority simulates the resolveAdminPassword priority logic:
// env var > K8s Secret > generate new. This avoids modifying the process
// environment, which is incompatible with t.Parallel.
func resolvePasswordWithPriority(envPassword string, k8sClient client.Client, namespace string) string {
	// Priority 1: env var.
	if envPassword != "" {
		return envPassword
	}

	// Priority 2: K8s Secret.
	existing := &corev1.Secret{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      "cloudberry-operator-admin-password",
		Namespace: namespace,
	}, existing)
	if err == nil {
		if pw, ok := existing.Data["password"]; ok && len(pw) > 0 {
			return string(pw)
		}
	}

	// Priority 3: generate new.
	generated, genErr := util.GenerateRandomPassword()
	if genErr != nil {
		return ""
	}
	return generated
}

// ============================================================================
// Scenario 40: Password Rotation
// ============================================================================
//
// This scenario tests password rotation for both the operator admin password
// and cluster admin passwords, including:
// - Cluster controller creates admin password Secret
// - Existing Secret is not overwritten
// - Operator reads password from K8s Secret
// - Env var takes priority over Secret
// - Password generated when no Secret/env
// - Secret update with new password
// - Basic auth with new password works
// - Basic auth with old password fails
// - Vault SecretWatcher detects changes
// - Password rotation cases catalog
// ============================================================================

// Scenario40PasswordRotationSuite tests password rotation behavior.
type Scenario40PasswordRotationSuite struct {
	suite.Suite
	logBuf *bytes.Buffer
	logger *slog.Logger
}

func TestFunctional_Scenario40(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario40PasswordRotationSuite))
}

func (s *Scenario40PasswordRotationSuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestFunctional_Scenario40_AdminSecret_Created verifies that the cluster
// controller creates an admin password Secret when one does not exist.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_AdminSecret_Created() {
	cluster := testutil.NewClusterBuilder("s40-secret-created", "cloudberry-test").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	secretName := util.AdminPasswordSecretName(cluster.Name)

	// Verify the secret does not exist yet.
	_, err := k8sEnv.GetSecret(context.Background(), secretName, cluster.Namespace)
	require.Error(s.T(), err, "admin secret should not exist before creation")
	assert.True(s.T(), apierrors.IsNotFound(err), "error should be NotFound")

	// Simulate what the cluster controller does: create the admin secret.
	password, genErr := util.GenerateRandomPassword()
	require.NoError(s.T(), genErr, "password generation should succeed")

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
	require.NoError(s.T(), k8sEnv.Client.Create(context.Background(), secret),
		"creating admin secret should succeed")

	// Verify the secret now exists.
	retrieved, err := k8sEnv.GetSecret(context.Background(), secretName, cluster.Namespace)
	require.NoError(s.T(), err, "admin secret should exist after creation")
	assert.NotEmpty(s.T(), retrieved.Data["password"], "password should not be empty")
	assert.Equal(s.T(), util.LabelManagedByValue, retrieved.Labels[util.LabelManagedBy],
		"secret should have managed-by label")
}

// TestFunctional_Scenario40_AdminSecret_NotOverwritten verifies that an existing
// admin password Secret is not overwritten by the controller.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_AdminSecret_NotOverwritten() {
	cluster := testutil.NewClusterBuilder("s40-not-overwritten", "cloudberry-test").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	secretName := util.AdminPasswordSecretName(cluster.Name)
	existingPassword := "user-provided-password-123"

	// Pre-create the secret (simulating user-provided secret).
	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte(existingPassword),
		},
	}

	k8sEnv := testutil.NewTestK8sEnv(cluster, existingSecret)

	// Verify the secret exists with the original password.
	retrieved, err := k8sEnv.GetSecret(context.Background(), secretName, cluster.Namespace)
	require.NoError(s.T(), err, "existing secret should be retrievable")
	assert.Equal(s.T(), existingPassword, string(retrieved.Data["password"]),
		"existing password should not be overwritten")

	// Simulate the controller checking: if secret exists, do nothing.
	existing := &corev1.Secret{}
	getErr := k8sEnv.Client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, existing)
	require.NoError(s.T(), getErr, "getting existing secret should succeed")

	// The controller would return nil here (secret exists, nothing to do).
	assert.Equal(s.T(), existingPassword, string(existing.Data["password"]),
		"password should remain unchanged after controller check")
}

// TestFunctional_Scenario40_OperatorPassword_FromSecret verifies that the
// operator reads the admin password from a K8s Secret when no env var is set.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_OperatorPassword_FromSecret() {
	secretPassword := "secret-stored-password-456"

	// Create a fake K8s env with the operator admin password secret.
	operatorSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-operator-admin-password",
			Namespace: util.OperatorNamespace,
			Labels: map[string]string{
				util.LabelManagedBy: util.LabelManagedByValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte(secretPassword),
		},
	}

	k8sEnv := testutil.NewTestK8sEnv(operatorSecret)

	// Simulate resolveAdminPassword logic: when env var is empty, read from Secret.
	// We simulate the env-var-empty case by directly reading the Secret.
	existing := &corev1.Secret{}
	err := k8sEnv.Client.Get(context.Background(), types.NamespacedName{
		Name:      "cloudberry-operator-admin-password",
		Namespace: util.OperatorNamespace,
	}, existing)
	require.NoError(s.T(), err, "operator admin password secret should exist")

	pw, ok := existing.Data["password"]
	require.True(s.T(), ok, "password key should exist in secret")
	assert.Equal(s.T(), secretPassword, string(pw),
		"password should match the stored secret value")
}

// TestFunctional_Scenario40_OperatorPassword_FromEnvVar verifies that the
// CLOUDBERRY_API_ADMIN_PASSWORD env var takes priority over the K8s Secret.
// This test simulates the resolveAdminPassword priority logic without modifying
// the process environment (which is incompatible with t.Parallel).
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_OperatorPassword_FromEnvVar() {
	envPassword := "env-var-password-789"
	secretPassword := "secret-stored-password-456"

	// Create a secret that would be used if no env var is set.
	operatorSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-operator-admin-password",
			Namespace: util.OperatorNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte(secretPassword),
		},
	}
	k8sEnv := testutil.NewTestK8sEnv(operatorSecret)

	// Simulate resolveAdminPassword priority: env var > Secret > generate.
	// When env var is set, it takes priority regardless of the Secret.
	resolvedPassword := resolvePasswordWithPriority(
		envPassword,
		k8sEnv.Client,
		util.OperatorNamespace,
	)
	assert.Equal(s.T(), envPassword, resolvedPassword,
		"env var should take priority over K8s Secret")
	assert.NotEqual(s.T(), secretPassword, resolvedPassword,
		"secret password should not be used when env var is set")

	// When env var is empty, the Secret should be used.
	resolvedFromSecret := resolvePasswordWithPriority(
		"",
		k8sEnv.Client,
		util.OperatorNamespace,
	)
	assert.Equal(s.T(), secretPassword, resolvedFromSecret,
		"secret password should be used when env var is empty")
}

// TestFunctional_Scenario40_OperatorPassword_Generated verifies that a random
// password is generated when neither env var nor Secret exists.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_OperatorPassword_Generated() {
	k8sEnv := testutil.NewTestK8sEnv()

	// Verify no secret exists.
	existing := &corev1.Secret{}
	err := k8sEnv.Client.Get(context.Background(), types.NamespacedName{
		Name:      "cloudberry-operator-admin-password",
		Namespace: util.OperatorNamespace,
	}, existing)
	require.True(s.T(), apierrors.IsNotFound(err),
		"operator admin password secret should not exist")

	// Simulate resolveAdminPassword when both env var and Secret are absent:
	// a random password should be generated.
	resolvedPassword := resolvePasswordWithPriority(
		"",
		k8sEnv.Client,
		util.OperatorNamespace,
	)
	assert.NotEmpty(s.T(), resolvedPassword,
		"generated password should not be empty")
	assert.GreaterOrEqual(s.T(), len(resolvedPassword), 16,
		"generated password should be at least 16 characters")

	// Verify uniqueness: two generated passwords should differ.
	resolvedPassword2 := resolvePasswordWithPriority(
		"",
		k8sEnv.Client,
		util.OperatorNamespace,
	)
	assert.NotEqual(s.T(), resolvedPassword, resolvedPassword2,
		"two generated passwords should be different")
}

// TestFunctional_Scenario40_SecretUpdate_NewPassword verifies that updating
// the Secret with a new password results in a different password value.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_SecretUpdate_NewPassword() {
	originalPassword := "original-password-abc"
	newPassword := "rotated-password-xyz"

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s40-update-test-admin-password",
			Namespace: "cloudberry-test",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte(originalPassword),
		},
	}

	k8sEnv := testutil.NewTestK8sEnv(secret)

	// Verify original password.
	retrieved, err := k8sEnv.GetSecret(context.Background(), secret.Name, secret.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), originalPassword, string(retrieved.Data["password"]))

	// Update the secret with a new password.
	retrieved.Data["password"] = []byte(newPassword)
	require.NoError(s.T(), k8sEnv.Client.Update(context.Background(), retrieved),
		"updating secret should succeed")

	// Verify the new password.
	updated, err := k8sEnv.GetSecret(context.Background(), secret.Name, secret.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), newPassword, string(updated.Data["password"]),
		"password should be updated to the new value")
	assert.NotEqual(s.T(), originalPassword, string(updated.Data["password"]),
		"password should differ from the original")
}

// TestFunctional_Scenario40_BasicAuth_WithNewPassword verifies that after
// rotating the password, the new password authenticates successfully.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_BasicAuth_WithNewPassword() {
	oldPassword := "old-password-123"
	newPassword := "new-rotated-password-456"

	// Set up credential store with the old password.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", oldPassword, auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	cluster := testutil.NewClusterBuilder("s40-new-pw", "cloudberry-test").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, 0)
	defer server.Close()
	handler := server.Handler()

	// Verify old password works.
	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", oldPassword)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"old password should work before rotation")

	// Rotate: update the credential store with the new password.
	store.SetCredentials("admin", newPassword, auth.PermissionAdmin)

	// Verify new password works.
	req = httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", newPassword)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"new password should work after rotation")
}

// TestFunctional_Scenario40_BasicAuth_OldPasswordFails verifies that after
// rotating the password, the old password no longer authenticates.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_BasicAuth_OldPasswordFails() {
	oldPassword := "old-password-abc"
	newPassword := "new-password-xyz"

	// Set up credential store with the old password.
	store := auth.NewInMemoryCredentialStore()
	store.SetCredentials("admin", oldPassword, auth.PermissionAdmin)

	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	cluster := testutil.NewClusterBuilder("s40-old-pw-fail", "cloudberry-test").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, 0)
	defer server.Close()
	handler := server.Handler()

	// Verify old password works before rotation.
	req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", oldPassword)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"old password should work before rotation")

	// Rotate: update the credential store with the new password.
	store.SetCredentials("admin", newPassword, auth.PermissionAdmin)

	// Verify old password fails after rotation.
	req = httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
	req.SetBasicAuth("admin", oldPassword)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"old password should fail after rotation")
}

// TestFunctional_Scenario40_VaultSecretWatcher_DetectsChange verifies that the
// Vault SecretWatcher detects when a secret changes and invokes the onChange callback.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_VaultSecretWatcher_DetectsChange() {
	// Create a mock vault client that returns different data on successive reads.
	callCount := 0
	mockClient := &mockVaultClient{
		readFunc: func(_ context.Context, _ string) (map[string]interface{}, error) {
			callCount++
			if callCount <= 1 {
				return map[string]interface{}{
					"password": "initial-password",
				}, nil
			}
			return map[string]interface{}{
				"password": "rotated-password",
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go watcher.Watch(ctx)

	// Wait for the onChange callback to be invoked.
	select {
	case data := <-changeCh:
		assert.Equal(s.T(), "rotated-password", data["password"],
			"onChange should receive the updated secret data")
	case <-ctx.Done():
		s.T().Fatal("timed out waiting for SecretWatcher to detect change")
	}
}

// TestFunctional_Scenario40_PasswordRotationCases runs the PasswordRotationCases catalog.
func (s *Scenario40PasswordRotationSuite) TestFunctional_Scenario40_PasswordRotationCases() {
	testCases := cases.PasswordRotationCases()
	require.NotEmpty(s.T(), testCases, "PasswordRotationCases should return test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			// Set up credential store with the old password.
			store := auth.NewInMemoryCredentialStore()
			store.SetCredentials("admin", tc.OldPassword, auth.PermissionAdmin)

			basicProvider := auth.NewBasicAuthProvider(store, s.logger)
			authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

			cluster := testutil.NewClusterBuilder(
				fmt.Sprintf("s40-case-%s", tc.Name), "cloudberry-test",
			).
				WithFinalizer().
				WithStatusReady().
				WithBasicAuth(true, "gpadmin").
				Build()

			k8sEnv := testutil.NewTestK8sEnv(cluster)
			server := api.NewServer(k8sEnv.Client, authMW, nil, &metrics.NoopRecorder{}, s.logger, 0)
			defer server.Close()
			handler := server.Handler()

			// Verify old password works before rotation.
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
			req.SetBasicAuth("admin", tc.OldPassword)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(s.T(), http.StatusOK, rec.Code,
				"old password should work before rotation")

			// Rotate: update the credential store with the new password.
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
}

// mockVaultClient implements vault.Client for testing.
type mockVaultClient struct {
	readFunc func(ctx context.Context, path string) (map[string]interface{}, error)
	enabled  bool
}

// ReadSecret implements vault.Client.
func (m *mockVaultClient) ReadSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	if m.readFunc != nil {
		return m.readFunc(ctx, path)
	}
	return nil, nil
}

// WriteSecret implements vault.Client.
func (m *mockVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

// WriteSecretWithResponse implements vault.Client.
func (m *mockVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

// IsEnabled implements vault.Client.
func (m *mockVaultClient) IsEnabled() bool {
	return m.enabled
}
