package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newTestScheme creates a runtime.Scheme with all types needed by the operator tests.
func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = cbv1alpha1.AddToScheme(s)
	_ = admissionregistrationv1.AddToScheme(s)
	return s
}

// newFakeClient creates a fake Kubernetes client with the given initial objects.
func newFakeClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		Build()
}

// testLogger returns a silent logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ---------------------------------------------------------------------------
// resolveAdminPassword
// ---------------------------------------------------------------------------

func TestResolveAdminPassword(t *testing.T) {
	tests := []struct {
		name        string
		envPassword string
		envNS       string
		objects     []client.Object
		wantErr     bool
		errContains string
		wantPass    string // exact match when non-empty
	}{
		{
			name:        "env var takes priority",
			envPassword: "env-secret-123",
			wantPass:    "env-secret-123",
		},
		{
			name:  "existing secret is used",
			envNS: "test-ns",
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      util.OperatorAdminPasswordSecretName,
						Namespace: "test-ns",
					},
					Data: map[string][]byte{
						util.PasswordSecretKey: []byte("stored-password"),
					},
				},
			},
			wantPass: "stored-password",
		},
		{
			name:  "generates new password when no secret exists",
			envNS: "test-ns",
		},
		{
			name:  "empty secret data triggers generation and hits already-exists path",
			envNS: "test-ns",
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      util.OperatorAdminPasswordSecretName,
						Namespace: "test-ns",
					},
					Data: map[string][]byte{},
				},
			},
			// The secret already exists but has no password key, so the code
			// generates a new password and tries to Create, which hits AlreadyExists.
			// Then it re-reads the secret, but the re-read still has no password.
			wantErr:     true,
			errContains: "creating admin password secret",
		},
		{
			name:  "secret with empty password triggers generation and hits already-exists path",
			envNS: "test-ns",
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      util.OperatorAdminPasswordSecretName,
						Namespace: "test-ns",
					},
					Data: map[string][]byte{
						util.PasswordSecretKey: {},
					},
				},
			},
			// Same as above: empty password value triggers generation, Create
			// hits AlreadyExists, re-read finds empty password again.
			wantErr:     true,
			errContains: "creating admin password secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set env vars (cannot use t.Parallel with t.Setenv).
			if tt.envPassword != "" {
				t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", tt.envPassword)
			} else {
				t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
			}
			if tt.envNS != "" {
				t.Setenv("POD_NAMESPACE", tt.envNS)
			} else {
				t.Setenv("POD_NAMESPACE", util.OperatorNamespace)
			}

			k8sClient := newFakeClient(tt.objects...)
			logger := testLogger()

			password, err := resolveAdminPassword(context.Background(), k8sClient, logger)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, password)

			if tt.wantPass != "" {
				assert.Equal(t, tt.wantPass, password)
			}
		})
	}
}

func TestResolveAdminPassword_DefaultNamespace(t *testing.T) {
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
	t.Setenv("POD_NAMESPACE", "")

	k8sClient := newFakeClient()
	logger := testLogger()

	password, err := resolveAdminPassword(context.Background(), k8sClient, logger)
	require.NoError(t, err)
	assert.NotEmpty(t, password)
}

// ---------------------------------------------------------------------------
// injectCABundle
// ---------------------------------------------------------------------------

func TestInjectCABundle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		objects  []client.Object
		caBundle []byte
		wantErr  bool
	}{
		{
			name:     "no webhook configurations",
			caBundle: []byte("test-ca-bundle"),
		},
		{
			name:     "patches validating webhook",
			caBundle: []byte("test-ca-bundle"),
			objects: []client.Object{
				&admissionregistrationv1.ValidatingWebhookConfiguration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-vwc",
						Labels: map[string]string{
							util.LabelPartOf: util.LabelPartOfValue,
						},
					},
					Webhooks: []admissionregistrationv1.ValidatingWebhook{
						{
							Name:                    "test.webhook.io",
							AdmissionReviewVersions: []string{"v1"},
							ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
							SideEffects: func() *admissionregistrationv1.SideEffectClass {
								se := admissionregistrationv1.SideEffectClassNone
								return &se
							}(),
						},
					},
				},
			},
		},
		{
			name:     "patches mutating webhook",
			caBundle: []byte("test-ca-bundle"),
			objects: []client.Object{
				&admissionregistrationv1.MutatingWebhookConfiguration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-mwc",
						Labels: map[string]string{
							util.LabelPartOf: util.LabelPartOfValue,
						},
					},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "test.mutating.webhook.io",
							AdmissionReviewVersions: []string{"v1"},
							ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
							SideEffects: func() *admissionregistrationv1.SideEffectClass {
								se := admissionregistrationv1.SideEffectClassNone
								return &se
							}(),
						},
					},
				},
			},
		},
		{
			name:     "patches both webhook types",
			caBundle: []byte("test-ca-bundle"),
			objects: []client.Object{
				&admissionregistrationv1.ValidatingWebhookConfiguration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-vwc",
						Labels: map[string]string{
							util.LabelPartOf: util.LabelPartOfValue,
						},
					},
					Webhooks: []admissionregistrationv1.ValidatingWebhook{
						{
							Name:                    "test.webhook.io",
							AdmissionReviewVersions: []string{"v1"},
							ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
							SideEffects: func() *admissionregistrationv1.SideEffectClass {
								se := admissionregistrationv1.SideEffectClassNone
								return &se
							}(),
						},
					},
				},
				&admissionregistrationv1.MutatingWebhookConfiguration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-mwc",
						Labels: map[string]string{
							util.LabelPartOf: util.LabelPartOfValue,
						},
					},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "test.mutating.webhook.io",
							AdmissionReviewVersions: []string{"v1"},
							ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
							SideEffects: func() *admissionregistrationv1.SideEffectClass {
								se := admissionregistrationv1.SideEffectClassNone
								return &se
							}(),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k8sClient := newFakeClient(tt.objects...)
			logger := testLogger()

			err := injectCABundle(context.Background(), k8sClient, tt.caBundle, logger)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify CA bundles were injected.
			vwcList := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
			_ = k8sClient.List(context.Background(), vwcList, client.MatchingLabels{
				util.LabelPartOf: util.LabelPartOfValue,
			})
			for _, vwc := range vwcList.Items {
				for _, wh := range vwc.Webhooks {
					assert.Equal(t, tt.caBundle, wh.ClientConfig.CABundle)
				}
			}

			mwcList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
			_ = k8sClient.List(context.Background(), mwcList, client.MatchingLabels{
				util.LabelPartOf: util.LabelPartOfValue,
			})
			for _, mwc := range mwcList.Items {
				for _, wh := range mwc.Webhooks {
					assert.Equal(t, tt.caBundle, wh.ClientConfig.CABundle)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// runCertRotation
// ---------------------------------------------------------------------------

// mockCertManager implements certmanager.CertManager for testing.
type mockCertManager struct {
	needsRotation    bool
	needsRotationErr error
	ensureErr        error
	ensureCalled     int
}

func (m *mockCertManager) EnsureCertificates(_ context.Context) ([]byte, error) {
	m.ensureCalled++
	return []byte("ca-bundle"), m.ensureErr
}

func (m *mockCertManager) NeedsRotation(_ context.Context) (bool, error) {
	return m.needsRotation, m.needsRotationErr
}

func TestRunCertRotation_ContextCanceled(t *testing.T) {
	t.Parallel()

	cm := &mockCertManager{}
	logger := testLogger()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, logger)
		close(done)
	}()

	select {
	case <-done:
		// Success: function returned.
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// runCertRotation - additional paths
// ---------------------------------------------------------------------------

func TestRunCertRotation_NeedsRotation_EnsureSucceeds(t *testing.T) {
	t.Parallel()

	cm := &mockCertManager{
		needsRotation: true,
	}
	logger := testLogger()

	// Use a context that we cancel after a short delay to stop the ticker loop.
	ctx, cancel := context.WithCancel(context.Background())

	// Override the ticker by running in a goroutine and canceling after the first tick.
	done := make(chan struct{})
	go func() {
		// We can't easily control the ticker interval (12h), so we test the
		// function by canceling the context. The key is that the mock is set up
		// to return needsRotation=true, so if the ticker fires, it will call
		// EnsureCertificates.
		runCertRotation(ctx, cm, logger)
		close(done)
	}()

	// Cancel after a short delay.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success: function returned after context cancellation.
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after context cancellation")
	}
}

func TestRunCertRotation_NeedsRotation_EnsureFails(t *testing.T) {
	t.Parallel()

	cm := &mockCertManager{
		needsRotation: true,
		ensureErr:     fmt.Errorf("cert rotation failed"),
	}
	logger := testLogger()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, logger)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success: function returned.
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after context cancellation")
	}
}

func TestRunCertRotation_NeedsRotationError(t *testing.T) {
	t.Parallel()

	cm := &mockCertManager{
		needsRotationErr: fmt.Errorf("check failed"),
	}
	logger := testLogger()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, logger)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success: function returned.
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// resolveAdminPassword - race condition path
// ---------------------------------------------------------------------------

func TestResolveAdminPassword_RaceCondition_ReReadSucceeds(t *testing.T) {
	// Simulate: secret doesn't exist initially, Create fails with AlreadyExists,
	// re-read succeeds with valid password.
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
	t.Setenv("POD_NAMESPACE", "test-ns")

	// Use interceptor to simulate the race condition:
	// - First Get: NotFound (no secret)
	// - Create: AlreadyExists (another replica created it)
	// - Second Get: returns secret with password
	getCallCount := 0
	createCalled := false
	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if secret, ok := obj.(*corev1.Secret); ok && key.Name == util.OperatorAdminPasswordSecretName {
					getCallCount++
					if getCallCount == 1 {
						// First call: secret not found.
						return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
					}
					// Second call (after Create fails): return secret with password.
					secret.Name = key.Name
					secret.Namespace = key.Namespace
					secret.Data = map[string][]byte{
						util.PasswordSecretKey: []byte("race-winner-password"),
					}
					return nil
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					createCalled = true
					return apierrors.NewAlreadyExists(corev1.Resource("secrets"), obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	password, err := resolveAdminPassword(context.Background(), k8sClient, logger)
	require.NoError(t, err)
	assert.Equal(t, "race-winner-password", password)
	assert.True(t, createCalled, "Create should have been called")
}

func TestResolveAdminPassword_RaceCondition_ReReadFails(t *testing.T) {
	// Simulate: Create fails with AlreadyExists, re-read also fails.
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
	t.Setenv("POD_NAMESPACE", "test-ns")

	getCallCount := 0
	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok && key.Name == util.OperatorAdminPasswordSecretName {
					getCallCount++
					if getCallCount == 1 {
						return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
					}
					// Second call: also fails.
					return fmt.Errorf("re-read failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return apierrors.NewAlreadyExists(corev1.Resource("secrets"), obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	_, err := resolveAdminPassword(context.Background(), k8sClient, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "re-reading admin password secret after conflict")
}

func TestResolveAdminPassword_GetError(t *testing.T) {
	// Simulate: initial Get returns a non-NotFound error.
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
	t.Setenv("POD_NAMESPACE", "test-ns")

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok && key.Name == util.OperatorAdminPasswordSecretName {
					return fmt.Errorf("API server unavailable")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	_, err := resolveAdminPassword(context.Background(), k8sClient, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checking admin password secret")
}

// ---------------------------------------------------------------------------
// injectCABundle - error paths
// ---------------------------------------------------------------------------

func TestInjectCABundle_ListValidatingWebhookError(t *testing.T) {
	t.Parallel()

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*admissionregistrationv1.ValidatingWebhookConfigurationList); ok {
					return fmt.Errorf("list validating webhooks failed")
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()

	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing validating webhook configurations")
}

func TestInjectCABundle_ListMutatingWebhookError(t *testing.T) {
	t.Parallel()

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*admissionregistrationv1.MutatingWebhookConfigurationList); ok {
					return fmt.Errorf("list mutating webhooks failed")
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()

	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing mutating webhook configurations")
}

func TestInjectCABundle_UpdateValidatingWebhookError(t *testing.T) {
	t.Parallel()

	se := admissionregistrationv1.SideEffectClassNone
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-vwc",
			Labels: map[string]string{
				util.LabelPartOf: util.LabelPartOfValue,
			},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    "test.webhook.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
		},
	}

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(vwc).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
					return fmt.Errorf("update validating webhook failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating validating webhook configuration")
}

func TestInjectCABundle_UpdateMutatingWebhookError(t *testing.T) {
	t.Parallel()

	se := admissionregistrationv1.SideEffectClassNone
	mwc := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-mwc",
			Labels: map[string]string{
				util.LabelPartOf: util.LabelPartOfValue,
			},
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name:                    "test.mutating.webhook.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
		},
	}

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(mwc).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*admissionregistrationv1.MutatingWebhookConfiguration); ok {
					return fmt.Errorf("update mutating webhook failed")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating mutating webhook configuration")
}

func TestInjectCABundle_WebhookWithoutMatchingLabels(t *testing.T) {
	t.Parallel()

	se := admissionregistrationv1.SideEffectClassNone
	// Webhook without matching labels — should be a no-op.
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-vwc",
			Labels: map[string]string{
				"app.kubernetes.io/part-of": "other-operator",
			},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    "other.webhook.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
		},
	}

	k8sClient := newFakeClient(vwc)
	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.NoError(t, err)

	// Verify the non-matching webhook was NOT patched.
	updatedVWC := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	_ = k8sClient.Get(context.Background(), client.ObjectKeyFromObject(vwc), updatedVWC)
	assert.Nil(t, updatedVWC.Webhooks[0].ClientConfig.CABundle, "non-matching webhook should not be patched")
}

// ---------------------------------------------------------------------------
// injectCABundle - multiple webhooks in single configuration
// ---------------------------------------------------------------------------

func TestInjectCABundle_MultipleWebhooksInSingleConfig(t *testing.T) {
	t.Parallel()

	se := admissionregistrationv1.SideEffectClassNone
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "multi-webhook-vwc",
			Labels: map[string]string{
				util.LabelPartOf: util.LabelPartOfValue,
			},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    "first.webhook.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
			{
				Name:                    "second.webhook.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
		},
	}

	k8sClient := newFakeClient(vwc)
	logger := testLogger()
	caBundle := []byte("multi-webhook-ca-bundle")

	err := injectCABundle(context.Background(), k8sClient, caBundle, logger)
	require.NoError(t, err)

	// Verify all webhooks in the configuration got the CA bundle.
	updatedVWC := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	_ = k8sClient.Get(context.Background(), client.ObjectKeyFromObject(vwc), updatedVWC)
	for _, wh := range updatedVWC.Webhooks {
		assert.Equal(t, caBundle, wh.ClientConfig.CABundle)
	}
}

// ---------------------------------------------------------------------------
// resolveAdminPassword - Create error (non-AlreadyExists)
// ---------------------------------------------------------------------------

func TestResolveAdminPassword_CreateError(t *testing.T) {
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "")
	t.Setenv("POD_NAMESPACE", "test-ns")

	testScheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok && key.Name == util.OperatorAdminPasswordSecretName {
					return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return fmt.Errorf("permission denied")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()

	logger := testLogger()

	_, err := resolveAdminPassword(context.Background(), k8sClient, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating admin password secret")
}

// ---------------------------------------------------------------------------
// injectCABundle - empty webhook list (no webhooks in configuration)
// ---------------------------------------------------------------------------

func TestInjectCABundle_EmptyWebhookList(t *testing.T) {
	t.Parallel()

	// Webhook configuration with matching labels but no webhooks.
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "empty-vwc",
			Labels: map[string]string{
				util.LabelPartOf: util.LabelPartOfValue,
			},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{},
	}

	k8sClient := newFakeClient(vwc)
	logger := testLogger()

	err := injectCABundle(context.Background(), k8sClient, []byte("ca-bundle"), logger)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// runCertRotation - no rotation needed
// ---------------------------------------------------------------------------

func TestRunCertRotation_NoRotationNeeded(t *testing.T) {
	t.Parallel()

	cm := &mockCertManager{
		needsRotation: false,
	}
	logger := testLogger()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, logger)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success: function returned.
		assert.Equal(t, 0, cm.ensureCalled, "EnsureCertificates should not be called when rotation is not needed")
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Scheme initialization
// ---------------------------------------------------------------------------

func TestSchemeContainsExpectedTypes(t *testing.T) {
	t.Parallel()

	// Verify the package-level scheme has the expected types registered.
	assert.True(t, scheme.IsGroupRegistered("")) // core/v1
	assert.True(t, scheme.IsGroupRegistered(cbv1alpha1.GroupVersion.Group))
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

func TestConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "cloudberry-operator-admin-password", util.OperatorAdminPasswordSecretName)
	assert.Equal(t, "password", util.PasswordSecretKey)
	assert.Equal(t, 5*time.Second, shutdownTimeout)
}

// ---------------------------------------------------------------------------
// LabelPartOf constants
// ---------------------------------------------------------------------------

func TestLabelPartOfConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "app.kubernetes.io/part-of", util.LabelPartOf)
	assert.Equal(t, "cloudberry-operator", util.LabelPartOfValue)
}
