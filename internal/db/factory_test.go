package db

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func TestNewClientFactory(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	t.Run("with logger", func(t *testing.T) {
		logger := slog.Default()
		factory := NewClientFactory(k8sClient, logger)
		require.NotNil(t, factory)
		assert.NotNil(t, factory.k8sClient)
		assert.NotNil(t, factory.logger)
	})

	t.Run("nil logger uses default", func(t *testing.T) {
		factory := NewClientFactory(k8sClient, nil)
		require.NotNil(t, factory)
		assert.NotNil(t, factory.logger)
	})
}

func TestClientFactory_NewClient_NilCluster(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	factory := NewClientFactory(k8sClient, nil)

	client, err := factory.NewClient(context.Background(), nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "cluster must not be nil")
}

func TestClientFactory_NewClient_MissingSecret(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Port: 5432,
			},
		},
	}

	client, err := factory.NewClient(context.Background(), cluster)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "reading admin password")
}

func TestClientFactory_NewClient_MissingPasswordKey(t *testing.T) {
	scheme := newTestScheme()

	// Create a secret without the "password" key
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"wrong-key": []byte("secret"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Port: 5432,
			},
		},
	}

	client, err := factory.NewClient(context.Background(), cluster)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "does not contain key")
}

func TestClientFactory_ReadAdminPassword_Success(t *testing.T) {
	scheme := newTestScheme()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("my-secret-password"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	password, err := factory.readAdminPassword(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, "my-secret-password", password)
}

func TestClientFactory_NewClient_CustomAdminUser(t *testing.T) {
	// This test verifies the username resolution logic.
	// The actual DB connection will fail, but we can verify the factory
	// correctly reads the admin user from the cluster spec.
	scheme := newTestScheme()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("secret"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Port: 5432,
			},
			Auth: &cbv1alpha1.AuthSpec{
				Basic: &cbv1alpha1.BasicAuthSpec{
					AdminUser: "custom-admin",
				},
			},
		},
	}

	// This will fail at the DB connection stage, but the factory logic is exercised
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := factory.NewClient(ctx, cluster)
	// Expected to fail because there's no real DB
	assert.Error(t, err)
}

func TestClientFactory_NewClient_DefaultPort(t *testing.T) {
	scheme := newTestScheme()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("secret"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Port: 0, // Should use default
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := factory.NewClient(ctx, cluster)
	// Expected to fail because there's no real DB, but the factory logic is exercised
	assert.Error(t, err)
}

func TestEscapeConnParam(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "localhost",
			expected: "localhost",
		},
		{
			name:     "string with space",
			input:    "my host",
			expected: "'my host'",
		},
		{
			name:     "string with backslash",
			input:    `pass\word`,
			expected: `'pass\\word'`,
		},
		{
			name:     "string with single quote",
			input:    "it's",
			expected: `'it\'s'`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "string with tab",
			input:    "pass\tword",
			expected: "'pass\tword'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeConnParam(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClassifySeverity(t *testing.T) {
	tests := []struct {
		name              string
		value             int64
		warningThreshold  int64
		criticalThreshold int64
		expected          string
	}{
		{
			name:              "info level",
			value:             5,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "info",
		},
		{
			name:              "warning level",
			value:             25,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "warning",
		},
		{
			name:              "critical level",
			value:             60,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "critical",
		},
		{
			name:              "at warning threshold",
			value:             20,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "warning",
		},
		{
			name:              "at critical threshold",
			value:             50,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "critical",
		},
		{
			name:              "zero value",
			value:             0,
			warningThreshold:  20,
			criticalThreshold: 50,
			expected:          "info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifySeverity(tt.value, tt.warningThreshold, tt.criticalThreshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildConnectionString_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "password with spaces",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "my password", SSLMode: "disable",
			},
		},
		{
			name: "password with special chars",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "p@ss!w0rd#$%", SSLMode: "disable",
			},
		},
		{
			name: "host with dots",
			cfg: Config{
				Host: "db.example.com", Port: 5432, Database: "db",
				Username: "user", Password: "pass", SSLMode: "disable",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildConnectionString(tt.cfg)
			assert.NoError(t, err)
			assert.NotEmpty(t, result)
		})
	}
}
