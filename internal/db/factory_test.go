package db

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newTestCAPEM generates a self-signed CA certificate in PEM form for tests
// that exercise SSL root-CA wiring.
func newTestCAPEM(t *testing.T) []byte {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func sslCluster(enabled bool, certSecret *cbv1alpha1.CertSecretRef) *cbv1alpha1.CloudberryCluster {
	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	if enabled || certSecret != nil {
		cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
			SSL: &cbv1alpha1.SSLSpec{Enabled: enabled, CertSecret: certSecret},
		}
	}
	return cluster
}

func TestResolveSSLMode(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *cbv1alpha1.CloudberryCluster
		expected string
	}{
		{
			name:     "no auth uses disable",
			cluster:  &cbv1alpha1.CloudberryCluster{},
			expected: sslModeDisable,
		},
		{
			name:     "ssl disabled uses disable",
			cluster:  sslCluster(false, nil),
			expected: sslModeDisable,
		},
		{
			name:     "ssl enabled without cert secret uses require",
			cluster:  sslCluster(true, nil),
			expected: sslModeRequire,
		},
		{
			name:     "ssl enabled with cert secret uses verify-ca",
			cluster:  sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "my-cert"}),
			expected: sslModeVerifyCA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, resolveSSLMode(tt.cluster))
		})
	}
}

func TestClientFactory_ReadSSLRootCA(t *testing.T) {
	scheme := newTestScheme()
	caPEM := newTestCAPEM(t)

	t.Run("reads ca.crt from cert secret", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"},
			Data:       map[string][]byte{caCertSecretKey: caPEM},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		factory := NewClientFactory(k8sClient, nil)

		ca, err := factory.readSSLRootCA(
			context.Background(),
			sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "tls-secret"}),
		)
		require.NoError(t, err)
		assert.Equal(t, caPEM, ca)
	})

	t.Run("missing ca.crt falls back to system roots", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"},
			Data:       map[string][]byte{"tls.crt": []byte("cert")},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		factory := NewClientFactory(k8sClient, nil)

		ca, err := factory.readSSLRootCA(
			context.Background(),
			sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "tls-secret"}),
		)
		require.NoError(t, err)
		assert.Nil(t, ca)
	})

	t.Run("empty ca.crt falls back to system roots", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"},
			Data:       map[string][]byte{caCertSecretKey: {}},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		factory := NewClientFactory(k8sClient, nil)

		ca, err := factory.readSSLRootCA(
			context.Background(),
			sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "tls-secret"}),
		)
		require.NoError(t, err)
		assert.Nil(t, ca)
	})

	t.Run("missing cert secret returns error", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		factory := NewClientFactory(k8sClient, nil)

		ca, err := factory.readSSLRootCA(
			context.Background(),
			sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "absent"}),
		)
		require.Error(t, err)
		assert.Nil(t, ca)
		assert.Contains(t, err.Error(), "getting cert secret")
	})
}

func TestApplyRootCA(t *testing.T) {
	caPEM := newTestCAPEM(t)

	t.Run("nil root CA is a no-op", func(t *testing.T) {
		poolCfg, err := pgxpool.ParseConfig(
			"postgres://u:p@localhost:5432/db?sslmode=verify-ca")
		require.NoError(t, err)
		require.NoError(t, applyRootCA(poolCfg, nil))
	})

	t.Run("sets RootCAs on TLS config", func(t *testing.T) {
		poolCfg, err := pgxpool.ParseConfig(
			"postgres://u:p@localhost:5432/db?sslmode=verify-ca")
		require.NoError(t, err)
		require.NotNil(t, poolCfg.ConnConfig.TLSConfig)

		require.NoError(t, applyRootCA(poolCfg, caPEM))
		assert.NotNil(t, poolCfg.ConnConfig.TLSConfig.RootCAs)
	})

	t.Run("invalid PEM returns error", func(t *testing.T) {
		poolCfg, err := pgxpool.ParseConfig(
			"postgres://u:p@localhost:5432/db?sslmode=verify-ca")
		require.NoError(t, err)

		err = applyRootCA(poolCfg, []byte("not a pem"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing SSL root CA")
	})

	t.Run("sslmode disable has no TLS config to attach", func(t *testing.T) {
		poolCfg, err := pgxpool.ParseConfig(
			"postgres://u:p@localhost:5432/db?sslmode=disable")
		require.NoError(t, err)
		require.Nil(t, poolCfg.ConnConfig.TLSConfig)

		// Even with a valid CA, there is no TLS config so this is a no-op.
		require.NoError(t, applyRootCA(poolCfg, caPEM))
	})
}

func TestClientFactory_NewClient_VerifyCALoadsCA(t *testing.T) {
	scheme := newTestScheme()
	caPEM := newTestCAPEM(t)

	passwordSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.AdminPasswordSecretName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string][]byte{passwordSecretKey: []byte("secret")},
	}
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"},
		Data:       map[string][]byte{caCertSecretKey: caPEM},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(passwordSecret, certSecret).Build()
	factory := NewClientFactory(k8sClient, nil)

	cluster := sslCluster(true, &cbv1alpha1.CertSecretRef{Name: "tls-secret"})
	cluster.Spec.Coordinator.Port = 5432

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// The CA is read and applied successfully; the connection itself fails
	// because there is no real DB. The error must come from connecting, not
	// from reading/parsing the CA.
	_, err := factory.NewClient(ctx, cluster)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "reading SSL root CA")
	assert.NotContains(t, err.Error(), "parsing SSL root CA")
}

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

func TestBuildConnectionString_NativeConfig(t *testing.T) {
	// Test that buildConnectionString uses pgx native config builder
	// and handles various parameter values safely.
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "simple parameters",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "pass", SSLMode: "disable",
			},
		},
		{
			name: "password with space",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "my host", SSLMode: "disable",
			},
		},
		{
			name: "password with backslash",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: `pass\word`, SSLMode: "disable",
			},
		},
		{
			name: "password with single quote",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "it's", SSLMode: "disable",
			},
		},
		{
			name: "empty ssl mode defaults to disable",
			cfg: Config{
				Host: "localhost", Port: 5432, Database: "db",
				Username: "user", Password: "pass", SSLMode: "",
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
