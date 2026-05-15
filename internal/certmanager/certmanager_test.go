package certmanager

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	fakeinterceptor "sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func newTestConfig() Config {
	return Config{
		ServiceName:          "test-webhook",
		ServiceNamespace:     "test-ns",
		SecretName:           "test-webhook-certs",
		SecretNamespace:      "test-ns",
		CertSource:           CertSourceSelfSigned,
		CertValidityDuration: 365 * 24 * time.Hour,
	}
}

func TestNew(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()

	cm := New(k8sClient, nil, cfg, nil)
	require.NotNil(t, cm)
}

func TestEnsureCertificates_CreateNew(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()

	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)

	// Verify the secret was created.
	secret := &corev1.Secret{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      cfg.SecretName,
		Namespace: cfg.SecretNamespace,
	}, secret)
	require.NoError(t, err)
	assert.NotEmpty(t, secret.Data[secretKeyCACert])
	assert.NotEmpty(t, secret.Data[secretKeyTLSCert])
	assert.NotEmpty(t, secret.Data[secretKeyTLSKey])
}

func TestEnsureCertificates_ExistingValid(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	// Generate valid certs and pre-create the secret.
	caCert, tlsCert, tlsKey, err := generateSelfSignedCert(
		[]string{"test-webhook.test-ns.svc", "test-webhook.test-ns.svc.cluster.local"},
		365*24*time.Hour,
	)
	require.NoError(t, err)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			secretKeyCACert:  caCert,
			secretKeyTLSCert: tlsCert,
			secretKeyTLSKey:  tlsKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	assert.Equal(t, caCert, caBundle)
}

func TestEnsureCertificates_ExistingExpired(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	// Generate an expired certificate.
	expiredCert, expiredKey := generateExpiredCert(t)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			secretKeyCACert:  expiredCert,
			secretKeyTLSCert: expiredCert,
			secretKeyTLSKey:  expiredKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)
	// Should have generated new certs (different from expired).
	assert.NotEqual(t, expiredCert, caBundle)
}

func TestNeedsRotation_NoSecret(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()

	cm := New(k8sClient, nil, cfg, nil)

	needs, err := cm.NeedsRotation(context.Background())
	require.NoError(t, err)
	assert.True(t, needs)
}

func TestNeedsRotation_ValidCert(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	caCert, tlsCert, tlsKey, err := generateSelfSignedCert(
		[]string{"test-webhook.test-ns.svc"},
		365*24*time.Hour,
	)
	require.NoError(t, err)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			secretKeyCACert:  caCert,
			secretKeyTLSCert: tlsCert,
			secretKeyTLSKey:  tlsKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	needs, err := cm.NeedsRotation(context.Background())
	require.NoError(t, err)
	assert.False(t, needs)
}

func TestNeedsRotation_ExpiredCert(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	expiredCert, expiredKey := generateExpiredCert(t)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			secretKeyCACert:  expiredCert,
			secretKeyTLSCert: expiredCert,
			secretKeyTLSKey:  expiredKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	needs, err := cm.NeedsRotation(context.Background())
	require.NoError(t, err)
	assert.True(t, needs)
}

func TestCheckCertRotation_EmptyData(t *testing.T) {
	cm := &certManager{}
	secret := &corev1.Secret{
		Data: map[string][]byte{},
	}

	needs, err := cm.checkCertRotation(secret)
	require.NoError(t, err)
	assert.True(t, needs)
}

func TestCheckCertRotation_InvalidPEM(t *testing.T) {
	cm := &certManager{}
	secret := &corev1.Secret{
		Data: map[string][]byte{
			secretKeyTLSCert: []byte("not-valid-pem"),
		},
	}

	needs, err := cm.checkCertRotation(secret)
	require.Error(t, err)
	assert.True(t, needs)
}

func TestDNSNames(t *testing.T) {
	cm := &certManager{
		config: Config{
			ServiceName:      "my-webhook",
			ServiceNamespace: "my-ns",
		},
	}

	names := cm.dnsNames()
	assert.Equal(t, []string{
		"my-webhook.my-ns.svc",
		"my-webhook.my-ns.svc.cluster.local",
	}, names)
}

func TestEnsureCertificates_UnsupportedSource(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()
	cfg.CertSource = "unsupported"

	cm := New(k8sClient, nil, cfg, nil)

	_, err := cm.EnsureCertificates(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported cert source")
}

func TestEnsureCertificates_DefaultValidity(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()
	cfg.CertValidityDuration = 0 // Should default to 1 year.

	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)
}

func TestEnsureCertificates_VaultPKISource(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()
	cfg.CertSource = CertSourceVaultPKI

	// Use a mock vault client that returns valid cert data.
	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "-----BEGIN CERTIFICATE-----\nMIIBtest\n-----END CERTIFICATE-----",
			"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEtest\n-----END RSA PRIVATE KEY-----",
			"issuing_ca":  "-----BEGIN CERTIFICATE-----\nMIIBca\n-----END CERTIFICATE-----",
		},
	}

	cm := New(k8sClient, mockVault, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)
	assert.Equal(t, []byte("-----BEGIN CERTIFICATE-----\nMIIBca\n-----END CERTIFICATE-----"), caBundle)
}

func TestEnsureCertificates_UpdateExistingSecret(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	// Pre-create a secret with invalid/expired cert data to trigger rotation.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			secretKeyCACert:  []byte("invalid-pem"),
			secretKeyTLSCert: []byte("invalid-pem"),
			secretKeyTLSKey:  []byte("invalid-key"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)

	// Verify the secret was updated with new certs.
	updated := &corev1.Secret{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      cfg.SecretName,
		Namespace: cfg.SecretNamespace,
	}, updated)
	require.NoError(t, err)
	assert.NotEqual(t, []byte("invalid-pem"), updated.Data[secretKeyTLSCert])
}

func TestIssueVaultPKICert_NilClient(t *testing.T) {
	cfg := newTestConfig()
	_, _, _, err := issueVaultPKICert(context.Background(), nil, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault client is not enabled")
}

func TestIssueVaultPKICert_ClientNotEnabled(t *testing.T) {
	cfg := newTestConfig()
	mockVault := &mockVaultClient{enabled: false}
	_, _, _, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault client is not enabled")
}

func TestIssueVaultPKICert_DefaultMountPathAndRole(t *testing.T) {
	cfg := newTestConfig()
	cfg.VaultPKIMountPath = ""
	cfg.VaultPKIRole = ""

	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "cert-pem",
			"private_key": "key-pem",
			"issuing_ca":  "ca-pem",
		},
	}

	ca, cert, key, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []byte("ca-pem"), ca)
	assert.Equal(t, []byte("cert-pem"), cert)
	assert.Equal(t, []byte("key-pem"), key)
}

func TestIssueVaultPKICert_CustomMountPathAndRole(t *testing.T) {
	cfg := newTestConfig()
	cfg.VaultPKIMountPath = "custom-pki"
	cfg.VaultPKIRole = "custom-role"

	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "cert-pem",
			"private_key": "key-pem",
			"issuing_ca":  "ca-pem",
		},
	}

	ca, cert, key, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc", "test.svc.cluster.local"}, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []byte("ca-pem"), ca)
	assert.Equal(t, []byte("cert-pem"), cert)
	assert.Equal(t, []byte("key-pem"), key)
}

func TestIssueVaultPKICert_WriteError(t *testing.T) {
	cfg := newTestConfig()
	mockVault := &mockVaultClient{
		enabled:  true,
		writeErr: fmt.Errorf("vault write failed"),
	}

	_, _, _, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuing certificate via vault PKI")
}

func TestIssueVaultPKICert_MissingCertificateField(t *testing.T) {
	cfg := newTestConfig()
	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"private_key": "key-pem",
			"issuing_ca":  "ca-pem",
		},
	}

	_, _, _, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'certificate' field")
}

func TestIssueVaultPKICert_MissingPrivateKeyField(t *testing.T) {
	cfg := newTestConfig()
	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "cert-pem",
			"issuing_ca":  "ca-pem",
		},
	}

	_, _, _, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'private_key' field")
}

func TestIssueVaultPKICert_MissingIssuingCAField(t *testing.T) {
	cfg := newTestConfig()
	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "cert-pem",
			"private_key": "key-pem",
		},
	}

	_, _, _, err := issueVaultPKICert(context.Background(), mockVault, cfg, []string{"test.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'issuing_ca' field")
}

func TestNeedsRotation_GetError(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	// Create a certmanager with a config pointing to a secret that doesn't exist
	// but inject a Get error via interceptor
	interceptClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(fakeinterceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).
		Build()
	_ = k8sClient // unused, we use interceptClient

	cm := New(interceptClient, nil, cfg, nil)

	_, err := cm.NeedsRotation(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting cert secret")
}

// mockVaultClient implements vault.Client for testing.
type mockVaultClient struct {
	enabled   bool
	writeData map[string]interface{}
	writeErr  error
}

func (m *mockVaultClient) ReadSecret(_ context.Context, _ string) (map[string]interface{}, error) {
	return nil, nil
}

func (m *mockVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return m.writeErr
}

func (m *mockVaultClient) WriteSecretWithResponse(_ context.Context, _ string, _ map[string]interface{}) (map[string]interface{}, error) {
	return m.writeData, m.writeErr
}

func (m *mockVaultClient) IsEnabled() bool {
	return m.enabled
}

// generateExpiredCert generates a self-signed certificate that has already expired.
func generateExpiredCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialNumberBitSize))
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"test"},
			CommonName:   "test-expired",
		},
		NotBefore:             time.Now().Add(-48 * time.Hour),
		NotAfter:              time.Now().Add(-24 * time.Hour), // Expired 24 hours ago.
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}
