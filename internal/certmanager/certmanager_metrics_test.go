package certmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// capturingRecorder captures certificate-related metric invocations. It embeds
// metrics.NoopRecorder so only the relevant methods are overridden.
type capturingRecorder struct {
	*metrics.NoopRecorder

	mu sync.Mutex

	rotations       int
	lastComponent   string
	lastSource      string
	lastResult      string
	expirySets      int
	expiryComponent string
	expirySeconds   float64
}

func newCapturingRecorder() *capturingRecorder {
	return &capturingRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (c *capturingRecorder) RecordCertRotation(component, source, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotations++
	c.lastComponent = component
	c.lastSource = source
	c.lastResult = result
}

func (c *capturingRecorder) SetCertExpirySeconds(component string, seconds float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expirySets++
	c.expiryComponent = component
	c.expirySeconds = seconds
}

// ============================================================================
// certSource
// ============================================================================

func TestCertSource(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		expected string
	}{
		{"vault-pki", CertSourceVaultPKI, CertSourceVaultPKI},
		{"self-signed", CertSourceSelfSigned, CertSourceSelfSigned},
		{"empty defaults to self-signed", "", CertSourceSelfSigned},
		{"unknown defaults to self-signed", "weird", CertSourceSelfSigned},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &certManager{config: Config{CertSource: tt.source}}
			assert.Equal(t, tt.expected, m.certSource())
		})
	}
}

// ============================================================================
// recordCertRotation
// ============================================================================

func TestRecordCertRotation_NilRecorder(t *testing.T) {
	m := &certManager{config: Config{CertSource: CertSourceSelfSigned}}
	// No recorder: must be a no-op and not panic.
	m.recordCertRotation(resultSuccess)
}

func TestRecordCertRotation_RecordsMetric(t *testing.T) {
	rec := newCapturingRecorder()
	m := &certManager{config: Config{CertSource: CertSourceVaultPKI}, recorder: rec}

	m.recordCertRotation(resultError)

	assert.Equal(t, 1, rec.rotations)
	assert.Equal(t, certComponent, rec.lastComponent)
	assert.Equal(t, CertSourceVaultPKI, rec.lastSource)
	assert.Equal(t, resultError, rec.lastResult)
}

// ============================================================================
// setCertExpiry
// ============================================================================

func TestSetCertExpiry_NilRecorder(t *testing.T) {
	m := &certManager{}
	// No recorder: must be a no-op and not panic.
	m.setCertExpiry([]byte("anything"))
}

func TestSetCertExpiry_EmptyCert(t *testing.T) {
	rec := newCapturingRecorder()
	m := &certManager{recorder: rec}
	m.setCertExpiry(nil)
	assert.Equal(t, 0, rec.expirySets)
}

func TestSetCertExpiry_InvalidPEM(t *testing.T) {
	rec := newCapturingRecorder()
	m := &certManager{recorder: rec}
	m.setCertExpiry([]byte("not-a-pem-block"))
	assert.Equal(t, 0, rec.expirySets)
}

func TestSetCertExpiry_InvalidDER(t *testing.T) {
	rec := newCapturingRecorder()
	m := &certManager{recorder: rec}
	// Valid PEM block but the DER bytes are not a parseable certificate.
	_, expiredKey := generateExpiredCert(t)
	_ = expiredKey
	m.setCertExpiry([]byte("-----BEGIN CERTIFICATE-----\nbm90LXZhbGlkLWRlcg==\n-----END CERTIFICATE-----\n"))
	assert.Equal(t, 0, rec.expirySets)
}

func TestSetCertExpiry_ValidCert(t *testing.T) {
	rec := newCapturingRecorder()
	m := &certManager{recorder: rec}

	_, tlsCert, _, err := generateSelfSignedCert([]string{"test.svc"}, 365*24*time.Hour)
	require.NoError(t, err)

	m.setCertExpiry(tlsCert)
	assert.Equal(t, 1, rec.expirySets)
	assert.Equal(t, certComponent, rec.expiryComponent)
	assert.Positive(t, rec.expirySeconds)
}

// ============================================================================
// End-to-end metric recording through EnsureCertificates
// ============================================================================

func TestEnsureCertificates_RecordsMetrics_SelfSigned(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig() // self-signed source

	rec := newCapturingRecorder()
	cm := New(k8sClient, nil, cfg, nil, rec)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)

	// A successful rotation was recorded for the self-signed source.
	assert.Equal(t, 1, rec.rotations)
	assert.Equal(t, CertSourceSelfSigned, rec.lastSource)
	assert.Equal(t, resultSuccess, rec.lastResult)
	// The expiry gauge was refreshed from the freshly generated cert.
	assert.Equal(t, 1, rec.expirySets)
	assert.Positive(t, rec.expirySeconds)
}

func TestEnsureCertificates_RecordsMetrics_VaultPKI(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()
	cfg.CertSource = CertSourceVaultPKI

	// Use a self-signed cert as the vault-issued cert so setCertExpiry can parse it.
	_, tlsCert, tlsKey, err := generateSelfSignedCert([]string{"test.svc"}, 365*24*time.Hour)
	require.NoError(t, err)
	caCert, _, _, err := generateSelfSignedCert([]string{"ca"}, 365*24*time.Hour)
	require.NoError(t, err)

	mockVault := &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": string(tlsCert),
			"private_key": string(tlsKey),
			"issuing_ca":  string(caCert),
		},
	}

	rec := newCapturingRecorder()
	cm := New(k8sClient, mockVault, cfg, nil, rec)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, caBundle)

	assert.Equal(t, 1, rec.rotations)
	assert.Equal(t, CertSourceVaultPKI, rec.lastSource)
	assert.Equal(t, resultSuccess, rec.lastResult)
	// vault-issued cert is a valid parseable cert, so expiry is set.
	assert.Equal(t, 1, rec.expirySets)
	assert.Positive(t, rec.expirySeconds)
}

func TestEnsureCertificates_RecordsErrorMetric(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cfg := newTestConfig()
	cfg.CertSource = CertSourceVaultPKI

	// Vault write error forces certificate generation to fail.
	mockVault := &mockVaultClient{enabled: false}

	rec := newCapturingRecorder()
	cm := New(k8sClient, mockVault, cfg, nil, rec)

	_, err := cm.EnsureCertificates(context.Background())
	require.Error(t, err)

	// An error rotation metric was recorded.
	assert.Equal(t, 1, rec.rotations)
	assert.Equal(t, CertSourceVaultPKI, rec.lastSource)
	assert.Equal(t, resultError, rec.lastResult)
}

func TestEnsureCertificates_ExistingValid_RefreshesExpiry(t *testing.T) {
	scheme := newTestScheme()
	cfg := newTestConfig()

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
	rec := newCapturingRecorder()
	cm := New(k8sClient, nil, cfg, nil, rec)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)
	assert.Equal(t, caCert, caBundle)

	// No rotation occurred (cert still valid), but the expiry gauge is refreshed.
	assert.Equal(t, 0, rec.rotations)
	assert.Equal(t, 1, rec.expirySets)
	assert.Positive(t, rec.expirySeconds)
}
