//go:build functional

package functional

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/certmanager"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// Scenario48WebhookCertsSuite tests Scenario 48: Webhook Certificate Management.
type Scenario48WebhookCertsSuite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario48(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario48WebhookCertsSuite))
}

func (s *Scenario48WebhookCertsSuite) SetupTest() {
	s.ctx = context.Background()
}

// TestFunctional_Scenario48a_VaultPKI_CertIssuance tests that the certmanager
// issues certificates via a mock Vault PKI server when cert source is vault-pki.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48a_VaultPKI_CertIssuance() {
	// Create a mock Vault PKI server that returns certificate data.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pki/issue/cloudberry-operator", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut && r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify expected fields are present.
		commonName, _ := body["common_name"].(string)
		assert.NotEmpty(s.T(), commonName, "common_name should be provided")

		altNames, _ := body["alt_names"].(string)
		assert.NotEmpty(s.T(), altNames, "alt_names should be provided")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"certificate": "-----BEGIN CERTIFICATE-----\nMOCK-SERVER-CERT\n-----END CERTIFICATE-----",
				"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMOCK-SERVER-KEY\n-----END RSA PRIVATE KEY-----",
				"issuing_ca":  "-----BEGIN CERTIFICATE-----\nMOCK-CA-CERT\n-----END CERTIFICATE-----",
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create a real vault client pointing at the mock server.
	vaultCfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	vaultClient, err := vault.NewClient(s.ctx, vaultCfg, nil)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), vaultClient)
	assert.True(s.T(), vaultClient.IsEnabled(), "vault client should be enabled")

	// Create a fake K8s client and certmanager with vault-pki source.
	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)
	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "test-webhook",
		ServiceNamespace:     "test-ns",
		SecretName:           "test-vault-pki-certs",
		SecretNamespace:      "test-ns",
		CertSource:           "vault-pki",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, vaultClient, cfg, nil)

	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle, "CA bundle should not be empty")
	assert.Contains(s.T(), string(caBundle), "CERTIFICATE",
		"CA bundle should contain certificate PEM data")

	// Verify the secret was created with all required keys.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "test-vault-pki-certs",
		Namespace: "test-ns",
	}, secret)
	require.NoError(s.T(), getErr)
	assert.NotEmpty(s.T(), secret.Data["ca.crt"], "secret should contain ca.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.crt"], "secret should contain tls.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.key"], "secret should contain tls.key")
}

// TestFunctional_Scenario48b_SelfSigned_CertGeneration tests that the certmanager
// generates self-signed certificates when cert source is self-signed.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48b_SelfSigned_CertGeneration() {
	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)
	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "test-webhook",
		ServiceNamespace:     "test-ns",
		SecretName:           "test-self-signed-certs",
		SecretNamespace:      "test-ns",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle, "CA bundle should not be empty")

	// Verify the CA bundle is valid PEM.
	block, _ := pem.Decode(caBundle)
	require.NotNil(s.T(), block, "CA bundle should be valid PEM")
	assert.Equal(s.T(), "CERTIFICATE", block.Type, "CA bundle should be a CERTIFICATE PEM block")

	// Parse the CA certificate.
	caCert, parseErr := x509.ParseCertificate(block.Bytes)
	require.NoError(s.T(), parseErr, "CA certificate should be parseable")
	assert.True(s.T(), caCert.IsCA, "CA certificate should have IsCA=true")
	assert.Contains(s.T(), caCert.Subject.Organization, "cloudberry-operator",
		"CA certificate should have the correct organization")

	// Verify the secret was created with all required keys.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "test-self-signed-certs",
		Namespace: "test-ns",
	}, secret)
	require.NoError(s.T(), getErr)
	assert.NotEmpty(s.T(), secret.Data["ca.crt"], "secret should contain ca.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.crt"], "secret should contain tls.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.key"], "secret should contain tls.key")

	// Verify the server certificate has the expected DNS names.
	serverBlock, _ := pem.Decode(secret.Data["tls.crt"])
	require.NotNil(s.T(), serverBlock, "server cert should be valid PEM")
	serverCert, parseErr := x509.ParseCertificate(serverBlock.Bytes)
	require.NoError(s.T(), parseErr)
	assert.Contains(s.T(), serverCert.DNSNames, "test-webhook.test-ns.svc",
		"server cert should contain the service DNS name")
	assert.Contains(s.T(), serverCert.DNSNames, "test-webhook.test-ns.svc.cluster.local",
		"server cert should contain the cluster-local DNS name")
}

// TestFunctional_Scenario48_CertRotation tests that the certmanager detects
// near-expiry certificates and triggers rotation.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48_CertRotation() {
	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)

	// Generate a certificate that is past the 2/3 rotation threshold.
	nearExpiryCert, nearExpiryKey := scenario48GenerateNearExpiryCert(s.T())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rotation-certs",
			Namespace: "test-ns",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"ca.crt":  nearExpiryCert,
			"tls.crt": nearExpiryCert,
			"tls.key": nearExpiryKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(secret).Build()

	cfg := certmanager.Config{
		ServiceName:          "test-webhook",
		ServiceNamespace:     "test-ns",
		SecretName:           "test-rotation-certs",
		SecretNamespace:      "test-ns",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	// The certificate should need rotation since it's past the 2/3 threshold.
	needsRotation, err := cm.NeedsRotation(s.ctx)
	require.NoError(s.T(), err)
	assert.True(s.T(), needsRotation,
		"certificate past 2/3 lifetime should need rotation")

	// EnsureCertificates should regenerate the certificate.
	caBundle, ensureErr := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), ensureErr)
	require.NotEmpty(s.T(), caBundle)

	// After regeneration, should NOT need rotation.
	needsRotation2, rotErr := cm.NeedsRotation(s.ctx)
	require.NoError(s.T(), rotErr)
	assert.False(s.T(), needsRotation2,
		"freshly regenerated certificate should not need rotation")
}

// TestFunctional_Scenario48_HelmAutoGeneration tests that the default cert
// secret name and service name are used when not explicitly configured.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48_HelmAutoGeneration() {
	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)
	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).Build()

	// Use default names matching Helm chart defaults.
	cfg := certmanager.Config{
		ServiceName:          "cloudberry-operator-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "cloudberry-operator-webhook-certs",
		SecretNamespace:      "cloudberry-system",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle)

	// Verify the secret was created with the default name.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "cloudberry-operator-webhook-certs",
		Namespace: "cloudberry-system",
	}, secret)
	require.NoError(s.T(), getErr, "secret with default Helm name should be created")
	assert.Equal(s.T(), corev1.SecretTypeTLS, secret.Type,
		"secret should be of type TLS")

	// Verify the server certificate DNS names match the default service name.
	serverBlock, _ := pem.Decode(secret.Data["tls.crt"])
	require.NotNil(s.T(), serverBlock, "server cert should be valid PEM")
	serverCert, parseErr := x509.ParseCertificate(serverBlock.Bytes)
	require.NoError(s.T(), parseErr)
	assert.Contains(s.T(), serverCert.DNSNames,
		"cloudberry-operator-webhook.cloudberry-system.svc",
		"server cert should contain the default service DNS name")
	assert.Contains(s.T(), serverCert.DNSNames,
		"cloudberry-operator-webhook.cloudberry-system.svc.cluster.local",
		"server cert should contain the default cluster-local DNS name")
}

// TestFunctional_Scenario48_DNSSANs tests that dnsNames() returns both
// .svc and .svc.cluster.local DNS names.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48_DNSSANs() {
	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)
	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "my-webhook-svc",
		ServiceNamespace:     "my-namespace",
		SecretName:           "my-webhook-certs",
		SecretNamespace:      "my-namespace",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle)

	// Verify the server certificate has both DNS SAN formats.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "my-webhook-certs",
		Namespace: "my-namespace",
	}, secret)
	require.NoError(s.T(), getErr)

	serverBlock, _ := pem.Decode(secret.Data["tls.crt"])
	require.NotNil(s.T(), serverBlock, "server cert should be valid PEM")
	serverCert, parseErr := x509.ParseCertificate(serverBlock.Bytes)
	require.NoError(s.T(), parseErr)

	expectedDNSNames := []string{
		"my-webhook-svc.my-namespace.svc",
		"my-webhook-svc.my-namespace.svc.cluster.local",
	}
	assert.Equal(s.T(), expectedDNSNames, serverCert.DNSNames,
		"server cert should contain exactly the .svc and .svc.cluster.local DNS names")
}

// TestFunctional_Scenario48_WebhookCertCases runs the webhook cert cases catalog.
func (s *Scenario48WebhookCertsSuite) TestFunctional_Scenario48_WebhookCertCases() {
	testCases := cases.WebhookCertCases()
	require.Len(s.T(), testCases, 3, "should have 3 webhook cert test cases")

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			assert.NotEmpty(t, tc.Name, "test case should have a name")
			assert.NotEmpty(t, tc.CertSource, "test case should have a cert source")
			assert.NotEmpty(t, tc.Description, "test case should have a description")
			assert.True(t, tc.ExpectCABundle, "all webhook cert cases should expect a CA bundle")
			assert.True(t, tc.ExpectSecret, "all webhook cert cases should expect a K8s Secret")

			// Verify the cert source is valid.
			validSources := map[string]bool{"vault-pki": true, "self-signed": true}
			assert.True(t, validSources[tc.CertSource],
				"cert source should be vault-pki or self-signed, got %q", tc.CertSource)
		})
	}
}

// scenario48GenerateNearExpiryCert generates a self-signed certificate that is
// past the 2/3 rotation threshold (i.e., more than 2/3 of its lifetime has elapsed).
func scenario48GenerateNearExpiryCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	// Create a certificate that started 10 days ago and expires in 2 days.
	// Total lifetime = 12 days, 2/3 threshold = 8 days from start.
	// Current time is 10 days from start, which is past the 2/3 threshold.
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"test"},
			CommonName:   "test-near-expiry",
		},
		NotBefore:             time.Now().Add(-10 * 24 * time.Hour),
		NotAfter:              time.Now().Add(2 * 24 * time.Hour),
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

// Compile-time interface check.
var _ vault.Client = (*scenario48MockVaultClient)(nil)

// scenario48MockVaultClient implements vault.Client for testing.
type scenario48MockVaultClient struct {
	writeData map[string]interface{}
	writeErr  error
	enabled   bool
}

func (m *scenario48MockVaultClient) ReadSecret(
	_ context.Context, _ string,
) (map[string]interface{}, error) {
	return nil, nil
}

func (m *scenario48MockVaultClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return m.writeErr
}

func (m *scenario48MockVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return m.writeData, m.writeErr
}

func (m *scenario48MockVaultClient) IsEnabled() bool {
	return m.enabled
}
