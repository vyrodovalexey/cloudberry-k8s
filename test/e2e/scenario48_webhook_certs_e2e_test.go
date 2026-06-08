//go:build e2e

package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/certmanager"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// Scenario48WebhookCertsE2ESuite tests Scenario 48: Webhook Certificate Management end-to-end.
type Scenario48WebhookCertsE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario48(t *testing.T) {
	suite.Run(t, new(Scenario48WebhookCertsE2ESuite))
}

// TestE2E_Scenario48_SelfSignedCertGeneration tests self-signed certificate
// generation and K8s Secret creation end-to-end.
func (s *Scenario48WebhookCertsE2ESuite) TestE2E_Scenario48_SelfSignedCertGeneration() {
	s.logger.Info("starting scenario 48 E2E: self-signed cert generation")

	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)
	k8sClient := fake.NewClientBuilder().WithScheme(k8sScheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "cloudberry-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "e2e-s48-self-signed-certs",
		SecretNamespace:      "cloudberry-system",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle)

	// Verify the CA bundle is valid PEM.
	block, _ := pem.Decode(caBundle)
	require.NotNil(s.T(), block, "CA bundle should be valid PEM")

	caCert, parseErr := x509.ParseCertificate(block.Bytes)
	require.NoError(s.T(), parseErr)
	assert.True(s.T(), caCert.IsCA, "CA certificate should have IsCA=true")

	// Verify the secret was created with all required keys.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "e2e-s48-self-signed-certs",
		Namespace: "cloudberry-system",
	}, secret)
	require.NoError(s.T(), getErr)
	assert.NotEmpty(s.T(), secret.Data["ca.crt"], "secret should contain ca.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.crt"], "secret should contain tls.crt")
	assert.NotEmpty(s.T(), secret.Data["tls.key"], "secret should contain tls.key")
	assert.Equal(s.T(), corev1.SecretTypeTLS, secret.Type,
		"secret should be of type TLS")

	// Verify the server certificate has the expected DNS names.
	serverBlock, _ := pem.Decode(secret.Data["tls.crt"])
	require.NotNil(s.T(), serverBlock, "server cert should be valid PEM")
	serverCert, parseErr := x509.ParseCertificate(serverBlock.Bytes)
	require.NoError(s.T(), parseErr)
	assert.Contains(s.T(), serverCert.DNSNames, "cloudberry-webhook.cloudberry-system.svc",
		"server cert should contain the service DNS name")
	assert.Contains(s.T(), serverCert.DNSNames, "cloudberry-webhook.cloudberry-system.svc.cluster.local",
		"server cert should contain the cluster-local DNS name")

	s.logger.Info("scenario 48 E2E: self-signed cert generation completed")
}

// TestE2E_Scenario48_CertRotationDetection tests that the certmanager detects
// near-expiry certificates and triggers rotation.
func (s *Scenario48WebhookCertsE2ESuite) TestE2E_Scenario48_CertRotationDetection() {
	s.logger.Info("starting scenario 48 E2E: cert rotation detection")

	k8sScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(k8sScheme)

	// Generate a near-expiry certificate (past 2/3 threshold).
	nearExpiryCert, nearExpiryKey := e2eScenario48GenerateNearExpiryCert(s.T())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-s48-rotation-certs",
			Namespace: "cloudberry-system",
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
		ServiceName:          "cloudberry-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "e2e-s48-rotation-certs",
		SecretNamespace:      "cloudberry-system",
		CertSource:           "self-signed",
		CertValidityDuration: 365 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	// Should need rotation.
	needsRotation, err := cm.NeedsRotation(s.ctx)
	require.NoError(s.T(), err)
	assert.True(s.T(), needsRotation,
		"certificate past 2/3 lifetime should need rotation")

	// EnsureCertificates should regenerate.
	caBundle, ensureErr := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), ensureErr)
	require.NotEmpty(s.T(), caBundle)

	// After regeneration, should NOT need rotation.
	needsRotation2, rotErr := cm.NeedsRotation(s.ctx)
	require.NoError(s.T(), rotErr)
	assert.False(s.T(), needsRotation2,
		"freshly regenerated certificate should not need rotation")

	s.logger.Info("scenario 48 E2E: cert rotation detection completed")
}

// TestE2E_Scenario48_WebhookCertCasesCatalog runs the webhook cert cases catalog.
func (s *Scenario48WebhookCertsE2ESuite) TestE2E_Scenario48_WebhookCertCasesCatalog() {
	s.logger.Info("starting scenario 48 E2E: webhook cert cases catalog")

	testCases := cases.WebhookCertCases()
	require.Len(s.T(), testCases, 3, "should have 3 webhook cert test cases")

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			assert.NotEmpty(t, tc.Name, "test case should have a name")
			assert.NotEmpty(t, tc.CertSource, "test case should have a cert source")
			assert.NotEmpty(t, tc.Description, "test case should have a description")
			assert.True(t, tc.ExpectCABundle,
				"[%s] should expect a CA bundle", tc.Name)
			assert.True(t, tc.ExpectSecret,
				"[%s] should expect a K8s Secret", tc.Name)

			// Verify the cert source is valid.
			validSources := map[string]bool{"vault-pki": true, "self-signed": true}
			assert.True(t, validSources[tc.CertSource],
				"[%s] cert source should be vault-pki or self-signed, got %q", tc.Name, tc.CertSource)
		})
	}

	s.logger.Info("scenario 48 E2E: webhook cert cases catalog completed")
}

// TestE2E_Scenario48_ClusterCRWithWebhookConfig creates a cluster CR and
// verifies it is accepted by the fake K8s client.
func (s *Scenario48WebhookCertsE2ESuite) TestE2E_Scenario48_ClusterCRWithWebhookConfig() {
	s.logger.Info("starting scenario 48 E2E: cluster CR with webhook config")

	cluster := testutil.NewClusterBuilder("e2e-s48-webhook", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	// Verify the cluster is valid.
	require.NotNil(s.T(), cluster)
	assert.Equal(s.T(), "e2e-s48-webhook", cluster.Name)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, cluster.Status.Phase)

	// Create the cluster in the fake K8s env and verify it persists.
	env := testutil.NewTestK8sEnv(cluster)
	retrieved, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cluster.Name, retrieved.Name)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, retrieved.Status.Phase)

	s.logger.Info("scenario 48 E2E: cluster CR with webhook config completed")
}

// e2eScenario48GenerateNearExpiryCert generates a self-signed certificate that
// is past the 2/3 rotation threshold for E2E testing.
func e2eScenario48GenerateNearExpiryCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	// Certificate started 10 days ago, expires in 2 days.
	// Total lifetime = 12 days, 2/3 threshold = 8 days from start.
	// Current time is 10 days from start, past the threshold.
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"test"},
			CommonName:   "e2e-s48-near-expiry",
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
