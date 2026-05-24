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
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/certmanager"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// Scenario47SSLSuite tests Scenario 47: SSL/TLS Configuration.
type Scenario47SSLSuite struct {
	suite.Suite
	builder *builder.DefaultBuilder
	ctx     context.Context
}

func TestFunctional_Scenario47(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario47SSLSuite))
}

func (s *Scenario47SSLSuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// reqFor builds a reconcile request for the given cluster.
func (s *Scenario47SSLSuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// getPostgresqlConf builds the postgresql.conf ConfigMap using the builder
// and returns its content.
func (s *Scenario47SSLSuite) getPostgresqlConf(
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	s.T().Helper()

	cm := s.builder.BuildPostgresqlConfConfigMap(cluster)
	content, ok := cm.Data["postgresql.conf"]
	require.True(s.T(), ok, "postgresql.conf key must exist in ConfigMap")
	return content
}

// buildSSLCluster creates a running cluster with SSL configuration.
func buildSSLCluster(name string, sslEnabled bool, certSecretName, minTLS string) *cbv1alpha1.CloudberryCluster {
	b := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSSL(sslEnabled, certSecretName)

	if minTLS != "" {
		b = b.WithSSLMinTLSVersion(minTLS)
	}

	return b.Build()
}

// --- 47a Tests: SSL Configuration in postgresql.conf and StatefulSet ---

// TestFunctional_Scenario47a_SSLEnabled_PostgresqlConf verifies that when SSL is
// enabled, postgresql.conf contains all required SSL settings.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLEnabled_PostgresqlConf() {
	cluster := buildSSLCluster("s47a-ssl-conf", true, "cloudberry-tls", "1.2")
	content := s.getPostgresqlConf(cluster)

	expectedLines := []string{
		"ssl = on",
		"ssl_cert_file = '/tls/tls.crt'",
		"ssl_key_file = '/tls/tls.key'",
		"ssl_ca_file = '/tls/ca.crt'",
		"ssl_min_protocol_version = 'TLSv1.2'",
	}

	for _, expected := range expectedLines {
		assert.Contains(s.T(), content, expected,
			"postgresql.conf should contain SSL setting: %s", expected)
	}
}

// TestFunctional_Scenario47a_SSLEnabled_TLSVolume verifies that when SSL is
// enabled with a cert secret, the StatefulSet has a TLS volume and mount.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLEnabled_TLSVolume() {
	cluster := buildSSLCluster("s47a-tls-vol", true, "cloudberry-tls", "1.2")

	sts, err := s.builder.BuildCoordinatorStatefulSet(cluster)
	require.NoError(s.T(), err)

	// Verify TLS secret source volume exists (tls-secret).
	var foundSecretVol bool
	for _, vol := range sts.Spec.Template.Spec.Volumes {
		if vol.Name == "tls-secret" {
			foundSecretVol = true
			require.NotNil(s.T(), vol.VolumeSource.Secret,
				"tls-secret volume should be sourced from a Secret")
			assert.Equal(s.T(), "cloudberry-tls", vol.VolumeSource.Secret.SecretName,
				"tls-secret volume should reference the cert secret")
			break
		}
	}
	assert.True(s.T(), foundSecretVol, "StatefulSet should have a 'tls-secret' volume")

	// Verify TLS emptyDir target volume exists (tls).
	var foundTLSVol bool
	for _, vol := range sts.Spec.Template.Spec.Volumes {
		if vol.Name == "tls" {
			foundTLSVol = true
			require.NotNil(s.T(), vol.VolumeSource.EmptyDir,
				"tls volume should be an EmptyDir (init container copies certs with correct perms)")
			break
		}
	}
	assert.True(s.T(), foundTLSVol, "StatefulSet should have a 'tls' emptyDir volume")

	// Verify TLS init container exists.
	var foundInitTLS bool
	for _, ic := range sts.Spec.Template.Spec.InitContainers {
		if ic.Name == "init-tls" {
			foundInitTLS = true
			break
		}
	}
	assert.True(s.T(), foundInitTLS, "StatefulSet should have an 'init-tls' init container")

	// Verify TLS volume mount exists on the main container.
	mainContainer := sts.Spec.Template.Spec.Containers[0]
	var foundMount bool
	for _, mount := range mainContainer.VolumeMounts {
		if mount.Name == "tls" {
			foundMount = true
			assert.Equal(s.T(), "/tls", mount.MountPath,
				"TLS volume should be mounted at /tls")
			assert.True(s.T(), mount.ReadOnly,
				"TLS volume mount should be read-only")
			break
		}
	}
	assert.True(s.T(), foundMount, "main container should have a 'tls' volume mount")
}

// TestFunctional_Scenario47a_SSLEnabled_MinTLS12 verifies that the default
// minTLSVersion produces TLSv1.2 in postgresql.conf.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLEnabled_MinTLS12() {
	cluster := buildSSLCluster("s47a-tls12", true, "cloudberry-tls", "1.2")
	content := s.getPostgresqlConf(cluster)

	assert.Contains(s.T(), content, "ssl_min_protocol_version = 'TLSv1.2'",
		"postgresql.conf should contain TLSv1.2 as minimum protocol version")
}

// TestFunctional_Scenario47a_SSLEnabled_MinTLS13 verifies that minTLSVersion=1.3
// produces TLSv1.3 in postgresql.conf.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLEnabled_MinTLS13() {
	cluster := buildSSLCluster("s47a-tls13", true, "cloudberry-tls", "1.3")
	content := s.getPostgresqlConf(cluster)

	assert.Contains(s.T(), content, "ssl_min_protocol_version = 'TLSv1.3'",
		"postgresql.conf should contain TLSv1.3 as minimum protocol version")
	assert.NotContains(s.T(), content, "ssl_min_protocol_version = 'TLSv1.2'",
		"postgresql.conf should NOT contain TLSv1.2 when 1.3 is specified")
}

// TestFunctional_Scenario47a_SSLDisabled_NoSSLInConf verifies that when SSL is
// disabled, postgresql.conf does not contain any SSL settings.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLDisabled_NoSSLInConf() {
	cluster := buildSSLCluster("s47a-no-ssl", false, "", "")
	content := s.getPostgresqlConf(cluster)

	sslSettings := []string{
		"ssl = on",
		"ssl_cert_file",
		"ssl_key_file",
		"ssl_ca_file",
		"ssl_min_protocol_version",
	}

	for _, setting := range sslSettings {
		assert.NotContains(s.T(), content, setting,
			"postgresql.conf should NOT contain SSL setting when disabled: %s", setting)
	}
}

// TestFunctional_Scenario47a_SSLEnabled_NoCertSecret verifies that when SSL is
// enabled but no certSecret is specified, the TLS volume is not added.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLEnabled_NoCertSecret() {
	cluster := buildSSLCluster("s47a-no-cert", true, "", "1.2")

	sts, err := s.builder.BuildCoordinatorStatefulSet(cluster)
	require.NoError(s.T(), err)

	// Verify no TLS volume exists (since no certSecret is specified).
	for _, vol := range sts.Spec.Template.Spec.Volumes {
		assert.NotEqual(s.T(), "tls", vol.Name,
			"StatefulSet should NOT have a 'tls' volume when certSecret is not specified")
	}

	// However, the TLS volume mount should still be present on the container
	// (the builder adds the mount when SSL is enabled, regardless of certSecret).
	mainContainer := sts.Spec.Template.Spec.Containers[0]
	var foundMount bool
	for _, mount := range mainContainer.VolumeMounts {
		if mount.Name == "tls" {
			foundMount = true
			break
		}
	}
	assert.True(s.T(), foundMount,
		"main container should still have a 'tls' volume mount when SSL is enabled")
}

// TestFunctional_Scenario47a_HostSSLRule verifies that a hostssl HBA rule works
// correctly with SSL enabled.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_HostSSLRule() {
	cluster := testutil.NewClusterBuilder("s47a-hostssl", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSSL(true, "cloudberry-tls").
		WithSSLMinTLSVersion("1.2").
		WithHBARules([]cbv1alpha1.HBARule{
			{
				Type:     cbv1alpha1.HBATypeHostSSL,
				Database: "all",
				User:     "all",
				Address:  "0.0.0.0/0",
				Method:   cbv1alpha1.AuthMethodScramSHA256,
			},
			{
				Type:     cbv1alpha1.HBATypeLocal,
				Database: "all",
				User:     "gpadmin",
				Method:   cbv1alpha1.AuthMethodTrust,
			},
		}).
		Build()

	// Verify pg_hba.conf contains the hostssl rule using the builder.
	hbaCM := s.builder.BuildPgHBAConfConfigMap(cluster)
	hbaContent, ok := hbaCM.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in ConfigMap")
	assert.Contains(s.T(), hbaContent, "hostssl\tall\tall\t0.0.0.0/0\tscram-sha-256",
		"pg_hba.conf should contain the hostssl rule")

	// Verify postgresql.conf has SSL enabled using the builder.
	pgCM := s.builder.BuildPostgresqlConfConfigMap(cluster)
	pgContent, ok := pgCM.Data["postgresql.conf"]
	require.True(s.T(), ok, "postgresql.conf key must exist in ConfigMap")
	assert.Contains(s.T(), pgContent, "ssl = on",
		"postgresql.conf should have SSL enabled when hostssl rules are used")

	// Also verify via the reconciler that pg_hba.conf is created correctly.
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAuthReconciler(
		env.Client, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	reconciledHBA, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	reconciledContent, ok := reconciledHBA.Data["pg_hba.conf"]
	require.True(s.T(), ok, "pg_hba.conf key must exist in reconciled ConfigMap")
	assert.Contains(s.T(), reconciledContent, "hostssl\tall\tall\t0.0.0.0/0\tscram-sha-256",
		"reconciled pg_hba.conf should contain the hostssl rule")
}

// TestFunctional_Scenario47a_SSLConfigCases runs the SSL config cases catalog.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47a_SSLConfigCases() {
	testCases := cases.SSLConfigCases()
	require.Len(s.T(), testCases, 4, "should have 4 SSL config test cases")

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			cluster := buildSSLCluster(
				"s47-case-"+strings.ReplaceAll(tc.Name, "_", "-"),
				tc.SSLEnabled,
				tc.CertSecretName,
				tc.MinTLSVersion,
			)

			// Verify postgresql.conf content.
			cm := s.builder.BuildPostgresqlConfConfigMap(cluster)
			content := cm.Data["postgresql.conf"]

			if tc.ExpectedConfLines != nil {
				for _, expected := range tc.ExpectedConfLines {
					assert.Contains(t, content, expected,
						"[%s] postgresql.conf should contain: %s", tc.Name, expected)
				}
			} else {
				// SSL disabled: verify no SSL settings.
				assert.NotContains(t, content, "ssl = on",
					"[%s] postgresql.conf should NOT contain ssl = on", tc.Name)
			}

			// Verify TLS volume presence.
			sts, err := s.builder.BuildCoordinatorStatefulSet(cluster)
			require.NoError(t, err)

			var hasTLSVolume bool
			for _, vol := range sts.Spec.Template.Spec.Volumes {
				if vol.Name == "tls" {
					hasTLSVolume = true
					break
				}
			}
			assert.Equal(t, tc.ExpectTLSVolume, hasTLSVolume,
				"[%s] TLS volume presence mismatch", tc.Name)
		})
	}
}

// --- 47b Tests: Vault PKI Certificate Issuance and Rotation ---

// TestFunctional_Scenario47b_VaultPKI_CertIssuance tests issuing certificates
// from a mock Vault PKI server.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47b_VaultPKI_CertIssuance() {
	// Create a mock Vault PKI server.
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

	cfg := vault.Config{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "token",
		Token:      "s.test-token",
		RetryOpts:  util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
	}

	client, err := vault.NewClient(s.ctx, cfg, nil)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), client)

	// Issue a certificate via the mock Vault PKI.
	result, writeErr := client.WriteSecretWithResponse(s.ctx, "pki/issue/cloudberry-operator", map[string]interface{}{
		"common_name": "test-webhook.test-ns.svc",
		"alt_names":   "test-webhook.test-ns.svc,test-webhook.test-ns.svc.cluster.local",
		"ttl":         "8760h",
	})
	require.NoError(s.T(), writeErr)
	require.NotNil(s.T(), result)

	// Verify the response contains expected certificate fields.
	certStr, ok := result["certificate"].(string)
	assert.True(s.T(), ok, "response should contain 'certificate' field")
	assert.Contains(s.T(), certStr, "CERTIFICATE", "certificate should be PEM-encoded")

	keyStr, ok := result["private_key"].(string)
	assert.True(s.T(), ok, "response should contain 'private_key' field")
	assert.Contains(s.T(), keyStr, "PRIVATE KEY", "private key should be PEM-encoded")

	caStr, ok := result["issuing_ca"].(string)
	assert.True(s.T(), ok, "response should contain 'issuing_ca' field")
	assert.Contains(s.T(), caStr, "CERTIFICATE", "issuing CA should be PEM-encoded")
}

// TestFunctional_Scenario47b_VaultPKI_CertRotation tests that the certmanager
// detects near-expiry certificates and triggers rotation.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47b_VaultPKI_CertRotation() {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Generate a certificate that is past the 2/3 rotation threshold.
	nearExpiryCert, nearExpiryKey := generateNearExpiryCert(s.T())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-webhook-certs",
			Namespace: "test-ns",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"ca.crt":  nearExpiryCert,
			"tls.crt": nearExpiryCert,
			"tls.key": nearExpiryKey,
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	cfg := certmanager.Config{
		ServiceName:          "test-webhook",
		ServiceNamespace:     "test-ns",
		SecretName:           "test-webhook-certs",
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
}

// TestFunctional_Scenario47b_SelfSigned_CertGeneration tests that the
// self-signed certificate generator produces valid certificates.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47b_SelfSigned_CertGeneration() {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

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

// TestFunctional_Scenario47b_CertManager_EnsureCertificates tests that
// EnsureCertificates creates a K8s Secret with valid certificate data.
func (s *Scenario47SSLSuite) TestFunctional_Scenario47b_CertManager_EnsureCertificates() {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "cloudberry-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "cloudberry-webhook-certs",
		SecretNamespace:      "cloudberry-system",
		CertSource:           "self-signed",
		CertValidityDuration: 90 * 24 * time.Hour,
	}

	cm := certmanager.New(k8sClient, nil, cfg, nil)

	// First call should create the secret.
	caBundle, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), caBundle)

	// Verify the secret exists.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "cloudberry-webhook-certs",
		Namespace: "cloudberry-system",
	}, secret)
	require.NoError(s.T(), getErr)
	assert.Equal(s.T(), corev1.SecretTypeTLS, secret.Type,
		"secret should be of type TLS")

	// Second call should return the same CA bundle without regenerating.
	caBundle2, err := cm.EnsureCertificates(s.ctx)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), caBundle, caBundle2,
		"second call should return the same CA bundle")

	// Verify NeedsRotation returns false for a fresh certificate.
	needsRotation, rotErr := cm.NeedsRotation(s.ctx)
	require.NoError(s.T(), rotErr)
	assert.False(s.T(), needsRotation,
		"freshly generated certificate should not need rotation")
}

// generateNearExpiryCert generates a self-signed certificate that is past the
// 2/3 rotation threshold (i.e., more than 2/3 of its lifetime has elapsed).
func generateNearExpiryCert(t *testing.T) (certPEM, keyPEM []byte) {
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

// Compile-time interface checks.
var (
	_ builder.ResourceBuilder = (*builder.DefaultBuilder)(nil)
	_ vault.Client            = (*scenario47MockVaultClient)(nil)
)

// scenario47MockVaultClient implements vault.Client for testing.
type scenario47MockVaultClient struct {
	writeData map[string]interface{}
	writeErr  error
	enabled   bool
}

func (m *scenario47MockVaultClient) ReadSecret(
	_ context.Context, _ string,
) (map[string]interface{}, error) {
	return nil, nil
}

func (m *scenario47MockVaultClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return m.writeErr
}

func (m *scenario47MockVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return m.writeData, m.writeErr
}

func (m *scenario47MockVaultClient) IsEnabled() bool {
	return m.enabled
}

// Ensure the mock implements the interface.
var _ = fmt.Sprintf("%v", (*scenario47MockVaultClient)(nil))
