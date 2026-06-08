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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// Scenario47SSLE2ESuite tests Scenario 47: SSL/TLS Configuration end-to-end.
type Scenario47SSLE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario47(t *testing.T) {
	suite.Run(t, new(Scenario47SSLE2ESuite))
}

// buildE2ESSLCluster creates a running cluster with SSL configuration for E2E tests.
func (s *Scenario47SSLE2ESuite) buildE2ESSLCluster(
	name string, sslEnabled bool, certSecretName, minTLS string,
) *cbv1alpha1.CloudberryCluster {
	b := testutil.NewClusterBuilder(name, s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSSL(sslEnabled, certSecretName)

	if minTLS != "" {
		b = b.WithSSLMinTLSVersion(minTLS)
	}

	return b.Build()
}

// getPostgresqlConf builds the postgresql.conf ConfigMap using the builder
// and returns its content.
func (s *Scenario47SSLE2ESuite) getPostgresqlConf(
	cluster *cbv1alpha1.CloudberryCluster,
) string {
	s.T().Helper()

	b := builder.NewBuilder()
	cm := b.BuildPostgresqlConfConfigMap(cluster)
	content, ok := cm.Data["postgresql.conf"]
	require.True(s.T(), ok, "postgresql.conf key must exist in ConfigMap")
	return content
}

// TestE2E_Scenario47a_SSLConfig_PostgresqlConf creates a cluster with SSL
// enabled and verifies the ConfigMap contains all SSL settings.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47a_SSLConfig_PostgresqlConf() {
	s.logger.Info("starting scenario 47a E2E: SSL config in postgresql.conf")

	cluster := s.buildE2ESSLCluster("e2e-s47a-conf", true, "cloudberry-tls", "1.2")
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

	s.logger.Info("scenario 47a E2E: SSL config in postgresql.conf completed")
}

// TestE2E_Scenario47a_SSLConfig_TLSVolume verifies that the StatefulSet has
// a TLS volume when SSL is enabled.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47a_SSLConfig_TLSVolume() {
	s.logger.Info("starting scenario 47a E2E: TLS volume in StatefulSet")

	cluster := s.buildE2ESSLCluster("e2e-s47a-vol", true, "cloudberry-tls", "1.2")
	b := builder.NewBuilder()

	sts, err := b.BuildCoordinatorStatefulSet(cluster)
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
				"tls volume should be an EmptyDir")
			break
		}
	}
	assert.True(s.T(), foundTLSVol, "StatefulSet should have a 'tls' emptyDir volume")

	// Verify TLS volume mount on the main container.
	mainContainer := sts.Spec.Template.Spec.Containers[0]
	var foundMount bool
	for _, mount := range mainContainer.VolumeMounts {
		if mount.Name == "tls" {
			foundMount = true
			assert.Equal(s.T(), "/tls", mount.MountPath)
			assert.True(s.T(), mount.ReadOnly)
			break
		}
	}
	assert.True(s.T(), foundMount, "main container should have a 'tls' volume mount")

	s.logger.Info("scenario 47a E2E: TLS volume in StatefulSet completed")
}

// TestE2E_Scenario47a_SSLConfig_MinTLSVersions tests both TLS 1.2 and 1.3
// minimum version settings.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47a_SSLConfig_MinTLSVersions() {
	s.logger.Info("starting scenario 47a E2E: min TLS versions")

	versions := []struct {
		version  string
		expected string
	}{
		{"1.2", "ssl_min_protocol_version = 'TLSv1.2'"},
		{"1.3", "ssl_min_protocol_version = 'TLSv1.3'"},
	}

	for _, v := range versions {
		s.T().Run("TLS_"+v.version, func(t *testing.T) {
			name := "e2e-s47a-tls" + strings.ReplaceAll(v.version, ".", "")
			cluster := s.buildE2ESSLCluster(name, true, "cloudberry-tls", v.version)
			content := s.getPostgresqlConf(cluster)

			assert.Contains(t, content, v.expected,
				"postgresql.conf should contain %s", v.expected)
		})
	}

	s.logger.Info("scenario 47a E2E: min TLS versions completed")
}

// TestE2E_Scenario47a_SSLConfig_HostSSLRule verifies that a hostssl HBA rule
// is correctly rendered in pg_hba.conf.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47a_SSLConfig_HostSSLRule() {
	s.logger.Info("starting scenario 47a E2E: hostssl HBA rule")

	cluster := testutil.NewClusterBuilder("e2e-s47a-hostssl", s.namespace).
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

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAuthReconciler(
		env.Client, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	result, err := reconciler.Reconcile(s.ctx, req)
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	hbaCM, err := env.GetConfigMap(
		s.ctx,
		util.PgHBAConfConfigMapName(cluster.Name),
		cluster.Namespace,
	)
	require.NoError(s.T(), err)

	hbaContent, ok := hbaCM.Data["pg_hba.conf"]
	require.True(s.T(), ok)
	assert.Contains(s.T(), hbaContent, "hostssl\tall\tall\t0.0.0.0/0\tscram-sha-256",
		"pg_hba.conf should contain the hostssl rule")

	s.logger.Info("scenario 47a E2E: hostssl HBA rule completed")
}

// TestE2E_Scenario47b_VaultPKI_SelfSignedFallback tests that the certmanager
// generates self-signed certificates when Vault PKI is not available.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47b_VaultPKI_SelfSignedFallback() {
	s.logger.Info("starting scenario 47b E2E: self-signed fallback")

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := certmanager.Config{
		ServiceName:          "cloudberry-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "e2e-self-signed-certs",
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

	// Verify the secret was created.
	secret := &corev1.Secret{}
	getErr := k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      "e2e-self-signed-certs",
		Namespace: "cloudberry-system",
	}, secret)
	require.NoError(s.T(), getErr)
	assert.NotEmpty(s.T(), secret.Data["ca.crt"])
	assert.NotEmpty(s.T(), secret.Data["tls.crt"])
	assert.NotEmpty(s.T(), secret.Data["tls.key"])

	s.logger.Info("scenario 47b E2E: self-signed fallback completed")
}

// TestE2E_Scenario47b_VaultPKI_CertRotationCheck tests the rotation threshold
// logic by creating a certificate that is past the 2/3 lifetime threshold.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47b_VaultPKI_CertRotationCheck() {
	s.logger.Info("starting scenario 47b E2E: cert rotation check")

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Generate a near-expiry certificate (past 2/3 threshold).
	nearExpiryCert, nearExpiryKey := e2eGenerateNearExpiryCert(s.T())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-rotation-certs",
			Namespace: "cloudberry-system",
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
		ServiceName:          "cloudberry-webhook",
		ServiceNamespace:     "cloudberry-system",
		SecretName:           "e2e-rotation-certs",
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

	s.logger.Info("scenario 47b E2E: cert rotation check completed")
}

// TestE2E_Scenario47_SSLConfigCases runs the SSL config cases catalog.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47_SSLConfigCases() {
	s.logger.Info("starting scenario 47 E2E: SSL config cases catalog")

	testCases := cases.SSLConfigCases()
	require.Len(s.T(), testCases, 4, "should have 4 SSL config test cases")

	b := builder.NewBuilder()

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			cluster := s.buildE2ESSLCluster(
				"e2e-case-"+strings.ReplaceAll(tc.Name, "_", "-"),
				tc.SSLEnabled,
				tc.CertSecretName,
				tc.MinTLSVersion,
			)

			// Verify postgresql.conf content.
			cm := b.BuildPostgresqlConfConfigMap(cluster)
			content := cm.Data["postgresql.conf"]

			if tc.ExpectedConfLines != nil {
				for _, expected := range tc.ExpectedConfLines {
					assert.Contains(t, content, expected,
						"[%s] postgresql.conf should contain: %s", tc.Name, expected)
				}
			} else {
				assert.NotContains(t, content, "ssl = on",
					"[%s] postgresql.conf should NOT contain ssl = on", tc.Name)
			}

			// Verify TLS volume presence.
			sts, err := b.BuildCoordinatorStatefulSet(cluster)
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

			assert.NotEmpty(t, tc.Description,
				"[%s] test case should have a description", tc.Name)
		})
	}

	s.logger.Info("scenario 47 E2E: SSL config cases catalog completed")
}

// TestE2E_Scenario47_ClusterWithSSL creates a cluster CR with SSL enabled
// and verifies it is accepted by the fake K8s client.
func (s *Scenario47SSLE2ESuite) TestE2E_Scenario47_ClusterWithSSL() {
	s.logger.Info("starting scenario 47 E2E: cluster with SSL")

	cluster := testutil.NewClusterBuilder("e2e-s47-ssl", s.namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		WithSSL(true, "cloudberry-tls").
		WithSSLMinTLSVersion("1.2").
		Build()

	// Verify the SSL spec is set correctly.
	require.NotNil(s.T(), cluster.Spec.Auth, "auth spec should be set")
	require.NotNil(s.T(), cluster.Spec.Auth.SSL, "SSL spec should be set")
	assert.True(s.T(), cluster.Spec.Auth.SSL.Enabled, "SSL should be enabled")
	require.NotNil(s.T(), cluster.Spec.Auth.SSL.CertSecret, "cert secret should be set")
	assert.Equal(s.T(), "cloudberry-tls", cluster.Spec.Auth.SSL.CertSecret.Name)
	assert.Equal(s.T(), "1.2", cluster.Spec.Auth.SSL.MinTLSVersion)

	// Create the cluster in the fake K8s env and verify it persists.
	env := testutil.NewTestK8sEnv(cluster)
	retrieved, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), retrieved.Spec.Auth)
	require.NotNil(s.T(), retrieved.Spec.Auth.SSL)
	assert.True(s.T(), retrieved.Spec.Auth.SSL.Enabled)
	assert.Equal(s.T(), "cloudberry-tls", retrieved.Spec.Auth.SSL.CertSecret.Name)
	assert.Equal(s.T(), "1.2", retrieved.Spec.Auth.SSL.MinTLSVersion)

	s.logger.Info("scenario 47 E2E: cluster with SSL completed")
}

// e2eGenerateNearExpiryCert generates a self-signed certificate that is past
// the 2/3 rotation threshold for E2E testing.
func e2eGenerateNearExpiryCert(t *testing.T) (certPEM, keyPEM []byte) {
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
			CommonName:   "e2e-near-expiry",
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
	_ metrics.Recorder        = (*metrics.NoopRecorder)(nil)
)
