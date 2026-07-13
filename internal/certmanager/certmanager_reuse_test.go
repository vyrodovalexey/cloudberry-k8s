package certmanager

// Tests for the DEV-2 CA-reuse (leaf-only renewal) feature and its guards
// (handoff gaps UT-1..UT-7): reuseCAForLeaf success + negative branches,
// the caUsableForReuse decision table, the legacy-Secret ca.key upgrade
// chain, the empty-dnsNames guards (L-2) and the issueLeafFromCA error
// paths.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testDNSNames returns the SANs matching newTestConfig's service identity.
func testDNSNames() []string {
	return []string{
		"test-webhook.test-ns.svc",
		"test-webhook.test-ns.svc.cluster.local",
	}
}

// newCertSecret builds the webhook cert Secret fixture from raw PEM material.
func newCertSecret(cfg Config, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: cfg.SecretNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: data,
	}
}

// mustParseCert parses the first PEM block as an X.509 certificate.
func mustParseCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	cert, err := parseCertificatePEM(certPEM)
	require.NoError(t, err)
	return cert
}

// ---------------------------------------------------------------------------
// UT-1: leaf-only renewal success — ca.crt stays byte-identical
// ---------------------------------------------------------------------------

func TestEnsureCertificates_LeafOnlyRenewal_ReusesPersistedCA(t *testing.T) {
	// Arrange: a valid, fresh CA persisted WITH its key, and an already
	// expired leaf signed by it — the exact H-1 rotation scenario.
	cfg := newTestConfig()
	caCert, caKey, err := generateCA()
	require.NoError(t, err)
	expiredLeaf, expiredLeafKey, err := issueLeafFromCA(caCert, caKey, testDNSNames(), -time.Hour)
	require.NoError(t, err)

	secret := newCertSecret(cfg, map[string][]byte{
		secretKeyCACert:  caCert,
		secretKeyCAKey:   caKey,
		secretKeyTLSCert: expiredLeaf,
		secretKeyTLSKey:  expiredLeafKey,
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	// Act: rotation must renew ONLY the leaf.
	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)

	// Assert: the returned bundle is the single, byte-identical persisted CA
	// (no union — the CA did not change).
	assert.Equal(t, caCert, caBundle,
		"leaf-only renewal must return the persisted CA bytes unchanged")

	updated := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: cfg.SecretName, Namespace: cfg.SecretNamespace,
	}, updated))
	assert.Equal(t, caCert, updated.Data[secretKeyCACert],
		"ca.crt must stay byte-identical across a leaf-only renewal")
	assert.Equal(t, caKey, updated.Data[secretKeyCAKey],
		"ca.key must stay byte-identical across a leaf-only renewal")
	assert.NotEqual(t, expiredLeaf, updated.Data[secretKeyTLSCert],
		"tls.crt must be re-issued")
	assert.NotEqual(t, expiredLeafKey, updated.Data[secretKeyTLSKey],
		"tls.key must be re-issued")

	// The fresh leaf is signed by the operator's own (reused) CA and is valid.
	leaf := mustParseCert(t, updated.Data[secretKeyTLSCert])
	assert.Equal(t, selfSignedCACommonName, leaf.Issuer.CommonName)
	assert.True(t, time.Now().Before(leaf.NotAfter), "renewed leaf must be time-valid")
}

// ---------------------------------------------------------------------------
// UT-2: caUsableForReuse decision table
// ---------------------------------------------------------------------------

func TestCAUsableForReuse(t *testing.T) {
	tests := []struct {
		name         string
		caCertPEM    []byte
		leafValidity time.Duration
		want         bool
	}{
		{
			name: "non-self-signed issuer is not reusable",
			caCertPEM: generateCertWithIssuer(t, "Some External CA", "external-org",
				10*365*24*time.Hour),
			leafValidity: 24 * time.Hour,
			want:         false,
		},
		{
			name: "operator CA past 2/3 of its lifetime is not reusable",
			// Lifetime 120d, 90d elapsed (75% > 2/3).
			caCertPEM:    generateSelfSignedLeafWithLifetime(t, -90*24*time.Hour, 30*24*time.Hour),
			leafValidity: 24 * time.Hour,
			want:         false,
		},
		{
			name: "CA remaining validity shorter than the leaf validity is not reusable",
			// Fresh CA (1h of 11h elapsed < 2/3) but only 10h left < 24h leaf.
			caCertPEM:    generateSelfSignedLeafWithLifetime(t, -time.Hour, 10*time.Hour),
			leafValidity: 24 * time.Hour,
			want:         false,
		},
		{
			name:         "fresh operator CA with room for the leaf is reusable",
			caCertPEM:    generateSelfSignedLeafWithLifetime(t, -time.Hour, 10*365*24*time.Hour),
			leafValidity: 365 * 24 * time.Hour,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caCert := mustParseCert(t, tt.caCertPEM)

			got := caUsableForReuse(caCert, tt.leafValidity)

			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// UT-3: reuseCAForLeaf negative branches
// ---------------------------------------------------------------------------

func TestReuseCAForLeaf_NegativeBranches(t *testing.T) {
	caCert, caKey, err := generateCA()
	require.NoError(t, err)
	agingCA := generateSelfSignedLeafWithLifetime(t, -90*24*time.Hour, 30*24*time.Hour)

	tests := []struct {
		name string
		data map[string][]byte
	}{
		{
			name: "missing ca.key (legacy Secret)",
			data: map[string][]byte{secretKeyCACert: caCert},
		},
		{
			name: "missing ca.crt",
			data: map[string][]byte{secretKeyCAKey: caKey},
		},
		{
			name: "unparseable ca.crt PEM",
			data: map[string][]byte{
				secretKeyCACert: []byte("not-a-pem"),
				secretKeyCAKey:  caKey,
			},
		},
		{
			name: "parseable CA but garbage ca.key fails leaf issuance",
			data: map[string][]byte{
				secretKeyCACert: caCert,
				secretKeyCAKey:  []byte("garbage-key"),
			},
		},
		{
			name: "aging CA past its own rotation threshold",
			data: map[string][]byte{
				secretKeyCACert: agingCA,
				secretKeyCAKey:  caKey,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &certManager{config: newTestConfig(), logger: slog.Default()}
			secret := &corev1.Secret{Data: tt.data}

			certs, ok := cm.reuseCAForLeaf(secret, testDNSNames(), 24*time.Hour)

			assert.False(t, ok, "reuse must be refused so the caller regenerates everything")
			assert.Equal(t, issuedCerts{}, certs)
		})
	}
}

// TestEnsureCertificates_UnparseableCA_FullRegeneration is the flow-level
// fallback contract of UT-3: an unusable persisted CA must fall through to a
// full CA+leaf regeneration (fresh ca.crt bytes) instead of failing.
func TestEnsureCertificates_UnparseableCA_FullRegeneration(t *testing.T) {
	cfg := newTestConfig()
	caCert, caKey, err := generateCA()
	require.NoError(t, err)
	expiredLeaf, expiredLeafKey, err := issueLeafFromCA(caCert, caKey, testDNSNames(), -time.Hour)
	require.NoError(t, err)

	secret := newCertSecret(cfg, map[string][]byte{
		secretKeyCACert:  []byte("corrupted-pem"), // unparseable -> reuse impossible
		secretKeyCAKey:   caKey,
		secretKeyTLSCert: expiredLeaf,
		secretKeyTLSKey:  expiredLeafKey,
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	caBundle, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)

	updated := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: cfg.SecretName, Namespace: cfg.SecretNamespace,
	}, updated))
	assert.NotEqual(t, []byte("corrupted-pem"), updated.Data[secretKeyCACert],
		"a fresh CA must be generated")
	assert.NotEqual(t, caCert, updated.Data[secretKeyCACert],
		"the fresh CA must not be the old one")
	assert.NotEmpty(t, updated.Data[secretKeyCAKey], "the fresh CA key must be persisted")
	// The unparseable previous CA cannot be unioned: single fresh CA bundle.
	assert.Equal(t, updated.Data[secretKeyCACert], caBundle)
}

// ---------------------------------------------------------------------------
// UT-4: legacy Secret (no ca.key) upgrade chain
// ---------------------------------------------------------------------------

func TestEnsureCertificates_LegacySecretUpgrade_PersistsCAKeyThenReusesCA(t *testing.T) {
	// Arrange: a legacy Secret WITHOUT ca.key holding an expired self-signed
	// leaf (pre-DEV-2 layout).
	cfg := newTestConfig()
	oldCA, oldCAKey, err := generateCA()
	require.NoError(t, err)
	expiredLeaf, expiredLeafKey, err := issueLeafFromCA(oldCA, oldCAKey, testDNSNames(), -time.Hour)
	require.NoError(t, err)

	secret := newCertSecret(cfg, map[string][]byte{
		secretKeyCACert:  oldCA,
		secretKeyTLSCert: expiredLeaf,
		secretKeyTLSKey:  expiredLeafKey,
		// no ca.key: legacy layout
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(secret).Build()
	cm := New(k8sClient, nil, cfg, nil)

	// Act 1: first rotation must fully regenerate and START persisting ca.key.
	_, err = cm.EnsureCertificates(context.Background())
	require.NoError(t, err)

	upgraded := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: cfg.SecretName, Namespace: cfg.SecretNamespace,
	}, upgraded))
	require.NotEmpty(t, upgraded.Data[secretKeyCAKey],
		"regeneration must persist ca.key for future leaf-only renewals")
	newCA := upgraded.Data[secretKeyCACert]
	assert.NotEqual(t, oldCA, newCA, "the legacy CA (without key) must be replaced")

	// Arrange 2: force another leaf rotation by swapping in an expired leaf
	// issued from the NEW persisted CA.
	expiredLeaf2, expiredLeaf2Key, err := issueLeafFromCA(
		newCA, upgraded.Data[secretKeyCAKey], testDNSNames(), -time.Hour)
	require.NoError(t, err)
	upgraded.Data[secretKeyTLSCert] = expiredLeaf2
	upgraded.Data[secretKeyTLSKey] = expiredLeaf2Key
	require.NoError(t, k8sClient.Update(context.Background(), upgraded))

	// Act 2: the second rotation must now REUSE the persisted CA.
	bundle2, err := cm.EnsureCertificates(context.Background())
	require.NoError(t, err)

	final := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: cfg.SecretName, Namespace: cfg.SecretNamespace,
	}, final))
	assert.Equal(t, newCA, final.Data[secretKeyCACert],
		"the upgraded Secret must enable CA reuse: ca.crt bytes unchanged")
	assert.Equal(t, newCA, bundle2, "reuse returns the single persisted CA bundle")
	assert.NotEqual(t, expiredLeaf2, final.Data[secretKeyTLSCert], "the leaf must be renewed")
}

// ---------------------------------------------------------------------------
// UT-5: empty dnsNames guards (L-2)
// ---------------------------------------------------------------------------

func TestGenerateSelfSignedCert_EmptyDNSNames(t *testing.T) {
	tests := []struct {
		name     string
		dnsNames []string
	}{
		{name: "nil slice", dnsNames: nil},
		{name: "empty slice", dnsNames: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caCert, caKey, tlsCert, tlsKey, err := generateSelfSignedCert(tt.dnsNames, time.Hour)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "DNS name")
			assert.Empty(t, caCert)
			assert.Empty(t, caKey)
			assert.Empty(t, tlsCert)
			assert.Empty(t, tlsKey)
		})
	}
}

func TestIssueLeafFromCA_EmptyDNSNames(t *testing.T) {
	caCert, caKey, err := generateCA()
	require.NoError(t, err)

	tests := []struct {
		name     string
		dnsNames []string
	}{
		{name: "nil slice", dnsNames: nil},
		{name: "empty slice", dnsNames: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert, key, err := issueLeafFromCA(caCert, caKey, tt.dnsNames, time.Hour)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "DNS name")
			assert.Empty(t, cert)
			assert.Empty(t, key)
		})
	}
}

// TestIssueSelfSigned_GenerateError covers the issueSelfSigned error
// propagation when full regeneration itself fails (empty dnsNames).
func TestIssueSelfSigned_GenerateError(t *testing.T) {
	cm := &certManager{config: newTestConfig(), logger: slog.Default()}

	certs, err := cm.issueSelfSigned(nil, true, nil, time.Hour)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "DNS name")
	assert.Equal(t, issuedCerts{}, certs)
}

// ---------------------------------------------------------------------------
// UT-6: issueLeafFromCA error paths
// ---------------------------------------------------------------------------

func TestIssueLeafFromCA_ErrorPaths(t *testing.T) {
	caCert, caKey, err := generateCA()
	require.NoError(t, err)

	// A syntactically valid PEM block whose payload is NOT an EC private key.
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	rsaKeyInECBlock := pem.EncodeToMemory(&pem.Block{
		Type:  pemTypeECPrivateKey,
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})

	tests := []struct {
		name        string
		caCertPEM   []byte
		caKeyPEM    []byte
		errContains string
	}{
		{
			name:        "bad CA certificate PEM",
			caCertPEM:   []byte("not-a-cert"),
			caKeyPEM:    caKey,
			errContains: "parsing CA certificate",
		},
		{
			name:        "undecodable CA key PEM",
			caCertPEM:   caCert,
			caKeyPEM:    []byte("not-a-key"),
			errContains: "failed to decode CA key PEM",
		},
		{
			name:        "valid PEM but non-EC private key",
			caCertPEM:   caCert,
			caKeyPEM:    rsaKeyInECBlock,
			errContains: "parsing CA key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert, key, err := issueLeafFromCA(tt.caCertPEM, tt.caKeyPEM, testDNSNames(), time.Hour)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Empty(t, cert)
			assert.Empty(t, key)
		})
	}
}

// ---------------------------------------------------------------------------
// UT-7: isSelfSignedIssuer organization mismatch
// ---------------------------------------------------------------------------

func TestIsSelfSignedIssuer_OrgMismatch(t *testing.T) {
	tests := []struct {
		name       string
		commonName string
		org        string
		want       bool
	}{
		{
			name:       "matching CN but foreign organization is not the operator CA",
			commonName: selfSignedCACommonName,
			org:        "other-org",
			want:       false,
		},
		{
			name:       "matching CN and organization is the operator CA",
			commonName: selfSignedCACommonName,
			org:        organizationName,
			want:       true,
		},
		{
			name:       "foreign CN is not the operator CA",
			commonName: "Test Root CA",
			org:        organizationName,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := mustParseCert(t, generateCertWithIssuer(t, tt.commonName, tt.org, time.Hour))

			assert.Equal(t, tt.want, isSelfSignedIssuer(cert))
		})
	}
}
