package certmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pkiMockVaultClient returns an enabled mock whose WriteSecretWithResponse
// answers like the Vault PKI issue endpoint.
func pkiMockVaultClient() *mockVaultClient {
	return &mockVaultClient{
		enabled: true,
		writeData: map[string]interface{}{
			"certificate": "CERT-PEM",
			"private_key": "KEY-PEM",
			"issuing_ca":  "CA-PEM",
		},
	}
}

func TestIssueServerCertificate_Success(t *testing.T) {
	ca, cert, key, err := IssueServerCertificate(
		context.Background(),
		pkiMockVaultClient(),
		"pki", "cloudberry",
		[]string{"db.default.svc.cluster.local", "*.db-hl.default.svc.cluster.local"},
		24*time.Hour,
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("CA-PEM"), ca)
	assert.Equal(t, []byte("CERT-PEM"), cert)
	assert.Equal(t, []byte("KEY-PEM"), key)
}

func TestIssueServerCertificate_DefaultsValidityAndMountRole(t *testing.T) {
	// Zero validity falls back to DefaultCertValidity; empty mount/role fall
	// back to the package defaults inside issueVaultPKICert ("pki"/operator org).
	ca, cert, key, err := IssueServerCertificate(
		context.Background(),
		pkiMockVaultClient(),
		"", "",
		[]string{"db.default.svc"},
		0,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, ca)
	assert.NotEmpty(t, cert)
	assert.NotEmpty(t, key)
}

func TestIssueServerCertificate_NoDNSNames(t *testing.T) {
	_, _, _, err := IssueServerCertificate(
		context.Background(), pkiMockVaultClient(), "pki", "role", nil, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one DNS name")
}

func TestIssueServerCertificate_DisabledVaultClient(t *testing.T) {
	_, _, _, err := IssueServerCertificate(
		context.Background(),
		&mockVaultClient{enabled: false},
		"pki", "role",
		[]string{"db.default.svc"},
		time.Hour,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

func TestIssueServerCertificate_MissingResponseFields(t *testing.T) {
	vc := pkiMockVaultClient()
	delete(vc.writeData, "private_key")
	_, _, _, err := IssueServerCertificate(
		context.Background(), vc, "pki", "role", []string{"db.default.svc"}, time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "private_key")
}

func TestNeedsRotationFromPEM(t *testing.T) {
	t.Run("empty PEM needs rotation without error", func(t *testing.T) {
		needs, err := NeedsRotationFromPEM(nil)
		require.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("garbage PEM needs rotation with error", func(t *testing.T) {
		needs, err := NeedsRotationFromPEM([]byte("not-a-pem"))
		require.Error(t, err)
		assert.True(t, needs)
	})

	t.Run("undecodable certificate needs rotation with error", func(t *testing.T) {
		// Valid PEM block, invalid DER body.
		pemBlock := "-----BEGIN CERTIFICATE-----\nYm9ndXM=\n-----END CERTIFICATE-----\n"
		needs, err := NeedsRotationFromPEM([]byte(pemBlock))
		require.Error(t, err)
		assert.True(t, needs)
	})

	t.Run("fresh certificate does not need rotation", func(t *testing.T) {
		certPEM := generateSelfSignedLeafWithLifetime(t, -time.Hour, 365*24*time.Hour)
		needs, err := NeedsRotationFromPEM(certPEM)
		require.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("certificate past two-thirds lifetime needs rotation", func(t *testing.T) {
		certPEM := generateSelfSignedLeafWithLifetime(t, -100*24*time.Hour, 10*24*time.Hour)
		needs, err := NeedsRotationFromPEM(certPEM)
		require.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("expired certificate needs rotation", func(t *testing.T) {
		certPEM, _ := generateExpiredCert(t)
		needs, err := NeedsRotationFromPEM(certPEM)
		require.NoError(t, err)
		assert.True(t, needs)
	})
}
