package certmanager

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	dnsNames := []string{
		"webhook.default.svc",
		"webhook.default.svc.cluster.local",
	}
	validity := 365 * 24 * time.Hour

	caCertPEM, serverCertPEM, serverKeyPEM, err := generateSelfSignedCert(dnsNames, validity)
	require.NoError(t, err)
	require.NotEmpty(t, caCertPEM)
	require.NotEmpty(t, serverCertPEM)
	require.NotEmpty(t, serverKeyPEM)

	// Parse and validate CA certificate.
	caBlock, _ := pem.Decode(caCertPEM)
	require.NotNil(t, caBlock)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	require.NoError(t, err)
	assert.True(t, caCert.IsCA)
	assert.Equal(t, organizationName+"-ca", caCert.Subject.CommonName)
	assert.Contains(t, caCert.Subject.Organization, organizationName)

	// Parse and validate server certificate.
	serverBlock, _ := pem.Decode(serverCertPEM)
	require.NotNil(t, serverBlock)
	serverCert, err := x509.ParseCertificate(serverBlock.Bytes)
	require.NoError(t, err)
	assert.False(t, serverCert.IsCA)
	assert.Equal(t, dnsNames[0], serverCert.Subject.CommonName)
	assert.Equal(t, dnsNames, serverCert.DNSNames)
	assert.Contains(t, serverCert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)

	// Verify server cert is signed by CA.
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	_, err = serverCert.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   dnsNames[0],
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	require.NoError(t, err)

	// Parse server key.
	keyBlock, _ := pem.Decode(serverKeyPEM)
	require.NotNil(t, keyBlock)
	assert.Equal(t, "EC PRIVATE KEY", keyBlock.Type)
}

func TestGenerateSelfSignedCert_Validity(t *testing.T) {
	dnsNames := []string{"test.default.svc"}
	validity := 30 * 24 * time.Hour // 30 days

	_, serverCertPEM, _, err := generateSelfSignedCert(dnsNames, validity)
	require.NoError(t, err)

	block, _ := pem.Decode(serverCertPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	// Check that the certificate validity is approximately 30 days.
	certLifetime := cert.NotAfter.Sub(cert.NotBefore)
	assert.InDelta(t, validity.Hours(), certLifetime.Hours(), 1)
}

func TestGenerateSerialNumber(t *testing.T) {
	serial1, err := generateSerialNumber()
	require.NoError(t, err)
	require.NotNil(t, serial1)

	serial2, err := generateSerialNumber()
	require.NoError(t, err)
	require.NotNil(t, serial2)

	// Serial numbers should be different.
	assert.NotEqual(t, serial1, serial2)
}
