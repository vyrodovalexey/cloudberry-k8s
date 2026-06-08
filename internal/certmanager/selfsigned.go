package certmanager

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	// caValidityDuration is the validity period for the self-signed CA certificate.
	caValidityDuration = 10 * 365 * 24 * time.Hour // 10 years

	// serialNumberBitSize is the bit size for certificate serial numbers.
	serialNumberBitSize = 128
)

// generateSelfSignedCert generates a self-signed CA and server certificate.
// Returns CA cert PEM, server cert PEM, server key PEM, and any error.
func generateSelfSignedCert(
	dnsNames []string,
	serverValidity time.Duration,
) (caCertPEM, serverCertPEM, serverKeyPEM []byte, err error) {
	// Generate CA key pair.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating CA key: %w", err)
	}

	// Create CA certificate.
	caSerial, err := generateSerialNumber()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating CA serial number: %w", err)
	}

	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			Organization: []string{organizationName},
			CommonName:   organizationName + "-ca",
		},
		NotBefore:             now,
		NotAfter:              now.Add(caValidityDuration),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parsing CA certificate: %w", err)
	}

	// Generate server key pair.
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating server key: %w", err)
	}

	// Create server certificate.
	serverSerial, err := generateSerialNumber()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating server serial number: %w", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject: pkix.Name{
			Organization: []string{organizationName},
			CommonName:   dnsNames[0],
		},
		DNSNames:              dnsNames,
		NotBefore:             now,
		NotAfter:              now.Add(serverValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating server certificate: %w", err)
	}

	// Encode to PEM.
	caCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	serverCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshaling server key: %w", err)
	}
	serverKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER})

	return caCertPEM, serverCertPEM, serverKeyPEM, nil
}

// generateSerialNumber generates a random serial number for X.509 certificates.
func generateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), serialNumberBitSize)
	serial, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("generating serial number: %w", err)
	}
	return serial, nil
}
