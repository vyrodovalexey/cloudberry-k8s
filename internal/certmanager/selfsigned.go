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

	// pemTypeCertificate and pemTypeECPrivateKey are the PEM block types used
	// for the generated certificate and key material.
	pemTypeCertificate  = "CERTIFICATE"
	pemTypeECPrivateKey = "EC PRIVATE KEY"
)

// generateSelfSignedCert generates a fresh self-signed CA and a server (leaf)
// certificate issued from it. Returns CA cert PEM, CA key PEM, server cert
// PEM, server key PEM, and any error. The CA key is returned so callers can
// persist it and later renew only the leaf via issueLeafFromCA (H-1
// hardening). dnsNames must be non-empty (L-2).
func generateSelfSignedCert(
	dnsNames []string,
	serverValidity time.Duration,
) (caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM []byte, err error) {
	// L-2: guard the dnsNames[0] Common-Name access below instead of
	// panicking on a future caller passing an empty slice (mirrors
	// IssueServerCertificate's validation).
	if len(dnsNames) == 0 {
		return nil, nil, nil, nil,
			fmt.Errorf("at least one DNS name is required to issue a server certificate")
	}

	caCertPEM, caKeyPEM, err = generateCA()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	serverCertPEM, serverKeyPEM, err = issueLeafFromCA(caCertPEM, caKeyPEM, dnsNames, serverValidity)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM, nil
}

// generateCA generates the operator's self-signed CA certificate and private
// key (PEM-encoded), valid for caValidityDuration (10 years). The CA is
// marked MaxPathLenZero so it can only sign leaf certificates.
func generateCA() (caCertPEM, caKeyPEM []byte, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating CA key: %w", err)
	}

	caSerial, err := generateSerialNumber()
	if err != nil {
		return nil, nil, fmt.Errorf("generating CA serial number: %w", err)
	}

	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			Organization: []string{organizationName},
			CommonName:   selfSignedCACommonName,
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
		return nil, nil, fmt.Errorf("creating CA certificate: %w", err)
	}

	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling CA key: %w", err)
	}

	caCertPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: caCertDER})
	caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeECPrivateKey, Bytes: caKeyDER})
	return caCertPEM, caKeyPEM, nil
}

// issueLeafFromCA issues a server (leaf) certificate for the given DNS SANs,
// signed by the provided PEM-encoded CA certificate and CA private key. It is
// the leaf-only renewal primitive: reusing a persisted CA keeps the injected
// CA bundle valid across rotations (H-1 hardening). dnsNames must be
// non-empty (the first name becomes the Common Name).
func issueLeafFromCA(
	caCertPEM, caKeyPEM []byte,
	dnsNames []string,
	validity time.Duration,
) (serverCertPEM, serverKeyPEM []byte, err error) {
	if len(dnsNames) == 0 {
		return nil, nil, fmt.Errorf("at least one DNS name is required to issue a server certificate")
	}

	caCert, err := parseCertificatePEM(caCertPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing CA certificate: %w", err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return nil, nil, fmt.Errorf("failed to decode CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing CA key: %w", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating server key: %w", err)
	}

	serverSerial, err := generateSerialNumber()
	if err != nil {
		return nil, nil, fmt.Errorf("generating server serial number: %w", err)
	}

	now := time.Now()
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject: pkix.Name{
			Organization: []string{organizationName},
			CommonName:   dnsNames[0],
		},
		DNSNames:              dnsNames,
		NotBefore:             now,
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("creating server certificate: %w", err)
	}

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling server key: %w", err)
	}

	serverCertPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: serverCertDER})
	serverKeyPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeECPrivateKey, Bytes: serverKeyDER})
	return serverCertPEM, serverKeyPEM, nil
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
