package certmanager

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

// IssueServerCertificate issues a server certificate from the Vault PKI engine
// at the given mount path and role, with the supplied DNS SANs (the first DNS
// name becomes the certificate Common Name). It returns the issuing CA, the
// server certificate and the server private key, all PEM-encoded.
//
// It is the exported entry point used by the cluster controller to auto-issue
// CloudberryCluster server certificates (spec.auth.ssl) from the SAME Vault PKI
// mount/role the operator uses for its webhook certificates.
func IssueServerCertificate(
	ctx context.Context,
	vaultClient vault.Client,
	mountPath, role string,
	dnsNames []string,
	validity time.Duration,
) (caCertPEM, serverCertPEM, serverKeyPEM []byte, err error) {
	if len(dnsNames) == 0 {
		return nil, nil, nil, fmt.Errorf("at least one DNS name is required to issue a server certificate")
	}
	if validity <= 0 {
		validity = DefaultCertValidity
	}
	cfg := Config{
		VaultPKIMountPath: mountPath,
		VaultPKIRole:      role,
	}
	return issueVaultPKICert(ctx, vaultClient, cfg, dnsNames, validity)
}

// DefaultCertValidity is the default certificate validity period used when no
// explicit validity is configured (one year, matching the webhook certificate
// default in generateCertificates).
const DefaultCertValidity = 365 * 24 * time.Hour

// NeedsRotationFromPEM reports whether the PEM-encoded certificate needs
// rotation: it is unparsable, already expired, or past the rotation threshold
// (2/3 of its lifetime — the same policy applied to webhook certificates).
func NeedsRotationFromPEM(certPEM []byte) (bool, error) {
	if len(certPEM) == 0 {
		return true, nil
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true, fmt.Errorf("failed to decode certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return true, fmt.Errorf("parsing certificate: %w", err)
	}
	return certPastRotationThreshold(cert), nil
}

// certPastRotationThreshold reports whether the certificate is expired or has
// passed the rotation threshold fraction (2/3) of its lifetime.
func certPastRotationThreshold(cert *x509.Certificate) bool {
	now := time.Now()
	if now.After(cert.NotAfter) {
		return true
	}
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	rotationTime := cert.NotBefore.Add(
		time.Duration(float64(lifetime) * rotationThresholdFraction),
	)
	return now.After(rotationTime)
}
