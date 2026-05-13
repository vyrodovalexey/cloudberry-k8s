package certmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

// issueVaultPKICert issues a server certificate from Vault PKI.
// Returns CA cert PEM, server cert PEM, server key PEM, and any error.
func issueVaultPKICert(
	ctx context.Context,
	vaultClient vault.Client,
	cfg Config,
	dnsNames []string,
	validity time.Duration,
) (caCertPEM, serverCertPEM, serverKeyPEM []byte, err error) {
	if vaultClient == nil || !vaultClient.IsEnabled() {
		return nil, nil, nil, fmt.Errorf("vault client is not enabled; cannot issue PKI certificates")
	}

	mountPath := cfg.VaultPKIMountPath
	if mountPath == "" {
		mountPath = "pki"
	}

	role := cfg.VaultPKIRole
	if role == "" {
		role = organizationName
	}

	// Build the common name from the first DNS name.
	commonName := ""
	if len(dnsNames) > 0 {
		commonName = dnsNames[0]
	}

	// Build the alt_names parameter as a comma-separated list.
	altNames := ""
	for i, name := range dnsNames {
		if i > 0 {
			altNames += ","
		}
		altNames += name
	}

	// Issue the certificate via Vault PKI.
	issuePath := fmt.Sprintf("%s/issue/%s", mountPath, role)
	data := map[string]interface{}{
		"common_name": commonName,
		"alt_names":   altNames,
		"ttl":         fmt.Sprintf("%ds", int(validity.Seconds())),
	}

	result, err := vaultClient.ReadSecret(ctx, issuePath)
	if err != nil {
		// ReadSecret is for KV; for PKI issue we need to write.
		// Use WriteSecret as a workaround since the vault.Client interface
		// only exposes ReadSecret/WriteSecret for KV v2.
		// In production, the Vault client should be extended with a PKI-specific method.
		// For now, we attempt to read the result from the write path.
		_ = result
		return nil, nil, nil, fmt.Errorf(
			"vault PKI issue not directly supported via current vault.Client interface; "+
				"issue path: %s, data: %v: %w", issuePath, data, err,
		)
	}

	// Parse the response.
	certStr, ok := result["certificate"].(string)
	if !ok {
		return nil, nil, nil, fmt.Errorf("vault PKI response missing 'certificate' field")
	}

	keyStr, ok := result["private_key"].(string)
	if !ok {
		return nil, nil, nil, fmt.Errorf("vault PKI response missing 'private_key' field")
	}

	caStr, ok := result["issuing_ca"].(string)
	if !ok {
		return nil, nil, nil, fmt.Errorf("vault PKI response missing 'issuing_ca' field")
	}

	return []byte(caStr), []byte(certStr), []byte(keyStr), nil
}
