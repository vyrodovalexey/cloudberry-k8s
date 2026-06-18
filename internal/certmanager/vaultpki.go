package certmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
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

	ctx, span := telemetry.StartSpan(ctx, certTracerName, "certmanager.issueVaultPKICert")
	defer span.End()

	result, err := vaultClient.WriteSecretWithResponse(ctx, issuePath, data)
	if err != nil {
		telemetry.SetSpanError(span, err)
		return nil, nil, nil, fmt.Errorf("issuing certificate via vault PKI at %s: %w", issuePath, err)
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
