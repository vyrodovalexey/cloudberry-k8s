package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// VaultTestHelper provides helpers for interacting with Vault in tests.
type VaultTestHelper struct {
	Addr       string
	Token      string
	HTTPClient *http.Client
}

// NewVaultTestHelper creates a new VaultTestHelper.
func NewVaultTestHelper(addr, token string) *VaultTestHelper {
	return &VaultTestHelper{
		Addr:  addr,
		Token: token,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// IsAvailable checks if Vault is available.
func (v *VaultTestHelper) IsAvailable(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.Addr+"/v1/sys/health", nil)
	if err != nil {
		return false
	}
	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ReadSecret reads a secret from Vault KV v2.
func (v *VaultTestHelper) ReadSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.Addr+"/v1/"+path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Vault-Token", v.Token)

	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("reading secret: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vault returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data struct {
			Data map[string]interface{} `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return result.Data.Data, nil
}

// WriteSecret writes a secret to Vault KV v2.
func (v *VaultTestHelper) WriteSecret(ctx context.Context, path string, data map[string]interface{}) error {
	payload := map[string]interface{}{
		"data": data,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.Addr+"/v1/"+path, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Vault-Token", v.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("writing secret: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vault returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// IssueCertificate issues a certificate from the PKI engine.
func (v *VaultTestHelper) IssueCertificate(ctx context.Context, pkiMount, role, commonName string) (*CertificateBundle, error) {
	payload := map[string]interface{}{
		"common_name": commonName,
		"ttl":         "1h",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	path := fmt.Sprintf("%s/issue/%s", pkiMount, role)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.Addr+"/v1/"+path, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Vault-Token", v.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("issuing certificate: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vault returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Data CertificateBundle `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &result.Data, nil
}

// CheckPKIEngine checks if the PKI engine is mounted and configured.
func (v *VaultTestHelper) CheckPKIEngine(ctx context.Context, mount string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.Addr+"/v1/sys/mounts/"+mount, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Vault-Token", v.Token)

	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("checking PKI engine: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PKI engine not mounted at %s (status %d)", mount, resp.StatusCode)
	}

	return nil
}

// CheckKVEngine checks if the KV engine is mounted.
func (v *VaultTestHelper) CheckKVEngine(ctx context.Context, mount string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.Addr+"/v1/sys/mounts/"+mount, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Vault-Token", v.Token)

	resp, err := v.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("checking KV engine: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("KV engine not mounted at %s (status %d)", mount, resp.StatusCode)
	}

	return nil
}

// CertificateBundle holds a certificate and its private key from Vault PKI.
type CertificateBundle struct {
	Certificate  string   `json:"certificate"`
	PrivateKey   string   `json:"private_key"`
	CAChain      []string `json:"ca_chain"`
	IssuingCA    string   `json:"issuing_ca"`
	SerialNumber string   `json:"serial_number"`
}
