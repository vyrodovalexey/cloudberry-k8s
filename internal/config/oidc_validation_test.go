package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadWithOIDCEnv loads a config with OIDC enabled plus the given extra envs.
func loadWithOIDCEnv(t *testing.T, extra map[string]string) (*OperatorConfig, error) {
	t.Helper()
	envs := map[string]string{
		"CLOUDBERRY_OIDC_ENABLED":    "true",
		"CLOUDBERRY_OIDC_ISSUER_URL": "https://idp.example.com/realms/x",
		"CLOUDBERRY_OIDC_CLIENT_ID":  "operator",
	}
	for k, v := range extra {
		envs[k] = v
	}
	for k, v := range envs {
		require.NoError(t, os.Setenv(k, v))
	}
	t.Cleanup(func() {
		for k := range envs {
			_ = os.Unsetenv(k)
		}
	})
	return NewLoader("").Load()
}

// TestValidateOIDC_RejectsUnsupportedRoleClaimSource verifies B-8: a typo'd
// role-claim-source fails validation with a clear message instead of being
// silently ignored.
func TestValidateOIDC_RejectsUnsupportedRoleClaimSource(t *testing.T) {
	_, err := loadWithOIDCEnv(t, map[string]string{
		"CLOUDBERRY_OIDC_ROLE_CLAIM_SOURCE": "userinfos",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "role-claim-source")
	assert.Contains(t, err.Error(), "userinfos")
}

// TestValidateOIDC_RejectsUnsupportedRoleMatchMode verifies the match-mode
// validation.
func TestValidateOIDC_RejectsUnsupportedRoleMatchMode(t *testing.T) {
	_, err := loadWithOIDCEnv(t, map[string]string{
		"CLOUDBERRY_OIDC_ROLE_MATCH_MODE": "regex",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "role-match-mode")
}

// TestValidateOIDC_AcceptsSupportedValues verifies both supported sources and
// modes pass validation.
func TestValidateOIDC_AcceptsSupportedValues(t *testing.T) {
	cfg, err := loadWithOIDCEnv(t, map[string]string{
		"CLOUDBERRY_OIDC_ROLE_CLAIM_SOURCE": "userinfo",
		"CLOUDBERRY_OIDC_ROLE_MATCH_MODE":   "suffix",
	})
	require.NoError(t, err)
	assert.Equal(t, "userinfo", cfg.OIDC.RoleClaimSource)
	assert.Equal(t, "suffix", cfg.OIDC.RoleMatchMode)
}

// TestNoListenAddressField verifies B-2: the dead listen-address option was
// removed from the flag set (no dead options).
func TestNoListenAddressField(t *testing.T) {
	fs := OperatorFlagSet()
	assert.Nil(t, fs.Lookup("listen-address"), "listen-address flag must be removed")
	assert.NotNil(t, fs.Lookup("api-address"))
	assert.NotNil(t, fs.Lookup("webhook-port"))
	assert.NotNil(t, fs.Lookup("reconcile-interval"))
	assert.NotNil(t, fs.Lookup("operation-timeout"))
}
