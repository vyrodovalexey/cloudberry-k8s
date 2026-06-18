package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Scenario 111 — Security: deepcopy roundtrip for the new API surface
//   - PxfKerberosSpec (SE.4)
//   - PxfServerSpec.Kerberos (SE.4)
//   - PxfSpec.DataLoaderRole (SE.6)
// Proves the generated deepcopy correctly (and independently) copies the new
// fields, so a mutation of the copy never leaks back into the source.
// ============================================================================

// TestPxfKerberosSpec_DeepCopy_Roundtrip proves PxfKerberosSpec.DeepCopy copies
// every field and yields a fully independent value.
func TestPxfKerberosSpec_DeepCopy_Roundtrip(t *testing.T) {
	src := &PxfKerberosSpec{
		Principal:     "pxf/_HOST@EXAMPLE.COM",
		KeytabSecret:  SecretReference{Name: "hdfs-keytab", Key: "pxf.keytab"},
		Krb5ConfigMap: "krb5-config",
		Realm:         "EXAMPLE.COM",
	}

	dst := src.DeepCopy()
	require.NotNil(t, dst)
	assert.NotSame(t, src, dst, "deepcopy must allocate a new value")
	assert.Equal(t, *src, *dst)

	// Independence: mutating the copy must not affect the source.
	dst.Principal = "other@REALM"
	dst.KeytabSecret.Name = "other-secret"
	dst.Krb5ConfigMap = "other-cm"
	dst.Realm = "OTHER"
	assert.Equal(t, "pxf/_HOST@EXAMPLE.COM", src.Principal)
	assert.Equal(t, "hdfs-keytab", src.KeytabSecret.Name)
	assert.Equal(t, "krb5-config", src.Krb5ConfigMap)
	assert.Equal(t, "EXAMPLE.COM", src.Realm)
}

// TestPxfKerberosSpec_DeepCopy_Nil proves the nil-receiver branch returns nil.
func TestPxfKerberosSpec_DeepCopy_Nil(t *testing.T) {
	var s *PxfKerberosSpec
	assert.Nil(t, s.DeepCopy())
}

// TestPxfServerSpec_DeepCopy_KerberosRoundtrip proves the PxfServerSpec deepcopy
// deep-copies the optional Kerberos pointer (independent allocation).
func TestPxfServerSpec_DeepCopy_KerberosRoundtrip(t *testing.T) {
	src := &PxfServerSpec{
		Name: "hdfs-kerb",
		Type: "hdfs",
		Config: map[string]string{
			"fs.defaultFS": "hdfs://nn:8020",
		},
		Kerberos: &PxfKerberosSpec{
			Principal:    "pxf/_HOST@R",
			KeytabSecret: SecretReference{Name: "kt", Key: "k"},
		},
	}

	dst := src.DeepCopy()
	require.NotNil(t, dst)
	require.NotNil(t, dst.Kerberos)
	assert.NotSame(t, src.Kerberos, dst.Kerberos, "Kerberos pointer must be deep-copied")
	assert.Equal(t, *src.Kerberos, *dst.Kerberos)

	// Independence: mutating the copy's Kerberos must not affect the source.
	dst.Kerberos.Principal = "mutated@R"
	assert.Equal(t, "pxf/_HOST@R", src.Kerberos.Principal)

	// A server WITHOUT Kerberos copies as nil (honest absence).
	plain := &PxfServerSpec{Name: "s3", Type: "s3"}
	assert.Nil(t, plain.DeepCopy().Kerberos)
}

// TestPxfSpec_DeepCopy_DataLoaderRoleRoundtrip proves PxfSpec.DataLoaderRole
// (SE.6) roundtrips through the generated deepcopy.
func TestPxfSpec_DeepCopy_DataLoaderRoleRoundtrip(t *testing.T) {
	src := &PxfSpec{
		Enabled:        true,
		Image:          "cloudberry/pxf:2.1.0",
		DataLoaderRole: "cb_dataload",
		Servers: []PxfServerSpec{
			{Name: "s3", Type: "s3"},
		},
	}

	dst := src.DeepCopy()
	require.NotNil(t, dst)
	assert.Equal(t, "cb_dataload", dst.DataLoaderRole)

	// Independence: the Servers slice and DataLoaderRole are independent.
	dst.DataLoaderRole = "other"
	dst.Servers[0].Name = "mutated"
	assert.Equal(t, "cb_dataload", src.DataLoaderRole)
	assert.Equal(t, "s3", src.Servers[0].Name)

	// Default (unset) DataLoaderRole stays empty through a roundtrip.
	plain := &PxfSpec{Enabled: true, Image: "x"}
	assert.Empty(t, plain.DeepCopy().DataLoaderRole)
}
