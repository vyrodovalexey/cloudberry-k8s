package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ============================================================================
// Scenario 111 — SE.4: PXF server Kerberos admission rules (111-SE4-U
// validation). validatePxfServerKerberos gates strictly on Kerberos != nil:
//   - Kerberos on hdfs/hive/hbase with principal+keytab (Name+Key) → ADMIT.
//   - Kerberos missing principal / keytab name / keytab key → REJECT.
//   - Kerberos on s3/jdbc (unsupported type) → REJECT.
//   - No-Kerberos server → unaffected (admit).
// All cases run through the public validateDataLoading path used by the W.*
// tests. server index [2] in validDataLoadingSpec is the hdfs server.
// ============================================================================

// validKerberos returns a complete, admissible PxfKerberosSpec.
func validKerberos() *cbv1alpha1.PxfKerberosSpec {
	return &cbv1alpha1.PxfKerberosSpec{
		Principal:    "pxf/_HOST@EXAMPLE.COM",
		KeytabSecret: cbv1alpha1.SecretReference{Name: "hdfs-keytab", Key: "pxf.keytab"},
		Realm:        "EXAMPLE.COM",
	}
}

func TestValidatePxfServerKerberos_Scenario111(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			// SE.4 admit: Kerberos on the hdfs server with principal + keytab.
			name: "SE.4 kerberos on hdfs with principal+keytab admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[2].Kerberos = validKerberos()
			}),
			expectErr: false,
		},
		{
			// SE.4 admit: Kerberos on a hive server.
			name: "SE.4 kerberos on hive admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = []cbv1alpha1.PxfServerSpec{
					{Name: "hv", Type: "hive", Kerberos: validKerberos()},
				}
				dl.Jobs[0].PxfJob.Server = "hv"
				dl.Jobs[0].PxfJob.Profile = "hive"
			}),
			expectErr: false,
		},
		{
			// SE.4 admit: Kerberos on an hbase server.
			name: "SE.4 kerberos on hbase admitted",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers = []cbv1alpha1.PxfServerSpec{
					{Name: "hb", Type: "hbase", Kerberos: validKerberos()},
				}
				dl.Jobs[0].PxfJob.Server = "hb"
				dl.Jobs[0].PxfJob.Profile = "hbase"
			}),
			expectErr: false,
		},
		{
			// SE.4 reject: Kerberos set but principal missing (descriptive error).
			name: "SE.4 kerberos missing principal rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				k := validKerberos()
				k.Principal = ""
				dl.Pxf.Servers[2].Kerberos = k
			}),
			expectErr:   true,
			errContains: "kerberos.principal",
		},
		{
			// SE.4 reject: Kerberos set but keytab Secret name missing.
			name: "SE.4 kerberos missing keytabSecret name rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				k := validKerberos()
				k.KeytabSecret.Name = ""
				dl.Pxf.Servers[2].Kerberos = k
			}),
			expectErr:   true,
			errContains: "kerberos.keytabSecret.name",
		},
		{
			// SE.4 reject: Kerberos set but keytab Secret key missing.
			name: "SE.4 kerberos missing keytabSecret key rejected",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				k := validKerberos()
				k.KeytabSecret.Key = ""
				dl.Pxf.Servers[2].Kerberos = k
			}),
			expectErr:   true,
			errContains: "kerberos.keytabSecret.key",
		},
		{
			// SE.4 reject: Kerberos on an s3 server (unsupported type).
			name: "SE.4 kerberos on s3 rejected (unsupported type)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Kerberos = validKerberos()
			}),
			expectErr:   true,
			errContains: "does not support kerberos",
		},
		{
			// SE.4 reject: Kerberos on a jdbc server (unsupported type).
			name: "SE.4 kerberos on jdbc rejected (unsupported type)",
			cluster: clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[1].Kerberos = validKerberos()
			}),
			expectErr:   true,
			errContains: "does not support kerberos",
		},
		{
			// Honesty: a no-Kerberos server is entirely unaffected (admit). This
			// is the baseline — Kerberos==nil must add no rejection.
			name:      "SE.4 no kerberos server unaffected (admit)",
			cluster:   clusterWithDataLoading(nil),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDataLoading(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidatePxfServerKerberos_DirectNilNoOp proves the helper is a strict
// no-op when Kerberos is nil (defense for the "gate on Kerberos != nil" rule).
func TestValidatePxfServerKerberos_DirectNilNoOp(t *testing.T) {
	srv := &cbv1alpha1.PxfServerSpec{Name: "s3", Type: "s3"}
	assert.NoError(t, validatePxfServerKerberos(srv, 0))
}
