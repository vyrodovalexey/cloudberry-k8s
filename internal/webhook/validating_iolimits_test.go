package webhook

// Tests for T5/C1b: TablespaceIOLimitSpec.Tablespace is validated against
// ^[A-Za-z_][A-Za-z0-9_]*$|^\*$ in validateWorkload (the CRD pattern marker is
// the defense-in-depth twin). The value is embedded in the rendered io_limit
// DDL string, so free-form text is an SQL-injection surface.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ioLimitCluster returns a workload-enabled cluster with a single resource
// group carrying one io-limit for the given tablespace.
func ioLimitCluster(tablespace string) *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{
				Name: "analytics",
				IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
					{Tablespace: tablespace, ReadIOPS: 100},
				},
			},
		},
	}
	return c
}

func TestValidateWorkload_IOLimitTablespace(t *testing.T) {
	tests := []struct {
		name        string
		tablespace  string
		expectErr   bool
		errContains string
	}{
		// Valid identifiers and the wildcard.
		{name: "plain identifier", tablespace: "data_ts"},
		{name: "leading underscore", tablespace: "_t1"},
		{name: "single letter", tablespace: "t"},
		{name: "wildcard", tablespace: "*"},
		{name: "mixed case with digits", tablespace: "Fast_TS_01"},

		// Invalid values, incl. injection attempts.
		{
			name: "quote injection", tablespace: "x'; DROP TABLE--",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
		{
			name: "semicolon injection", tablespace: "x;DROP",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
		{
			name: "space", tablespace: "has space",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
		{
			name: "empty", tablespace: "",
			expectErr: true, errContains: "ioLimits[0].tablespace is required",
		},
		{
			name: "leading digit", tablespace: "1abc",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
		{
			name: "wildcard with suffix", tablespace: "*x",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
		{
			name: "io_limit format metacharacter", tablespace: "ts:rbps=1",
			expectErr: true, errContains: "ioLimits[0].tablespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkload(ioLimitCluster(tt.tablespace))
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Contains(t, err.Error(), "workload.resourceGroups[0]",
					"error must carry the indexed field path")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestValidateWorkload_IOLimitIndexedPath pins the field-indexed message for
// a later resource group / io-limit entry.
func TestValidateWorkload_IOLimitIndexedPath(t *testing.T) {
	c := newValidCluster()
	c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "etl"},
			{
				Name: "analytics",
				IOLimits: []cbv1alpha1.TablespaceIOLimitSpec{
					{Tablespace: "ok_ts"},
					{Tablespace: "bad name"},
				},
			},
		},
	}

	err := validateWorkload(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workload.resourceGroups[1].ioLimits[1].tablespace")
}
