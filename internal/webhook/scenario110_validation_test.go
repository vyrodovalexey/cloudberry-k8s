package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// Scenario 110 — Webhook Validation (All Rules) (W.1–W.15).
//
// This is the COMPLETE, systematic negative matrix proving that EACH of the 15
// data-loading webhook rules rejects an otherwise-valid CloudberryCluster that
// carries EXACTLY ONE violation, with a DESCRIPTIVE (field-path + reason) error.
//
// It drives the SAME public validator entrypoint the real admission chain uses
// — CloudberryClusterValidator.ValidateCreate — so the assertions exercise
// validateCreate → validateCluster → validateDataLoading and all the
// type-specific validators (validatePxfServerType / validateDataLoadingJobs /
// validateErrorHandling, etc.) end-to-end.
//
// Source intent (see task-breakdown §1): W.3/W.8/W.15 are ALSO constrained by
// the CRD OpenAPI enum at a LIVE apiserver, so on a live apply they are rejected
// by the SCHEMA before the webhook runs. The webhook nevertheless implements
// them defensively (validatePxfServerType / validateDataLoadingJobs /
// validateErrorHandling). Because this unit test calls the validator DIRECTLY
// (no apiserver/schema), it asserts the validator's DESCRIPTIVE defense-in-depth
// message — proving the rule cannot silently rot if the CRD enum is ever
// relaxed.
//
// Catalog IDs covered (all 15 + sub-cases + control):
//
//	110-W1-U, 110-W2-empty-U, 110-W2-dup-U, 110-W3-U, 110-W4-endpoint-U,
//	110-W4-creds-U, 110-W5-driver-U, 110-W5-url-U, 110-W6-U, 110-W7-empty-U,
//	110-W7-dup-U, 110-W8-U, 110-W9-U, 110-W10-U, 110-W11-U, 110-W12-U,
//	110-W13-U, 110-W14-U, 110-W15-U, 110-CONTROL-admit.

// valid110Cluster returns a fully-valid CloudberryCluster with data loading
// enabled: PXF enabled with an image, ONE valid s3 server (endpoint +
// credentialSecrets), ONE valid jdbc server (driver + url), ONE valid hdfs
// server (defaultFS), ONE valid pxf job (valid server/profile/targetTable), and
// ONE valid gpload job (targetTable). Every Scenario 110 negative case mutates a
// COPY of this baseline to introduce exactly ONE violation, so each test
// isolates a single rule. The 110-CONTROL-admit case proves this baseline is
// itself valid (passes ValidateCreate with no error).
func valid110Cluster() *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake", Type: "s3",
					Config:            map[string]string{"fs.s3a.endpoint": "s3.amazonaws.com"},
					CredentialSecrets: []cbv1alpha1.SecretReference{{Name: "s3-creds"}},
				},
				{
					Name: "mysql-oltp", Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/db",
					},
				},
				{
					Name: "hadoop-cluster", Type: "hdfs",
					Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name: "s3-ingest", Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server: "s3-datalake", Profile: "s3:parquet", TargetTable: "public.events",
				},
			},
			{
				Name: "csv-load", Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{TargetTable: "public.raw_data"},
			},
		},
	}
	return c
}

// mutate110 returns a valid110Cluster baseline with the single-field violation
// applied by fn (operating on the DataLoadingSpec).
func mutate110(fn func(dl *cbv1alpha1.DataLoadingSpec)) *cbv1alpha1.CloudberryCluster {
	c := valid110Cluster()
	if fn != nil {
		fn(c.Spec.DataLoading)
	}
	return c
}

// TestScenario110_WebhookValidationMatrix is the systematic W.1–W.15 negative
// matrix + control. Every row builds the shared valid110Cluster baseline,
// applies EXACTLY ONE violation, and asserts ValidateCreate returns a non-nil
// error whose message contains the descriptive (field path + reason) substring.
// The 110-CONTROL-admit row asserts the untouched baseline ADMITS (no error),
// proving each negative isolates a single rule.
func TestScenario110_WebhookValidationMatrix(t *testing.T) {
	tests := []struct {
		// id is the Scenario 110 catalog ID (110-W{n}-U with sub-case suffixes).
		id string
		// cluster is the CR under test (baseline + one violation, or the
		// untouched baseline for the control).
		cluster *cbv1alpha1.CloudberryCluster
		// expectErr is true for the negative rows, false for the control.
		expectErr bool
		// wantSubstrings are ALL required to appear in the error message — the
		// field path AND the reason — proving the error is descriptive.
		wantSubstrings []string
	}{
		// 110-CONTROL-admit: the untouched valid baseline passes ValidateCreate.
		{
			id:        "110-CONTROL-admit",
			cluster:   valid110Cluster(),
			expectErr: false,
		},

		// W.1 — pxf.enabled + empty pxf.image.
		{
			id: "110-W1-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Image = ""
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.pxf.image is required when pxf.enabled is true"},
		},

		// W.2 — server name empty (sub-case 1).
		{
			id: "110-W2-empty-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Name = ""
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.pxf.servers[0].name", "is required"},
		},
		// W.2 — duplicate server name (sub-case 2).
		{
			id: "110-W2-dup-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.pxf.servers[1].name", "is a duplicate"},
		},

		// W.3 — server type not in enum (validator defense-in-depth message).
		{
			id: "110-W3-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].Type = "ftp"
			}),
			expectErr: true,
			wantSubstrings: []string{
				"dataLoading.pxf.servers[0].type must be one of", `"ftp"`,
			},
		},

		// W.4 — s3 server missing fs.s3a.endpoint (sub-case 1).
		{
			id: "110-W4-endpoint-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
			}),
			expectErr:      true,
			wantSubstrings: []string{"must include", `"fs.s3a.endpoint"`},
		},
		// W.4 — s3 server missing credentialSecrets (sub-case 2).
		{
			id: "110-W4-creds-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].CredentialSecrets = nil
			}),
			expectErr:      true,
			wantSubstrings: []string{"must include", "credentialSecrets"},
		},

		// W.5 — jdbc server missing jdbc.driver (sub-case 1).
		{
			id: "110-W5-driver-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[1].Config, "jdbc.driver")
			}),
			expectErr:      true,
			wantSubstrings: []string{"must include", `"jdbc.driver"`},
		},
		// W.5 — jdbc server missing jdbc.url (sub-case 2).
		{
			id: "110-W5-url-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[1].Config, "jdbc.url")
			}),
			expectErr:      true,
			wantSubstrings: []string{"must include", `"jdbc.url"`},
		},

		// W.6 — hdfs server missing fs.defaultFS.
		{
			id: "110-W6-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[2].Config, "fs.defaultFS")
			}),
			expectErr:      true,
			wantSubstrings: []string{"must include", `"fs.defaultFS"`},
		},

		// W.7 — job name empty (sub-case 1).
		{
			id: "110-W7-empty-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Name = ""
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.jobs[0].name", "is required"},
		},
		// W.7 — duplicate job name (sub-case 2).
		{
			id: "110-W7-dup-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].Name = dl.Jobs[0].Name
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.jobs[1].name", "is a duplicate"},
		},

		// W.8 — job type not in enum (validator defense-in-depth message).
		{
			id: "110-W8-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Type = "spark"
			}),
			expectErr: true,
			wantSubstrings: []string{
				`dataLoading.jobs[0].type must be "pxf" or "gpload"`, `"spark"`,
			},
		},

		// W.9 — pxfJob.server references an undefined server.
		{
			id: "110-W9-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "does-not-exist"
			}),
			expectErr: true,
			wantSubstrings: []string{
				"dataLoading.jobs[0].pxfJob.server",
				"does not reference a defined pxf.servers[].name",
			},
		},

		// W.10 — pxfJob.profile is not a valid PXF profile.
		{
			id: "110-W10-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
			}),
			expectErr: true,
			wantSubstrings: []string{
				"dataLoading.jobs[0].pxfJob.profile", "is not a valid PXF",
			},
		},

		// W.11 — pxfJob.targetTable empty (webhook-reachable empty-string expr).
		{
			id: "110-W11-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.TargetTable = ""
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.jobs[0].pxfJob.targetTable is required"},
		},

		// W.12 — gploadJob.targetTable empty (webhook-reachable empty-string expr).
		{
			id: "110-W12-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.TargetTable = ""
			}),
			expectErr:      true,
			wantSubstrings: []string{"dataLoading.jobs[1].gploadJob.targetTable is required"},
		},

		// W.13 — schedule is not a valid cron expression.
		{
			id: "110-W13-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].Schedule = "not a cron"
			}),
			expectErr: true,
			wantSubstrings: []string{
				"dataLoading.jobs[0].schedule is not a valid cron expression",
			},
		},

		// W.14 — partitioning column set without range/interval.
		{
			id: "110-W14-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning = &cbv1alpha1.PartitioningSpec{
					Column: "order_date",
				}
			}),
			expectErr: true,
			wantSubstrings: []string{
				"dataLoading.jobs[0].pxfJob.partitioning requires column, range, and interval together",
			},
		},

		// W.15 — segmentRejectLimitType not rows/percent (validator DiD message).
		{
			id: "110-W15-U",
			cluster: mutate110(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit: 100, SegmentRejectLimitType: "fraction",
				}
			}),
			expectErr: true,
			wantSubstrings: []string{
				`segmentRejectLimitType must be "rows" or "percent"`, `"fraction"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			// reader=nil: skip the duplicate-name List so the test isolates the
			// data-loading validation under test (mirrors the existing harness).
			v := NewCloudberryClusterValidator(nil)
			warnings, err := v.ValidateCreate(context.Background(), tt.cluster)

			if !tt.expectErr {
				require.NoError(t, err, "%s: baseline must ADMIT", tt.id)
				return
			}

			require.Error(t, err, "%s: expected a rejection", tt.id)
			for _, want := range tt.wantSubstrings {
				assert.Contains(t, err.Error(), want,
					"%s: error must be descriptive and contain %q; got %q",
					tt.id, want, err.Error())
			}
			_ = warnings
		})
	}
}
