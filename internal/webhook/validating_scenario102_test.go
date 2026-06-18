package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// boolPtr returns a pointer to b (test helper for the *bool Continuous field).
func boolPtr(b bool) *bool { return &b }

// kafkaConnectorDataLoading returns a DataLoadingSpec mutator that installs a
// single custom-connector-backed kafka server + a kafka pxf job, mirroring the
// Scenario 102 sample CR (§6.2). The base spec keeps the s3/jdbc/hdfs servers so
// the existing baseline shape is preserved; the kafka pieces are appended/added
// by the helper and then further mutated by the per-case fn.
func kafkaConnectorDataLoading(fn func(dl *cbv1alpha1.DataLoadingSpec)) *cbv1alpha1.CloudberryCluster {
	return clusterWithDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
		// A custom server kafka-connector backed by a matching customConnectors[]
		// entry of the same name (the W.23/W.24 link is by NAME).
		dl.Pxf.CustomConnectors = []cbv1alpha1.PxfCustomConnector{
			{Name: "kafka-connector", JarURL: "s3://cloudberry-data/connectors/kafka-connector.jar"},
		}
		dl.Pxf.Servers = append(dl.Pxf.Servers, cbv1alpha1.PxfServerSpec{
			Name: "kafka-connector", Type: "custom",
		})
		// The kafka streaming job referencing the connector-backed custom server.
		dl.Jobs = append(dl.Jobs, cbv1alpha1.DataLoadingJob{
			Name: "kafka-cdc", Type: "pxf",
			PxfJob: &cbv1alpha1.PxfJobSpec{
				Server:        "kafka-connector",
				Profile:       "kafka",
				Resource:      "cloudberry-cdc",
				TargetTable:   "public.kafka_events",
				Continuous:    boolPtr(true),
				BatchSize:     10000,
				FlushInterval: "30s",
			},
		})
		if fn != nil {
			fn(dl)
		}
	})
}

// lastJob returns the kafka-cdc job (the last appended job) for per-case mutation.
func lastJob(dl *cbv1alpha1.DataLoadingSpec) *cbv1alpha1.DataLoadingJob {
	return &dl.Jobs[len(dl.Jobs)-1]
}

// lastServer returns the kafka-connector server (the last appended server).
func lastServer(dl *cbv1alpha1.DataLoadingSpec) *cbv1alpha1.PxfServerSpec {
	return &dl.Pxf.Servers[len(dl.Pxf.Servers)-1]
}

// TestValidateDataLoading_Scenario102 exercises the Scenario 102 webhook rules
// added for the kafka custom-connector / continuous-streaming path:
//   - W.3 custom server type + W.24 (custom-server-requires-connector) [U1]
//   - W.10 recognition of the kafka scheme + W.23 (kafka-profile-requires-custom
//     -connector) accept/reject matrix [U2]
//   - W.23c continuous/batchSize/flushInterval validation [U3]
//
// All cases run through the public validateDataLoading path (the same way the
// existing W.* table tests do).
func TestValidateDataLoading_Scenario102(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		// ---- U1: W.3 custom + W.24 -------------------------------------------
		{
			// SC102-J41-SERVER-CUSTOM / SC102-J42-PROFILE-OK: a type=custom server
			// WITH a matching customConnectors[] entry + a kafka job is ACCEPTED.
			name:      "U1 W.3 custom server with matching connector + kafka job accepted",
			cluster:   kafkaConnectorDataLoading(nil),
			expectErr: false,
		},
		{
			// SC102-J41-SERVER-NOCONN: a type=custom server WITHOUT a matching
			// customConnectors[] entry is REJECTED by W.24. The kafka job is
			// dropped so the failure is unambiguously the server-side guard.
			name: "U1 W.24 custom server without matching connector rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.CustomConnectors = nil
				// Drop the kafka job: with no connector the server itself is
				// rejected first (W.24), independent of any job.
				dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
			}),
			expectErr:   true,
			errContains: "of type custom requires a matching customConnectors",
		},
		{
			// W.24 names the offending server by its connector name in the message.
			name: "U1 W.24 message names the connector",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.CustomConnectors = nil
				dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
			}),
			expectErr:   true,
			errContains: `"kafka-connector"`,
		},
		{
			// A custom server whose connector name does NOT match (mismatched
			// name) is still rejected by W.24 — the link is by NAME.
			name: "U1 W.24 mismatched connector name rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.CustomConnectors = []cbv1alpha1.PxfCustomConnector{
					{Name: "other-connector", JarURL: "s3://x/other.jar"},
				}
				dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
			}),
			expectErr:   true,
			errContains: "of type custom requires a matching customConnectors",
		},

		// ---- U2: W.10 recognition + W.23 accept/reject matrix ----------------
		{
			// SC102-J42-PROFILE-NOCONN (a): a kafka profile whose server is
			// type=custom but has NO matching connector is rejected (W.24 trips
			// first on the server, but the profile is also gated by W.23). Here the
			// server is type=s3 (a non-custom server): the kafka profile is
			// REJECTED by W.23.
			name: "U2 W.23 kafka profile on non-custom (s3) server rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				// Point the kafka job at the s3 server instead of the custom one.
				lastJob(dl).PxfJob.Server = "s3-datalake"
			}),
			expectErr:   true,
			errContains: "custom-connector profile",
		},
		{
			name: "U2 W.23 kafka profile on non-custom (s3) server rejected (requires token)",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.Server = "s3-datalake"
			}),
			expectErr:   true,
			errContains: "requires the referenced server",
		},
		{
			// SC102-J42-PROFILE-NOCONN (b): a kafka profile referencing a custom
			// server that has NO matching connector. The server-side W.24 rejects
			// first; the error names the missing connector. (Guards "no built-in
			// streaming": kafka without a connector never admits.)
			name: "U2 W.24/W.23 kafka profile with custom server but no connector rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.CustomConnectors = nil
			}),
			expectErr:   true,
			errContains: "of type custom requires a matching customConnectors",
		},
		{
			// rabbitmq is the other recognized custom-connector scheme: on a
			// connector-backed custom server it is ACCEPTED (W.10 recognizes it,
			// W.23 passes). Non-continuous so no streaming-param fuss.
			name: "U2 W.10/W.23 rabbitmq profile on connector-backed server accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.Profile = "rabbitmq"
				lastJob(dl).PxfJob.Continuous = nil
				lastJob(dl).PxfJob.BatchSize = 0
				lastJob(dl).PxfJob.FlushInterval = ""
			}),
			expectErr: false,
		},

		// ---- U3: W.23c continuous / batchSize / flushInterval ----------------
		{
			// batchSize unset (0) is OK (kubebuilder Min=1 only constrains a set
			// value; 0 means "use the loader default").
			name: "U3 W.23c batchSize unset (0) accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.BatchSize = 0
			}),
			expectErr: false,
		},
		{
			name: "U3 W.23c batchSize 10000 accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.BatchSize = 10000
			}),
			expectErr: false,
		},
		{
			// A negative batchSize (cannot arise from kubebuilder Min=1 but the
			// webhook guards it defensively) is REJECTED by W.23c.
			name: "U3 W.23c negative batchSize rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.BatchSize = -1
			}),
			expectErr:   true,
			errContains: "must be >= 1",
		},
		{
			name: "U3 W.23c flushInterval 30s accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.FlushInterval = "30s"
			}),
			expectErr: false,
		},
		{
			name: "U3 W.23c flushInterval 1m accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.FlushInterval = "1m"
			}),
			expectErr: false,
		},
		{
			name: "U3 W.23c flushInterval empty accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.FlushInterval = ""
			}),
			expectErr: false,
		},
		{
			name: "U3 W.23c flushInterval non-duration rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.FlushInterval = "banana"
			}),
			expectErr:   true,
			errContains: "must be a valid duration",
		},
		{
			// SC102-J43-CONTINUOUS-W23c: Continuous=true + a non-empty Schedule is
			// REJECTED (protects J.46 "Job NOT CronJob" at admission time).
			name: "U3 W.23c continuous + schedule rejected",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).Schedule = "*/5 * * * *"
			}),
			expectErr:   true,
			errContains: "continuous streaming jobs must not set a schedule",
		},
		{
			// Continuous=true with NO schedule is OK (the kafka-cdc happy path).
			name: "U3 W.23c continuous without schedule accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).Schedule = ""
			}),
			expectErr: false,
		},
		{
			// A NON-continuous kafka job MAY carry a schedule (the W.23c
			// continuous/schedule mutual-exclusion only trips when Continuous=true).
			name: "U3 W.23c non-continuous kafka job with schedule accepted",
			cluster: kafkaConnectorDataLoading(func(dl *cbv1alpha1.DataLoadingSpec) {
				lastJob(dl).PxfJob.Continuous = boolPtr(false)
				lastJob(dl).Schedule = "*/5 * * * *"
			}),
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

// TestScenario102_ProfileRecognition asserts the IN-PACKAGE profile helpers
// directly (U2): the built-in W.10 allowlist (isValidPxfProfile) is UNCHANGED —
// "kafka" is NOT a built-in profile (no TestIsValidPxfProfile regression) — while
// the SEPARATE custom-connector recognizer (isCustomConnectorProfile) reports the
// kafka/rabbitmq streaming schemes as recognized. The two sets are disjoint by
// design.
func TestScenario102_ProfileRecognition(t *testing.T) {
	// W.10 built-in allowlist is undisturbed: kafka/rabbitmq are NOT built-in.
	assert.False(t, isValidPxfProfile("kafka"),
		"kafka must NOT be a built-in PXF profile (no W.10 table regression)")
	assert.False(t, isValidPxfProfile("rabbitmq"),
		"rabbitmq must NOT be a built-in PXF profile")

	// The custom-connector recognizer reports the streaming schemes (case-
	// insensitive, scheme is the part before the first ":").
	assert.True(t, isCustomConnectorProfile("kafka"))
	assert.True(t, isCustomConnectorProfile("KAFKA"))
	assert.True(t, isCustomConnectorProfile("kafka:json"))
	assert.True(t, isCustomConnectorProfile("rabbitmq"))

	// Built-in / unknown profiles are NOT custom-connector profiles.
	assert.False(t, isCustomConnectorProfile("s3:parquet"))
	assert.False(t, isCustomConnectorProfile("jdbc"))
	assert.False(t, isCustomConnectorProfile(""))
	assert.False(t, isCustomConnectorProfile("nonsense"))

	// The two recognizers are disjoint for the streaming schemes: a kafka profile
	// is recognized ONLY by the custom-connector path, never by the built-in one.
	for _, p := range []string{"kafka", "rabbitmq"} {
		assert.False(t, isValidPxfProfile(p))
		assert.True(t, isCustomConnectorProfile(p))
	}
}
