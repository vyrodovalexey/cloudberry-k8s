package pxfpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestModeWritableConst pins the writable-mode sentinel value so the webhook and
// builder (which both compare against it) can never silently diverge from the
// CRD documented value.
func TestModeWritableConst(t *testing.T) {
	assert.Equal(t, "writable", ModeWritable)
}

// TestFormatConstants pins the canonical lowercase format literals. These are
// re-used by the webhook's W.10 allowlist, so a change here would ripple to the
// admission policy and must be deliberate.
func TestFormatConstants(t *testing.T) {
	assert.Equal(t, "text", FormatText)
	assert.Equal(t, "parquet", FormatParquet)
	assert.Equal(t, "avro", FormatAvro)
	assert.Equal(t, "json", FormatJSON)
	assert.Equal(t, "orc", FormatORC)
	assert.Equal(t, "sequencefile", FormatSequenceFile)
	assert.Equal(t, "rc", FormatRC)
}

// TestWritableFormatsMembership asserts the WritableFormats set is EXACTLY
// {text, parquet, avro, sequencefile} per the spec Read/Write matrix — the
// writable formats are members and the read-only formats (json/orc/rc) are not.
func TestWritableFormatsMembership(t *testing.T) {
	// Members (Write=Yes).
	for _, f := range []string{FormatText, FormatParquet, FormatAvro, FormatSequenceFile} {
		_, ok := WritableFormats[f]
		assert.Truef(t, ok, "%q must be a writable format", f)
	}
	// Non-members (Write=No).
	for _, f := range []string{FormatJSON, FormatORC, FormatRC} {
		_, ok := WritableFormats[f]
		assert.Falsef(t, ok, "%q must NOT be a writable format", f)
	}
	// Exact size guards against accidental additions.
	assert.Len(t, WritableFormats, 4)
}

// TestReadOnlySchemesMembership asserts the readOnlySchemes set is EXACTLY
// {hive, hbase} per the spec Hadoop Profiles table: the Hive and HBase
// connectors are read-only at the SCHEME level (Write=No regardless of format).
// This pins the branch in IsProfileWritable that overrides the format check so
// hive:text (a writable format on other schemes) is still non-writable.
func TestReadOnlySchemesMembership(t *testing.T) {
	for _, s := range []string{"hive", "hbase"} {
		_, ok := readOnlySchemes[s]
		assert.Truef(t, ok, "%q must be a read-only scheme", s)
	}
	// Writable schemes are NOT read-only.
	for _, s := range []string{"s3", "gs", "abfss", "wasbs", "hdfs", "jdbc"} {
		_, ok := readOnlySchemes[s]
		assert.Falsef(t, ok, "%q must NOT be a read-only scheme", s)
	}
	// Exact size guards against accidental additions.
	assert.Len(t, readOnlySchemes, 2)
}

// TestIsProfileWritable is the table-driven write-capability matrix covering
// every scheme×format combination plus bare profiles, invalid/empty inputs and
// case-insensitivity. It is the single-source-of-truth predicate that drives
// both the admission webhook (FF.1-FF.5) and the builder defense-in-depth check.
func TestIsProfileWritable(t *testing.T) {
	tests := []struct {
		name    string
		profile string
		want    bool
	}{
		// ---- Writable object-store / hadoop formats (Write=Yes) ----
		{"s3 text writable", "s3:text", true},
		{"s3 parquet writable", "s3:parquet", true},
		{"s3 avro writable", "s3:avro", true},
		{"gs parquet writable", "gs:parquet", true},
		{"abfss avro writable", "abfss:avro", true},
		{"wasbs text writable", "wasbs:text", true},
		{"hdfs parquet writable", "hdfs:parquet", true},
		{"hdfs sequencefile writable", "hdfs:sequencefile", true},

		// ---- Scenario 97: explicit HDFS Hadoop write-matrix (HP.*/FF.7) ----
		// hdfs:{text,parquet,avro,sequencefile} are writable; hdfs:{json,orc}
		// are read-only. These pin the Hadoop scheme rows independently of the
		// object-store rows above so a change to either set is caught.
		{"hdfs text writable", "hdfs:text", true},
		{"hdfs avro writable", "hdfs:avro", true},

		// ---- Bare profiles ----
		// jdbc supports INSERT (writable); bare hive/hbase are read-only schemes
		// (members of readOnlySchemes) and are never writable.
		{"bare jdbc writable", "jdbc", true},
		{"bare hive read-only (read-only scheme)", "hive", false},
		{"bare hbase read-only (read-only scheme)", "hbase", false},

		// ---- Read-only formats (Write=No) ----
		{"s3 json read-only", "s3:json", false},
		{"s3 orc read-only", "s3:orc", false},
		{"gs json read-only", "gs:json", false},
		{"abfss orc read-only", "abfss:orc", false},
		{"hdfs json read-only", "hdfs:json", false},
		{"hdfs orc read-only", "hdfs:orc", false},

		// ---- Scenario 97: Hive profiles (WRej.3-6/FF.6b) ----
		// Hive/HBase are read-only SCHEMES — write-unsupported regardless of
		// format. Every hive* profile (bare hive, hive:text, hive:orc, hive:rc)
		// is NON-writable because the Hive connector row is Write=No, not because
		// of any per-format check. hive:text is non-writable even though "text"
		// is a writable format on hdfs/object stores: the read-only scheme
		// overrides the format. hive:rc is the FF.6b write-reject leg.
		{"hive orc read-only", "hive:orc", false},
		{"hive rc read-only", "hive:rc", false},
		// Hive/HBase are read-only schemes — write-unsupported regardless of
		// format. hive:text is rejected even though "text" is writable on other
		// schemes, because the Hive scheme is read-only end-to-end.
		{"hive text read-only (read-only scheme)", "hive:text", false},

		// ---- Invalid / empty inputs ----
		{"empty string", "", false},
		{"garbage bare token", "garbage", false},
		{"scheme with empty format", "s3:", false},
		// NOTE: IsProfileWritable is a pure FORMAT predicate — it does NOT
		// validate the scheme (that is the webhook's isValidPxfProfile / W.10
		// job, which runs FIRST and rejects ":text" before the writable check is
		// ever reached). So ":text" — an empty scheme with a writable format —
		// reports writable=true at this layer. This is by design and harmless:
		// no valid CR can deliver an empty-scheme profile here because W.10 gates
		// it. The test pins the real, shipped behavior (not a production bug).
		{"empty scheme with writable format is format-only true", ":text", true},
		{"empty scheme with read-only format is false", ":json", false},

		// ---- Case-insensitivity ----
		{"S3:PARQUET uppercase writable", "S3:PARQUET", true},
		{"S3:JSON uppercase read-only", "S3:JSON", false},
		{"Gs:Avro mixed-case writable", "Gs:Avro", true},
		{"JDBC uppercase bare writable", "JDBC", true},

		// ---- Scenario 97: Hadoop case-insensitivity (HP.6/HB.1/FF.6b) ----
		{"HDFS:SequenceFile uppercase writable", "HDFS:SequenceFile", true},
		{"Hdfs:Text mixed-case writable", "Hdfs:Text", true},
		{"HIVE:RC uppercase read-only", "HIVE:RC", false},
		// Read-only scheme is matched case-insensitively: HIVE:TEXT (a writable
		// format on other schemes) is still rejected for the Hive scheme.
		{"HIVE:TEXT uppercase read-only (read-only scheme)", "HIVE:TEXT", false},
		{"HBase mixed-case bare read-only", "HBase", false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equalf(t, tc.want, IsProfileWritable(tc.profile),
				"IsProfileWritable(%q)", tc.profile)
		})
	}
}
