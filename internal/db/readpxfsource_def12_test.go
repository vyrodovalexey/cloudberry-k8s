package db

// DEF-12 regression (reported by TASK-U022 e2e run 5): buildPXFSampleLocation
// percent-encoded the PXF PROFILE option via url.QueryEscape, so the live form
// "s3:text" reached PXF as "s3%3atext" and the preview read failed with
// "s3%3atext is not defined in pxf-profiles.xml". PXF matches the PROFILE and
// SERVER option tokens literally (against pxf-profiles.xml / the servers
// directory), so the LOCATION must carry them VERBATIM — exactly the grammar the
// job DDL builder (builder.buildPXFLocation) emits live. These tests pin:
//   - special-character profile/server/resource values survive UNESCAPED in the
//     rendered LOCATION, both at the pure-function level and in the DDL actually
//     executed by ReadPXFSourceSample against the in-process PostgreSQL mock;
//   - the SQL-injection defense that replaces URI-escaping: embedded single
//     quotes are doubled inside the single-quoted LOCATION literal.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildPXFSampleLocation_ProfileServerResourceVerbatim pins the DEF-12 fix
// at the pure-function level: every component is emitted verbatim, byte-identical
// to the builder's live-proven job grammar — never percent-encoded.
func TestBuildPXFSampleLocation_ProfileServerResourceVerbatim(t *testing.T) {
	tests := []struct {
		name     string
		server   string
		profile  string
		resource string
		want     string
	}{
		{
			// The exact live form that failed before the fix (operator log:
			// "s3%3atext is not defined in pxf-profiles.xml").
			name:     "colon-bearing profile (DEF-12 live form)",
			server:   "s3-datalake",
			profile:  "s3:text",
			resource: "data-lake/events/",
			want:     "pxf://data-lake/events/?PROFILE=s3:text&SERVER=s3-datalake",
		},
		{
			name:     "mixed-case colon profile",
			server:   "hadoop-cluster",
			profile:  "hdfs:SequenceFile",
			resource: "/warehouse/raw/events",
			want:     "pxf:///warehouse/raw/events?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster",
		},
		{
			name:     "profile with space",
			server:   "srv",
			profile:  "my profile",
			resource: "data/events.csv",
			want:     "pxf://data/events.csv?PROFILE=my profile&SERVER=srv",
		},
		{
			name:     "profile with unicode",
			server:   "srv",
			profile:  "s3:tëxt",
			resource: "data/events.csv",
			want:     "pxf://data/events.csv?PROFILE=s3:tëxt&SERVER=srv",
		},
		{
			name:     "profile with plus and dot",
			server:   "srv",
			profile:  "a+b.c",
			resource: "data/events.csv",
			want:     "pxf://data/events.csv?PROFILE=a+b.c&SERVER=srv",
		},
		{
			name:     "colon-bearing server name",
			server:   "srv:1",
			profile:  "s3:parquet",
			resource: "bucket/data/",
			want:     "pxf://bucket/data/?PROFILE=s3:parquet&SERVER=srv:1",
		},
		{
			name:     "s3a-style resource path",
			server:   "s3-datalake",
			profile:  "s3:parquet",
			resource: "s3a://data-lake/events/",
			want:     "pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake",
		},
		{
			name:     "empty server omits SERVER option",
			server:   "",
			profile:  "jdbc",
			resource: "sales.orders",
			want:     "pxf://sales.orders?PROFILE=jdbc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPXFSampleLocation(tt.server, tt.profile, tt.resource)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "%",
				"no component of a pxf:// LOCATION may be percent-encoded (DEF-12)")
		})
	}
}

// TestPgxClient_ReadPXFSourceSample_LocationVerbatimInDDL proves the fix on the
// live-behavior path used by the existing client tests: ReadPXFSourceSample
// executes CREATE EXTERNAL TABLE with the special-character PROFILE/SERVER
// values VERBATIM inside the LOCATION literal — no percent-escapes anywhere in
// the emitted DDL.
func TestPgxClient_ReadPXFSourceSample_LocationVerbatimInDDL(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return emptyRowResponse([]string{"line"})
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	sample, err := client.ReadPXFSourceSample(
		context.Background(), "s3-datalake", "s3:text", "data-lake/events/", 10)
	require.NoError(t, err)
	require.NotNil(t, sample)

	// The executed DDL carries the exact builder-grammar LOCATION: profile and
	// server verbatim, nothing percent-encoded.
	assert.True(t, log.sawContaining(
		"LOCATION ('pxf://data-lake/events/?PROFILE=s3:text&SERVER=s3-datalake')"),
		"LOCATION must carry PROFILE/SERVER verbatim (DEF-12)")
	assert.False(t, log.sawContaining("%3"),
		"no percent-escapes may appear in the executed DDL")
}

// TestPgxClient_ReadPXFSourceSample_LocationSingleQuotesDoubled pins the
// injection defense that replaces URI-escaping: a single quote embedded in the
// resource or profile is doubled inside the single-quoted LOCATION literal, so
// a crafted value cannot break out of the SQL string.
func TestPgxClient_ReadPXFSourceSample_LocationSingleQuotesDoubled(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return emptyRowResponse([]string{"line"})
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	_, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "p'ro", "data/ev'ents.csv', FORMAT 'CSV'--", 10)
	require.NoError(t, err)

	// Every embedded single quote is doubled (SQL escape) inside the literal...
	assert.True(t, log.sawContaining(
		"LOCATION ('pxf://data/ev''ents.csv'', FORMAT ''CSV''--?PROFILE=p''ro&SERVER=s3srv')"),
		"embedded single quotes must be doubled inside the LOCATION literal")
	// ...and the crafted quote never terminates the literal early: the FORMAT
	// clause of the DDL itself (not the injected one) is the last FORMAT token.
	assert.True(t, log.sawContaining("FORMAT 'TEXT' (DELIMITER E'\\x01');"),
		"the DDL FORMAT clause must remain the statement's own")
}
