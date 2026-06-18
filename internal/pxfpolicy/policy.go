// Package pxfpolicy is the SINGLE source of truth for the PXF profile
// write-capability policy (Scenario 96 — Object Store Profiles & Format
// Write-Capability). It encodes the Read/Write matrix from the data-loading
// spec exactly once so that BOTH the admission webhook (internal/webhook) and
// the DDL builder (internal/builder) can share it without duplicating the
// table.
//
// It is a deliberately tiny LEAF package with ZERO non-stdlib dependencies: the
// webhook already imports the builder, so a builder→webhook (or webhook→builder)
// policy dependency would create an import cycle. Both packages instead depend
// on this leaf, which depends on nothing in the project.
//
// # Write-capability matrix (spec lines 161-169 / 955-964)
//
// A PXF profile is "<scheme>:<format>" (e.g. s3:parquet) or a bare profile
// (e.g. jdbc). Whether a WRITABLE external table may be created for a profile
// is driven by the FORMAT for the object-store/Hadoop schemes:
//
//	Format        Read    Write
//	------------  -----   -----
//	Text          Yes     Yes
//	Parquet       Yes     Yes
//	Avro          Yes     Yes
//	SequenceFile  Yes     Yes
//	JSON          Yes     No
//	ORC           Yes     No
//	RCFile        Yes     No
//
// Object stores (s3/gs/abfss/wasbs) can only reach text/parquet/avro/json/orc
// (there is no s3:sequencefile), so for the object-store schemes the writable
// set effectively reduces to {text, parquet, avro} and json/orc are rejected as
// write-unsupported.
//
// The Hive and HBase connectors are READ-ONLY at the SCHEME level: every hive*
// profile (hive, hive:text, hive:orc, hive:rc) and the HBase profile are
// Write=No regardless of format, so they are never writable. Bare profiles
// otherwise follow their connector row: jdbc is writable (JDBC supports INSERT).
//
// The API is intentionally minimal, deterministic and side-effect-free (pure
// functions over package-level data); no observability is needed here.
package pxfpolicy

import "strings"

// ModeWritable is the PxfJobSpec.Mode sentinel that selects a WRITABLE external
// table (data export) instead of a read/import. It is shared by the webhook
// (admission enforcement) and the builder (writable DDL emission).
const ModeWritable = "writable"

// Canonical, lowercase PXF format constants. These are the SAME literal values
// the webhook's W.10 profile allowlist uses; both refer to these constants so
// the values can never diverge.
const (
	FormatText         = "text"
	FormatParquet      = "parquet"
	FormatAvro         = "avro"
	FormatJSON         = "json"
	FormatORC          = "orc"
	FormatSequenceFile = "sequencefile"
	FormatRC           = "rc"
)

// WritableFormats is the set of formats for which a WRITABLE external table is
// supported, per the spec Read/Write matrix: text, parquet, avro and
// SequenceFile have Write=Yes; json, orc and rc have Write=No.
//
// For the object-store schemes (s3/gs/abfss/wasbs) only text/parquet/avro are
// reachable (no object-store SequenceFile), so json/orc fall outside this set
// and are rejected as write-unsupported.
//
// The map is treated as read-only after package initialization; callers must
// never mutate it.
var WritableFormats = map[string]struct{}{
	FormatText:         {},
	FormatParquet:      {},
	FormatAvro:         {},
	FormatSequenceFile: {},
}

// bareWritableProfiles is the set of bare profiles (no "<scheme>:<format>"
// suffix) that support writes per their connector row. jdbc supports INSERT and
// is therefore writable; bare hbase/hive are read-only and are intentionally
// absent.
var bareWritableProfiles = map[string]struct{}{
	"jdbc": {},
}

// readOnlySchemes is the set of profile SCHEMES whose connector is read-only
// REGARDLESS of the format suffix, per the spec PXF Profile Reference (Hadoop
// Profiles table): the Hive connector (hive, hive:text, hive:orc, hive:rc) and
// the HBase connector are all Write=No. PXF reads Hive/HBase tables but does not
// create writable external tables over them, so a format-only writability check
// is not sufficient for these schemes — e.g. hive:text must be REJECTED for a
// writable table even though "text" is a writable format on hdfs/object stores.
var readOnlySchemes = map[string]struct{}{
	"hive":  {},
	"hbase": {},
}

// IsProfileWritable reports whether a WRITABLE external table may be created for
// the given PXF profile, per the spec Read/Write matrix documented on this
// package.
//
// Parsing mirrors the webhook's isValidPxfProfile: the profile is lowercased
// and split once on ":" via strings.Cut.
//
//   - An empty profile is not writable (returns false).
//   - A read-only SCHEME (hive, hbase) is never writable, regardless of any
//     format suffix: hive, hive:text, hive:orc, hive:rc and hbase all return
//     false (the Hive/HBase connectors are Read=Yes, Write=No in the spec).
//   - A bare profile (no ":") is otherwise writable only when its connector
//     supports writes (jdbc).
//   - A "<scheme>:<format>" profile (for a non-read-only scheme) is writable
//     when its format is in WritableFormats (e.g. s3:parquet → true,
//     gs:json → false, hdfs:sequencefile → true).
//
// Matching is case-insensitive (e.g. "S3:Parquet" → true, "HIVE:TEXT" → false).
func IsProfileWritable(profile string) bool {
	if profile == "" {
		return false
	}
	lower := strings.ToLower(profile)
	scheme, format, hasSep := strings.Cut(lower, ":")
	// Read-only connectors (hive, hbase) are never writable, with or without a
	// format suffix — this overrides the format check so e.g. hive:text (a
	// "writable" format on other schemes) is still rejected for the Hive scheme.
	if _, readOnly := readOnlySchemes[scheme]; readOnly {
		return false
	}
	if !hasSep {
		// Bare profile (no format suffix): writable only for connectors whose
		// row in the matrix supports writes (jdbc).
		_, ok := bareWritableProfiles[scheme]
		return ok
	}
	_, ok := WritableFormats[format]
	return ok
}
