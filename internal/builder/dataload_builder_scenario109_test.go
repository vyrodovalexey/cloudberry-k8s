package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ============================================================================
// Scenario 109 — builder honesty for M.10 (DATALOAD_BYTES) and M.2/M.3
// (PXF actuator prometheus enablement).
//
// HONESTY: the gpload script emits a DATALOAD_BYTES marker (a real `wc -c` byte
// count) ONLY for a LOCAL source where the staged files are present in the pod;
// for a gpfdist/remote source no honest byte count exists so the marker is
// OMITTED and data_loading_bytes_total stays absent for that job.
// ============================================================================

// localGploadJob returns a gpload job with a LOCAL input source and concrete
// file paths (present in the pod), so the builder can measure real bytes.
func localGploadJob(name string) cbv1alpha1.DataLoadingJob {
	return gploadJobSpecJob(name, &cbv1alpha1.GploadJobSpec{
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
		FilePaths:   []string{"/data/incoming/a.csv", "/data/incoming/b.csv"},
		Format:      "csv",
		TargetTable: "public.raw_data",
	})
}

// remoteGploadJob returns a gpload job with a gpfdist (remote) input source: the
// files are served by an external gpfdist, NOT present in the pod, so no honest
// byte count is available.
func remoteGploadJob(name string) cbv1alpha1.DataLoadingJob {
	return gploadJobSpecJob(name, &cbv1alpha1.GploadJobSpec{
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
		FilePaths:   []string{"/incoming/*.csv"},
		Format:      "csv",
		TargetTable: "public.raw_data",
	})
}

// TestBuildGploadScript_LocalSourceEmitsBytesMarker covers 109-M10-BUILDER: for a
// LOCAL gpload source the rendered script measures real bytes via `wc -c` and
// emits the DATALOAD_BYTES marker over the staged input files.
func TestBuildGploadScript_LocalSourceEmitsBytesMarker(t *testing.T) {
	script := buildGploadScript(localGploadJob("gpload-local"))

	// The bytes measurement uses wc -c and the DATALOAD_BYTES marker is emitted.
	assert.Contains(t, script, "wc -c", "local source must measure real bytes via wc -c")
	assert.Contains(t, script, dataLoadBytesMarker, "local source must emit the DATALOAD_BYTES marker")
	// Both staged local files are part of the measured set.
	assert.Contains(t, script, "/data/incoming/a.csv")
	assert.Contains(t, script, "/data/incoming/b.csv")
	// The bytes marker is gated on a measured value (honest absence guard).
	assert.Contains(t, script, "if [ -n \"${bytes:-}\" ]; then",
		"the bytes marker must only be echoed when a real count was measured")
	// The rows marker remains present too (independent of bytes).
	assert.Contains(t, script, dataLoadRowsMarker)
}

// TestBuildGploadScript_RemoteSourceOmitsBytesMeasurement covers 109-M10-BUILDER
// honesty: for a gpfdist (remote) source the staged files are NOT in the pod, so
// the script does NOT MEASURE bytes (no `wc -c`) and the runtime `bytes` variable
// is never set. The marker-echo block is runtime-gated on a non-empty `${bytes}`,
// so with no measurement the DATALOAD_BYTES marker is never actually emitted —
// the bytes metric stays honestly absent (never synthesized from a guessed size).
//
// NOTE: the literal "DATALOAD_BYTES=" echo block is present in the script text
// for BOTH sources, but it only fires at runtime when `bytes` was measured (local
// only). The honest distinction is the PRESENCE of the `wc -c` measurement, which
// is what this test asserts is ABSENT for the remote source.
func TestBuildGploadScript_RemoteSourceOmitsBytesMeasurement(t *testing.T) {
	script := buildGploadScript(remoteGploadJob("gpload-remote"))

	assert.NotContains(t, script, "wc -c",
		"a gpfdist/remote source must NOT measure bytes (files not local)")
	assert.NotContains(t, script, "bytes=$(wc -c",
		"a gpfdist/remote source must NOT assign a real byte count to bytes")
	// The rows marker is still emitted (rowcount comes from gpload's summary).
	assert.Contains(t, script, dataLoadRowsMarker)
}

// TestBuildGploadScript_BytesMarkerRuntimeGated proves the marker-emission block
// is runtime-gated on a measured `${bytes}`: the echo line is guarded by
// `if [ -n "${bytes:-}" ]` so an unmeasured (remote) source never emits the
// marker even though the echo text exists in the script.
func TestBuildGploadScript_BytesMarkerRuntimeGated(t *testing.T) {
	remote := buildGploadScript(remoteGploadJob("gpload-remote"))
	local := buildGploadScript(localGploadJob("gpload-local"))

	// Both scripts carry the runtime gate (the echo only fires when measured).
	for _, s := range []string{remote, local} {
		assert.Contains(t, s, "if [ -n \"${bytes:-}\" ]; then")
	}
	// Only the LOCAL script actually measures bytes (sets the variable).
	assert.Contains(t, local, "bytes=$(wc -c")
	assert.NotContains(t, remote, "bytes=$(wc -c")
}

// TestWriteGploadBytesMeasurement_OmittedCases covers the writeGploadBytesMeasurement
// honest-omission branches: a nil GploadJob, a non-local source, and an empty
// file list all emit NOTHING (no bytes measurement).
func TestWriteGploadBytesMeasurement_OmittedCases(t *testing.T) {
	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
	}{
		{
			name: "nil gpload job → no measurement",
			job:  cbv1alpha1.DataLoadingJob{Name: "x", Type: "gpload"},
		},
		{
			name: "gpfdist source → no measurement",
			job:  remoteGploadJob("remote"),
		},
		{
			name: "local source with no files → no measurement",
			job: gploadJobSpecJob("empty", &cbv1alpha1.GploadJobSpec{
				InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
				FilePaths:   []string{"   "}, // whitespace-only → skipped
				TargetTable: "public.t",
			}),
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var s strings.Builder
			writeGploadBytesMeasurement(&s, tc.job)
			assert.Empty(t, s.String(), "no honest byte source → no measurement emitted")
		})
	}
}

// TestWriteGploadBytesMeasurement_LocalEmitsWcAndGuard covers the local-source
// emission path: it writes the `wc -c` measurement plus the numeric guard that
// drops a non-integer (so a partial read never produces a fabricated marker).
func TestWriteGploadBytesMeasurement_LocalEmitsWcAndGuard(t *testing.T) {
	var s strings.Builder
	writeGploadBytesMeasurement(&s, localGploadJob("local"))
	out := s.String()

	assert.Contains(t, out, "bytes=$(wc -c")
	assert.Contains(t, out, "/data/incoming/a.csv")
	assert.Contains(t, out, "/data/incoming/b.csv")
	// The integer guard blanks a non-numeric measurement (honest absence).
	assert.Contains(t, out, "case \"${bytes:-}\" in ''|*[!0-9]*) bytes=\"\" ;; esac")
}

// TestWriteGploadBytesMeasurement_ShellQuotesSingleQuotePath locks the A-1
// shell-safe quoting fix: a local file path containing a single quote (e.g.
// "/data/in/o'brien.csv") must be rendered with the POSIX-shell '\” escape
// idiom (shellQuote), NOT the SQL ” single-quote-doubling form. Without this
// the `wc -c` argument would be unsafely terminated and the command would break
// (or, worse, allow injection). The function is at 100% STATEMENT coverage, so
// this is a pure BEHAVIOR-verification guard: a silent revert to quoteSQLLiteral
// would keep statement coverage at 100% and pass every quote-free test while
// producing a broken/unsafe shell command — this test catches that regression.
func TestWriteGploadBytesMeasurement_ShellQuotesSingleQuotePath(t *testing.T) {
	job := gploadJobSpecJob("quoted", &cbv1alpha1.GploadJobSpec{
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
		FilePaths:   []string{"/data/in/o'brien.csv"},
		Format:      "csv",
		TargetTable: "public.raw_data",
	})

	var s strings.Builder
	writeGploadBytesMeasurement(&s, job)
	out := s.String()

	// The single quote must be rendered using the POSIX shell '\'' idiom: the
	// path is wrapped in single quotes, the embedded quote is closed-escaped-
	// reopened. shellQuote("/data/in/o'brien.csv") => '/data/in/o'\''brien.csv'.
	assert.Contains(t, out, `'/data/in/o'\''brien.csv'`,
		"single-quote path must use the shell '\\'' escape idiom (shellQuote)")

	// It must NOT use the SQL single-quote-doubling form (quoteSQLLiteral): that
	// would be an unsafe/broken shell argument. This is the regression guard.
	assert.NotContains(t, out, `'/data/in/o''brien.csv'`,
		"single-quote path must NOT use the SQL '' doubling form (quoteSQLLiteral)")
}

// TestWriteGploadBytesMeasurement_ShellQuotesMetacharPaths proves that paths
// containing shell metacharacters (space, $, ;) are wrapped in single quotes so
// the shell treats them as a single literal argument and never expands or
// splits them — the broader shellQuote contract behind the A-1 fix.
func TestWriteGploadBytesMeasurement_ShellQuotesMetacharPaths(t *testing.T) {
	job := gploadJobSpecJob("meta", &cbv1alpha1.GploadJobSpec{
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
		FilePaths: []string{
			"/data/in/my file.csv",
			"/data/in/$HOME.csv",
			"/data/in/a;rm.csv",
		},
		Format:      "csv",
		TargetTable: "public.raw_data",
	})

	var s strings.Builder
	writeGploadBytesMeasurement(&s, job)
	out := s.String()

	// Each metachar path is single-quoted as one literal argument (no expansion
	// of $HOME, no word-splitting on the space, no command separation on ';').
	assert.Contains(t, out, `'/data/in/my file.csv'`)
	assert.Contains(t, out, `'/data/in/$HOME.csv'`)
	assert.Contains(t, out, `'/data/in/a;rm.csv'`)
}
