package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// gploadJobSpecJob wraps a GploadJobSpec into a DataLoadingJob named "gpload-csv".
func gploadJobSpecJob(name string, gp *cbv1alpha1.GploadJobSpec) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:      name,
		Type:      "gpload",
		Enabled:   true,
		GploadJob: gp,
	}
}

// specGploadCSVJob returns the spec's "gpload-csv" job body (spec §317-394 +
// §963-995): gpfdist source, /incoming/*.csv glob, csv/",", header true,
// UTF-8, error-limit 50 + log-errors, target public.raw_data, mode insert,
// preload truncate true, postAction ANALYZE — the GOLDEN fixture.
func specGploadCSVJob() cbv1alpha1.DataLoadingJob {
	return gploadJobSpecJob("gpload-csv", &cbv1alpha1.GploadJobSpec{
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
		FilePaths:   []string{"/incoming/*.csv"},
		Format:      "csv",
		Delimiter:   ",",
		Header:      util.Ptr(true),
		Encoding:    "UTF-8",
		ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit: 50,
			LogErrors:          util.Ptr(true),
		},
		TargetTable: "public.raw_data",
		Mode:        "insert",
		Preload:     &cbv1alpha1.GploadPreloadSpec{Truncate: util.Ptr(true)},
		PostActions: []string{"ANALYZE public.raw_data"},
	})
}

// --- GOLDEN control file (SC101-J-STABLE, GL.1-GL.7) -----------------------

// TestBuildGploadControlFile_Golden asserts the FULL byte-exact control file for
// the spec gpload-csv fixture matches the spec block (GL.1-GL.7). The cluster is
// the test cluster, so HOST is "test-cluster-coord-hl" and the gpfdist SOURCE
// block emits LOCAL_HOSTNAME "test-cluster-gpfdist-svc" + PORT 8080 plus the
// LOCAL FILE path "/incoming/*.csv" (NO gpfdist:// URL).
func TestBuildGploadControlFile_Golden(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := specGploadCSVJob()

	got, err := b.BuildGploadControlFile(cluster, job)
	require.NoError(t, err)

	want := "VERSION: 1.0.0.1\n" +
		"DATABASE: postgres\n" +
		"USER: gpadmin\n" +
		"HOST: test-cluster-coord-hl\n" +
		"PORT: 5432\n" +
		"GPLOAD:\n" +
		"  INPUT:\n" +
		"    - SOURCE:\n" +
		"        LOCAL_HOSTNAME:\n" +
		"          - test-cluster-gpfdist-svc\n" +
		"        PORT: 8080\n" +
		"        FILE:\n" +
		"          - /incoming/*.csv\n" +
		"    - FORMAT: csv\n" +
		"    - DELIMITER: ','\n" +
		"    - HEADER: true\n" +
		"    - ENCODING: UTF-8\n" +
		"    - ERROR_LIMIT: 50\n" +
		"    - LOG_ERRORS: true\n" +
		"  OUTPUT:\n" +
		"    - TABLE: public.raw_data\n" +
		"    - MODE: INSERT\n" +
		"  PRELOAD:\n" +
		"    - TRUNCATE: true\n" +
		"  SQL:\n" +
		"    - AFTER: \"ANALYZE public.raw_data\"\n"

	assert.Equal(t, want, got)
}

// TestBuildGploadControlFile_ByteStable asserts the control file is byte-stable:
// the same input rendered twice yields identical output (deterministic ordering).
func TestBuildGploadControlFile_ByteStable(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := specGploadCSVJob()

	first, err := b.BuildGploadControlFile(cluster, job)
	require.NoError(t, err)
	second, err := b.BuildGploadControlFile(cluster, job)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

// TestBuildGploadControlFile_Errors asserts the builder errors on a
// mis-configured job (nil gploadJob or empty targetTable) so callers never emit
// an invalid control file.
func TestBuildGploadControlFile_Errors(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	t.Run("nil gploadJob", func(t *testing.T) {
		job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload"}
		_, err := b.BuildGploadControlFile(cluster, job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "gploadJob is nil")
	})
	t.Run("empty targetTable", func(t *testing.T) {
		job := gploadJobSpecJob("bad", &cbv1alpha1.GploadJobSpec{FilePaths: []string{"/a.csv"}})
		_, err := b.BuildGploadControlFile(cluster, job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "targetTable is required")
	})
}

// --- Per-line cases (GL.1-GL.7 / J.27-J.40) -------------------------------

// build renders the control file for a job body against the test cluster.
func buildGploadControl(t *testing.T, gp *cbv1alpha1.GploadJobSpec) string {
	t.Helper()
	b := NewBuilder()
	cluster := newTestCluster()
	got, err := b.BuildGploadControlFile(cluster, gploadJobSpecJob("gpload-csv", gp))
	require.NoError(t, err)
	return got
}

// baseGpload returns a minimal valid gpload spec (only the required targetTable),
// used by per-line cases that toggle one field at a time.
func baseGpload() *cbv1alpha1.GploadJobSpec {
	return &cbv1alpha1.GploadJobSpec{
		TargetTable: "public.raw_data",
		FilePaths:   []string{"/incoming/*.csv"},
	}
}

// TestBuildGploadControlFile_Header (GL.1) asserts the fixed header block.
func TestBuildGploadControlFile_Header(t *testing.T) {
	got := buildGploadControl(t, baseGpload())
	assert.Contains(t, got, "VERSION: 1.0.0.1\n")            // GL.1
	assert.Contains(t, got, "DATABASE: postgres\n")          // GL.1
	assert.Contains(t, got, "USER: gpadmin\n")               // GL.1
	assert.Contains(t, got, "HOST: test-cluster-coord-hl\n") // GL.1
	assert.Contains(t, got, "PORT: 5432\n")                  // GL.1
}

// TestBuildGploadControlFile_FileEntries (GL.2 / J.27-J.29) asserts the SOURCE
// block composition for gpfdist (default svc/port -> LOCAL_HOSTNAME/PORT, custom
// host, custom port) and local (NO LOCAL_HOSTNAME/PORT, verbatim FILE path). For
// a gpfdist source the FILE list now carries the LOCAL path (NO gpfdist:// URL).
func TestBuildGploadControlFile_FileEntries(t *testing.T) {
	tests := []struct {
		name string
		gp   *cbv1alpha1.GploadJobSpec
		// wantContains: substrings the SOURCE block MUST contain.
		wantContains []string
		// wantAbsent: substrings the SOURCE block MUST NOT contain.
		wantAbsent []string
	}{
		{
			name: "GL.2 gpfdist default svc + port 8080",
			gp: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.raw_data",
				FilePaths:   []string{"/incoming/*.csv"},
			},
			wantContains: []string{
				"        LOCAL_HOSTNAME:\n          - test-cluster-gpfdist-svc\n",
				"        PORT: 8080\n",
				"        FILE:\n          - /incoming/*.csv\n",
			},
			wantAbsent: []string{"gpfdist://"},
		},
		{
			name: "J.28 custom host -> LOCAL_HOSTNAME override",
			gp: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.raw_data",
				InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist", Host: "files.internal"},
				FilePaths:   []string{"/incoming/*.csv"},
			},
			wantContains: []string{
				"        LOCAL_HOSTNAME:\n          - files.internal\n",
				"        PORT: 8080\n",
				"        FILE:\n          - /incoming/*.csv\n",
			},
			wantAbsent: []string{"gpfdist://"},
		},
		{
			name: "J.29 custom port -> PORT override",
			gp: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.raw_data",
				InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist", Port: 9999},
				FilePaths:   []string{"/incoming/*.csv"},
			},
			wantContains: []string{
				"        LOCAL_HOSTNAME:\n          - test-cluster-gpfdist-svc\n",
				"        PORT: 9999\n",
				"        FILE:\n          - /incoming/*.csv\n",
			},
			wantAbsent: []string{"gpfdist://"},
		},
		{
			name: "J.27 local verbatim path (no LOCAL_HOSTNAME/PORT, no gpfdist:// prefix)",
			gp: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.raw_data",
				InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
				FilePaths:   []string{"/data/incoming/*.csv"},
			},
			wantContains: []string{
				"        FILE:\n          - /data/incoming/*.csv\n",
			},
			wantAbsent: []string{"gpfdist://", "LOCAL_HOSTNAME", "        PORT:"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildGploadControl(t, tt.gp)
			for _, want := range tt.wantContains {
				assert.Contains(t, got, want)
			}
			for _, absent := range tt.wantAbsent {
				assert.NotContains(t, got, absent)
			}
		})
	}
}

// TestBuildGploadControlFile_LocalLeadingSlashPreserved confirms a local path
// without a leading slash is used verbatim (NOT normalized to gpfdist://).
func TestBuildGploadControlFile_LocalLeadingSlashPreserved(t *testing.T) {
	got := buildGploadControl(t, &cbv1alpha1.GploadJobSpec{
		TargetTable: "public.raw_data",
		InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
		FilePaths:   []string{"relative/path.csv"},
	})
	assert.Contains(t, got, "          - relative/path.csv\n")
	assert.NotContains(t, got, "gpfdist://")
}

// TestBuildGploadControlFile_SkipsEmptyPaths asserts empty/whitespace filePaths
// entries are skipped for BOTH gpfdist and local sources (no blank FILE entries).
func TestBuildGploadControlFile_SkipsEmptyPaths(t *testing.T) {
	t.Run("gpfdist skips empty/whitespace", func(t *testing.T) {
		got := buildGploadControl(t, &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.raw_data",
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
			FilePaths:   []string{"", "  ", "/incoming/a.csv"},
		})
		assert.Contains(t, got, "          - /incoming/a.csv\n")
		assert.NotContains(t, got, "gpfdist://")
		// Exactly one FILE entry rendered (the two blanks skipped). The
		// LOCAL_HOSTNAME value also uses the "          - " indent, so the
		// gpfdist source has TWO such lines (LOCAL_HOSTNAME value + the FILE).
		assert.Equal(t, 2, countSubstr(got, "          - "))
	})
	t.Run("local skips empty/whitespace", func(t *testing.T) {
		got := buildGploadControl(t, &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.raw_data",
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
			FilePaths:   []string{"", "  ", "/data/a.csv"},
		})
		assert.Contains(t, got, "          - /data/a.csv\n")
		assert.Equal(t, 1, countSubstr(got, "          - "))
	})
}

// TestBuildGploadControlFile_Format (GL.3 / J.30) asserts FORMAT csv/text +
// default csv.
func TestBuildGploadControlFile_Format(t *testing.T) {
	tests := []struct {
		name, format, want string
	}{
		{name: "csv", format: "csv", want: "    - FORMAT: csv\n"},
		{name: "text", format: "text", want: "    - FORMAT: text\n"},
		{name: "default csv when unset", format: "", want: "    - FORMAT: csv\n"},
		{name: "default csv for garbage", format: "weird", want: "    - FORMAT: csv\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gp := baseGpload()
			gp.Format = tt.format
			assert.Contains(t, buildGploadControl(t, gp), tt.want)
		})
	}
}

// TestBuildGploadControlFile_Delimiter (GL.3 / J.31) asserts DELIMITER + default.
func TestBuildGploadControlFile_Delimiter(t *testing.T) {
	tests := []struct {
		name, delim, want string
	}{
		{name: "comma", delim: ",", want: "    - DELIMITER: ','\n"},
		{name: "pipe", delim: "|", want: "    - DELIMITER: '|'\n"},
		{name: "tab", delim: "\t", want: "    - DELIMITER: '\t'\n"},
		{name: "default comma when unset", delim: "", want: "    - DELIMITER: ','\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gp := baseGpload()
			gp.Delimiter = tt.delim
			assert.Contains(t, buildGploadControl(t, gp), tt.want)
		})
	}
}

// TestBuildGploadControlFile_Header_Toggle (GL.3 / J.32) asserts HEADER true is
// emitted only when Header==true; absent when nil or false.
func TestBuildGploadControlFile_Header_Toggle(t *testing.T) {
	t.Run("header true emits HEADER", func(t *testing.T) {
		gp := baseGpload()
		gp.Header = util.Ptr(true)
		assert.Contains(t, buildGploadControl(t, gp), "    - HEADER: true\n")
	})
	t.Run("header nil omits HEADER", func(t *testing.T) {
		gp := baseGpload()
		gp.Header = nil
		assert.NotContains(t, buildGploadControl(t, gp), "HEADER")
	})
	t.Run("header false omits HEADER", func(t *testing.T) {
		gp := baseGpload()
		gp.Header = util.Ptr(false)
		assert.NotContains(t, buildGploadControl(t, gp), "HEADER")
	})
}

// TestBuildGploadControlFile_Encoding (GL.3 / J.33) asserts ENCODING + default.
func TestBuildGploadControlFile_Encoding(t *testing.T) {
	tests := []struct {
		name, enc, want string
	}{
		{name: "utf-8", enc: "UTF-8", want: "    - ENCODING: UTF-8\n"},
		{name: "latin1", enc: "LATIN1", want: "    - ENCODING: LATIN1\n"},
		{name: "default UTF-8 when unset", enc: "", want: "    - ENCODING: UTF-8\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gp := baseGpload()
			gp.Encoding = tt.enc
			assert.Contains(t, buildGploadControl(t, gp), tt.want)
		})
	}
}

// TestBuildGploadControlFile_ErrorHandling (GL.4 / J.38) asserts ERROR_LIMIT +
// LOG_ERRORS presence/absence.
func TestBuildGploadControlFile_ErrorHandling(t *testing.T) {
	t.Run("limit + log errors emitted", func(t *testing.T) {
		gp := baseGpload()
		gp.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit: 50, LogErrors: util.Ptr(true),
		}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - ERROR_LIMIT: 50\n")
		assert.Contains(t, got, "    - LOG_ERRORS: true\n")
	})
	t.Run("nil errorHandling omits both", func(t *testing.T) {
		gp := baseGpload()
		gp.ErrorHandling = nil
		got := buildGploadControl(t, gp)
		assert.NotContains(t, got, "ERROR_LIMIT")
		assert.NotContains(t, got, "LOG_ERRORS")
	})
	t.Run("zero limit omits ERROR_LIMIT", func(t *testing.T) {
		gp := baseGpload()
		gp.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{SegmentRejectLimit: 0}
		assert.NotContains(t, buildGploadControl(t, gp), "ERROR_LIMIT")
	})
	t.Run("logErrors false omits LOG_ERRORS", func(t *testing.T) {
		gp := baseGpload()
		gp.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit: 10, LogErrors: util.Ptr(false),
		}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - ERROR_LIMIT: 10\n")
		assert.NotContains(t, got, "LOG_ERRORS")
	})
}

// TestBuildGploadControlFile_Table (GL.5 / J.34) asserts the TABLE line.
func TestBuildGploadControlFile_Table(t *testing.T) {
	gp := baseGpload()
	gp.TargetTable = "schema.tbl"
	assert.Contains(t, buildGploadControl(t, gp), "    - TABLE: schema.tbl\n")
}

// TestBuildGploadControlFile_Mode (GL.5 / J.35-J.37) asserts MODE INSERT/UPDATE/
// MERGE (upper-cased) and that update/merge emit MATCH_COLUMNS (and optional
// UPDATE_COLUMNS) while insert emits neither.
func TestBuildGploadControlFile_Mode(t *testing.T) {
	t.Run("J.35 insert (default) -> MODE INSERT, no match cols", func(t *testing.T) {
		gp := baseGpload()
		gp.Mode = "insert"
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - MODE: INSERT\n")
		assert.NotContains(t, got, "MATCH_COLUMNS")
		assert.NotContains(t, got, "UPDATE_COLUMNS")
	})
	t.Run("mode unset defaults to INSERT", func(t *testing.T) {
		gp := baseGpload()
		gp.Mode = ""
		assert.Contains(t, buildGploadControl(t, gp), "    - MODE: INSERT\n")
	})
	t.Run("J.36 update -> MODE UPDATE + MATCH_COLUMNS", func(t *testing.T) {
		gp := baseGpload()
		gp.Mode = "update"
		gp.MatchColumns = []string{"id"}
		gp.UpdateColumns = []string{"payload"}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - MODE: UPDATE\n")
		assert.Contains(t, got, "    - MATCH_COLUMNS: [ id ]\n")
		assert.Contains(t, got, "    - UPDATE_COLUMNS: [ payload ]\n")
	})
	t.Run("J.37 merge -> MODE MERGE + MATCH_COLUMNS", func(t *testing.T) {
		gp := baseGpload()
		gp.Mode = "merge"
		gp.MatchColumns = []string{"id", "tenant"}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - MODE: MERGE\n")
		assert.Contains(t, got, "    - MATCH_COLUMNS: [ id, tenant ]\n")
	})
	t.Run("update without update_columns omits UPDATE_COLUMNS", func(t *testing.T) {
		gp := baseGpload()
		gp.Mode = "update"
		gp.MatchColumns = []string{"id"}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "    - MATCH_COLUMNS: [ id ]\n")
		assert.NotContains(t, got, "UPDATE_COLUMNS")
	})
}

// TestBuildGploadControlFile_Preload (GL.6 / J.39) asserts PRELOAD TRUNCATE is
// emitted only when truncate==true.
func TestBuildGploadControlFile_Preload(t *testing.T) {
	t.Run("truncate true emits PRELOAD TRUNCATE", func(t *testing.T) {
		gp := baseGpload()
		gp.Preload = &cbv1alpha1.GploadPreloadSpec{Truncate: util.Ptr(true)}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "  PRELOAD:\n")
		assert.Contains(t, got, "    - TRUNCATE: true\n")
	})
	t.Run("nil preload omits PRELOAD", func(t *testing.T) {
		gp := baseGpload()
		gp.Preload = nil
		assert.NotContains(t, buildGploadControl(t, gp), "PRELOAD")
	})
	t.Run("truncate false omits PRELOAD", func(t *testing.T) {
		gp := baseGpload()
		gp.Preload = &cbv1alpha1.GploadPreloadSpec{Truncate: util.Ptr(false)}
		assert.NotContains(t, buildGploadControl(t, gp), "PRELOAD")
	})
}

// TestBuildGploadControlFile_SQLAfter (GL.7 / J.40) asserts one SQL AFTER entry
// per postActions element; absent when empty.
func TestBuildGploadControlFile_SQLAfter(t *testing.T) {
	t.Run("one AFTER per action", func(t *testing.T) {
		gp := baseGpload()
		gp.PostActions = []string{"ANALYZE public.raw_data", "VACUUM public.raw_data"}
		got := buildGploadControl(t, gp)
		assert.Contains(t, got, "  SQL:\n")
		assert.Contains(t, got, "    - AFTER: \"ANALYZE public.raw_data\"\n")
		assert.Contains(t, got, "    - AFTER: \"VACUUM public.raw_data\"\n")
	})
	t.Run("empty postActions omits SQL block", func(t *testing.T) {
		gp := baseGpload()
		gp.PostActions = nil
		assert.NotContains(t, buildGploadControl(t, gp), "  SQL:")
	})
}

// --- ConfigMap (SC101-J-CM) ------------------------------------------------

// TestBuildGploadControlFileConfigMap asserts the per-job ConfigMap name
// "<cluster>-gpload-<job>", the "<job>.yml" data key carrying the EXACT control
// file, and a cluster ownerRef.
func TestBuildGploadControlFileConfigMap(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := specGploadCSVJob()

	cm := b.BuildGploadControlFileConfigMap(cluster, job)
	require.NotNil(t, cm)

	assert.Equal(t, "test-cluster-gpload-gpload-csv", cm.Name)
	assert.Equal(t, util.GploadControlFileConfigMapName(cluster.Name, job.Name), cm.Name)
	assert.Equal(t, cluster.Namespace, cm.Namespace)
	assertOwnedByCluster(t, cm.OwnerReferences, cluster)

	// Data key == "<job>.yml"; value == BuildGploadControlFile output.
	want, err := b.BuildGploadControlFile(cluster, job)
	require.NoError(t, err)
	require.Contains(t, cm.Data, "gpload-csv.yml")
	assert.Equal(t, want, cm.Data["gpload-csv.yml"])
}

// TestBuildGploadControlFileConfigMap_NilOnError asserts a mis-configured job
// (nil gploadJob) yields a nil ConfigMap (no usable control file).
func TestBuildGploadControlFileConfigMap_NilOnError(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload"}
	assert.Nil(t, b.BuildGploadControlFileConfigMap(cluster, job))
}

// --- Job / CronJob (SC101-J25-*, SC101-J-POD-ARGS) -------------------------

// TestBuildGploadJob asserts the one-off gpload Job: deterministic name, ownerRef,
// the `gpload -f /etc/gpload/<job>.yml` invocation in the container args, the CM
// volume mounted read-only at /etc/gpload, and the DATALOAD_ROWS marker.
func TestBuildGploadJob(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := gploadJobSpecJob("csv-bulk-load", &cbv1alpha1.GploadJobSpec{
		TargetTable: "public.raw_data",
		FilePaths:   []string{"/incoming/*.csv"},
	})

	out := b.BuildGploadJob(cluster, job)
	require.NotNil(t, out)

	assert.Equal(t, util.DataLoadJobName(cluster.Name, job.Name), out.Name)
	assert.Equal(t, cluster.Namespace, out.Namespace)
	assertOwnedByCluster(t, out.OwnerReferences, cluster)

	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	c := out.Spec.Template.Spec.Containers[0]
	assert.Equal(t, []string{shellCommand, shellFlag}, c.Command)
	require.Len(t, c.Args, 1)
	script := c.Args[0]
	assert.Contains(t, script, "gpload -f /etc/gpload/csv-bulk-load.yml")
	assert.Contains(t, script, "DATALOAD_ROWS=")

	// CM mounted read-only at /etc/gpload.
	found := false
	for _, m := range c.VolumeMounts {
		if m.MountPath == "/etc/gpload" {
			found = true
			assert.True(t, m.ReadOnly)
		}
	}
	assert.True(t, found, "gpload Job must mount the control-file ConfigMap at /etc/gpload")

	// The CM volume references the per-job control-file ConfigMap.
	foundVol := false
	for _, v := range out.Spec.Template.Spec.Volumes {
		if v.ConfigMap != nil &&
			v.ConfigMap.Name == util.GploadControlFileConfigMapName(cluster.Name, job.Name) {
			foundVol = true
		}
	}
	assert.True(t, foundVol)

	// RestartPolicy Never (one-off load).
	assert.Equal(t, "Never", string(out.Spec.Template.Spec.RestartPolicy))
}

// TestBuildGploadJob_NilOnError asserts a mis-configured job yields a nil Job.
func TestBuildGploadJob_NilOnError(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload"}
	assert.Nil(t, b.BuildGploadJob(cluster, job))
}

// TestBuildGploadCronJob (SC101-J25-CRONJOB) asserts a gpload job WITH a schedule
// yields a CronJob (schedule + ForbidConcurrent + history limits) and WITHOUT a
// schedule yields nil. The pod mounts the CM + runs `gpload -f`.
func TestBuildGploadCronJob(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	t.Run("J.25 schedule -> CronJob", func(t *testing.T) {
		job := gploadJobSpecJob("gpload-csv", &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.raw_data",
			FilePaths:   []string{"/incoming/*.csv"},
		})
		job.Schedule = "*/30 * * * *"

		cron := b.BuildGploadCronJob(cluster, job)
		require.NotNil(t, cron)
		assert.Equal(t, util.DataLoadJobName(cluster.Name, job.Name), cron.Name)
		assert.Equal(t, "*/30 * * * *", cron.Spec.Schedule)
		assert.Equal(t, "Forbid", string(cron.Spec.ConcurrencyPolicy))
		assertOwnedByCluster(t, cron.OwnerReferences, cluster)

		c := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
		require.Len(t, c.Args, 1)
		assert.Contains(t, c.Args[0], "gpload -f /etc/gpload/gpload-csv.yml")
		// CM mounted at /etc/gpload in the CronJob pod too.
		found := false
		for _, m := range c.VolumeMounts {
			if m.MountPath == "/etc/gpload" {
				found = true
			}
		}
		assert.True(t, found)
	})

	t.Run("no schedule -> nil CronJob", func(t *testing.T) {
		job := gploadJobSpecJob("gpload-csv", &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.raw_data",
		})
		assert.Nil(t, b.BuildGploadCronJob(cluster, job))
	})

	t.Run("scheduled but misconfigured -> nil CronJob", func(t *testing.T) {
		job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload", Schedule: "*/30 * * * *"}
		assert.Nil(t, b.BuildGploadCronJob(cluster, job))
	})
}
