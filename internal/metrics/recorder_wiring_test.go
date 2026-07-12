package metrics

// E-4: the Recorder wiring sweep. Every method of the Recorder interface must
// have at least one PRODUCTION call site (non-test Go file outside this
// package); otherwise a metric family is registered and documented but never
// emitted — exactly the class of dead wiring the metrics-honesty rule forbids.
// The single documented exception is allowlisted below.

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wiringAllowlist enumerates Recorder methods that intentionally have no
// production call site, each with the justification required by E-4.
//
// The data-loading metrics (RecordDataLoadingRows + the four new families) now
// have real production call sites in the controller's reconcileDataLoadingJobs
// terminal-state handling (the DATALOAD_ROWS marker harvest and the Job-status
// derived gauges), so RecordDataLoadingRows is no longer exempt. The allowlist
// is intentionally empty: every Recorder method must be wired.
var wiringAllowlist = map[string]string{}

// collectProductionSources returns the contents of every non-test .go file in
// the repository outside internal/metrics (the defining package, whose own
// references must not count as wiring).
func collectProductionSources(t *testing.T) []string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	var sources []string
	for _, dir := range []string{"api", "cmd", "internal"} {
		walkErr := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "vendor" || filepath.Base(filepath.Dir(path)) == "internal" && d.Name() == "metrics" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			sources = append(sources, string(data))
			return nil
		})
		require.NoError(t, walkErr)
	}
	require.NotEmpty(t, sources, "no production sources found — wrong working directory?")
	return sources
}

func TestRecorderWiring_EveryMethodHasProductionCallSite(t *testing.T) {
	recorderType := reflect.TypeOf((*Recorder)(nil)).Elem()
	require.Positive(t, recorderType.NumMethod())

	sources := collectProductionSources(t)

	var unwired []string
	for i := 0; i < recorderType.NumMethod(); i++ {
		method := recorderType.Method(i).Name
		if _, allowed := wiringAllowlist[method]; allowed {
			continue
		}

		needle := "." + method + "("
		found := false
		for _, src := range sources {
			if strings.Contains(src, needle) {
				found = true
				break
			}
		}
		if !found {
			unwired = append(unwired, method)
		}
	}

	assert.Empty(t, unwired,
		"Recorder methods without any production call site (add the wiring or "+
			"allowlist with a documented justification): %v", unwired)
}

// TestRecorderWiring_AllowlistEntriesAreReal guards the allowlist itself:
// every allowlisted name must still exist on the interface (so a removed
// method does not leave a stale exemption), and must NOT have gained a
// production call site (in which case the exemption must be deleted).
func TestRecorderWiring_AllowlistEntriesAreReal(t *testing.T) {
	recorderType := reflect.TypeOf((*Recorder)(nil)).Elem()
	sources := collectProductionSources(t)

	for name, justification := range wiringAllowlist {
		_, exists := recorderType.MethodByName(name)
		assert.True(t, exists, "allowlisted method %q no longer exists on Recorder", name)
		assert.NotEmpty(t, justification)

		needle := "." + name + "("
		for _, src := range sources {
			assert.NotContains(t, src, needle,
				"allowlisted method %q now HAS a production call site — remove the exemption", name)
		}
	}
}
