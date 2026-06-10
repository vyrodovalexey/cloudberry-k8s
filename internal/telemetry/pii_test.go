package telemetry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

// recordingReporter captures AssertNoPII failures instead of failing the test.
type recordingReporter struct {
	failures []string
}

func (r *recordingReporter) Helper() {}
func (r *recordingReporter) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func TestAssertNoPII_DetectsCredentialKeysAndValues(t *testing.T) {
	sr, restore := InstallSpanRecorder()
	defer restore()

	_, span := StartSpan(context.Background(), "pii-test", "leaky")
	span.SetAttributes(
		attribute.String("db.password", "hunter2"),               // key match
		attribute.String("http.header", "Bearer abc.def.ghi"),    // value match
		attribute.String("conn", "host=db password=supersecret"), // value match
	)
	span.End()

	rep := &recordingReporter{}
	AssertNoPII(rep, sr.Ended())
	assert.GreaterOrEqual(t, len(rep.failures), 3,
		"credential-looking key, bearer value, and password= value must all be flagged")
}

func TestAssertNoPII_CleanSpansPass(t *testing.T) {
	sr, restore := InstallSpanRecorder()
	defer restore()

	_, span := StartSpan(context.Background(), "pii-test", "clean")
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", "PromoteStandby"),
		attribute.Int("attempt", 2),
	)
	span.End()

	rep := &recordingReporter{}
	AssertNoPII(rep, sr.Ended())
	assert.Empty(t, rep.failures, "benign attributes must not be flagged: %v", rep.failures)
}
