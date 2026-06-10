package telemetry

import (
	"fmt"
	"regexp"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// InstallSpanRecorder installs an in-memory span recorder as the global
// tracer provider and returns it together with a restore function that
// reinstates the previous provider. It is shared test support for every
// package that asserts span presence/naming/propagation (the production
// code path is identical: spans flow through the global provider).
//
// Usage (in tests):
//
//	sr, restore := telemetry.InstallSpanRecorder()
//	defer restore()
//	... exercise code ...
//	spans := sr.Ended()
func InstallSpanRecorder() (*tracetest.SpanRecorder, func()) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	return sr, func() {
		otel.SetTracerProvider(prev)
	}
}

// piiAttrKeyPattern matches attribute KEYS that suggest credential material.
var piiAttrKeyPattern = regexp.MustCompile(`(?i)(password|passwd|secret|token|credential|authorization|api[-_]?key)`)

// piiAttrValuePattern matches attribute VALUES that look like credential
// material leaking into telemetry (bearer/basic headers, password fragments,
// connection-string credentials).
var piiAttrValuePattern = regexp.MustCompile(
	`(?i)(bearer\s+[a-z0-9._-]+|basic\s+[a-z0-9+/=]+|password=|pwd=|secret=|token=)`)

// testReporter is the subset of *testing.T used by AssertNoPII; declared
// locally so this shared test-support file does not import the testing
// package into the production build of every consumer.
type testReporter interface {
	Helper()
	Errorf(format string, args ...any)
}

// AssertNoPII scans every attribute (and event attribute) of the recorded
// spans for credential-looking keys and values, failing the test on a match.
// It is the shared E-5 PII gate: every package that records spans applies it
// to its span fixtures so passwords/tokens/secrets can never silently land in
// exported traces.
func AssertNoPII(t testReporter, spans []sdktrace.ReadOnlySpan) {
	t.Helper()
	check := func(spanName, scope, key, value string) {
		if piiAttrKeyPattern.MatchString(key) {
			t.Errorf("span %q %s attribute key %q looks like credential material", spanName, scope, key)
		}
		if piiAttrValuePattern.MatchString(value) {
			t.Errorf("span %q %s attribute %q carries a credential-looking value %q",
				spanName, scope, key, value)
		}
	}
	for _, span := range spans {
		for _, attr := range span.Attributes() {
			check(span.Name(), "span", string(attr.Key), fmt.Sprint(attr.Value.AsInterface()))
		}
		for _, ev := range span.Events() {
			for _, attr := range ev.Attributes {
				check(span.Name(), "event "+ev.Name, string(attr.Key), fmt.Sprint(attr.Value.AsInterface()))
			}
		}
	}
}
