package db

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// opDurationRecorder captures ObserveDBQueryDuration calls (C-2 hook test).
type opDurationRecorder struct {
	metrics.NoopRecorder
	mu  sync.Mutex
	ops []string
}

func (r *opDurationRecorder) ObserveDBQueryDuration(operation string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, operation)
}

func (r *opDurationRecorder) operations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

// spanNames extracts the span names from ended spans.
func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
	}
	return names
}

// TestPromoteStandbySpanAndDuration verifies that a composite method produces
// a named child span parented on the caller's span (trace continuity across
// the package boundary), per-query child spans from the pgx tracer, and a
// query-duration histogram observation via the shared hook.
func TestPromoteStandbySpanAndDuration(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	rec := &opDurationRecorder{}
	client.SetRecorder(rec, "c1", "ns1")

	// Install the pgx tracer manually: the mock client builds its pool
	// directly instead of going through NewClient.
	parentCtx, parentSpan := telemetry.StartSpan(context.Background(), "test", "controller.parent")
	err := client.PromoteStandby(parentCtx)
	parentSpan.End()
	require.NoError(t, err)

	spans := sr.Ended()
	names := spanNames(spans)
	assert.Contains(t, names, "db.PromoteStandby")
	assert.Contains(t, names, "controller.parent")

	// Parent linkage: db.PromoteStandby's parent is controller.parent.
	var opSpan, parent sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "db.PromoteStandby":
			opSpan = s
		case "controller.parent":
			parent = s
		}
	}
	require.NotNil(t, opSpan)
	require.NotNil(t, parent)
	assert.Equal(t, parent.SpanContext().SpanID(), opSpan.Parent().SpanID())
	assert.Equal(t, parent.SpanContext().TraceID(), opSpan.SpanContext().TraceID())

	// The duration histogram hook observed the operation exactly once.
	assert.Equal(t, []string{"PromoteStandby"}, rec.operations())

	// E-5 PII gate: db spans must not carry statement text or credentials.
	telemetry.AssertNoPII(t, spans)
}

// TestCompositeMethodErrorSetsSpanStatus verifies the error path marks the
// operation span with codes.Error.
func TestCompositeMethodErrorSetsSpanStatus(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	err := client.PromoteStandby(context.Background())
	require.Error(t, err)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "db.PromoteStandby" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code)
		}
	}
	assert.True(t, found, "db.PromoteStandby span not exported")
}

// TestPgxQueryTracerSpans verifies that the pgx tracer creates a per-query
// child span (no SQL text in attributes) when installed on the pool config.
func TestPgxQueryTracerSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	tracer := &pgxQueryTracer{database: "testdb"}
	ctx := tracer.TraceQueryStart(context.Background(), nil,
		pgx.TraceQueryStartData{SQL: "SELECT secret FROM credentials"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "db.query", span.Name())
	for _, attr := range span.Attributes() {
		assert.NotContains(t, attr.Value.AsString(), "SELECT secret",
			"SQL text must not be recorded in span attributes")
	}
}

// TestRepresentativeOperationsObserveDuration exercises several composite
// methods against the mock server and asserts each observes its duration
// histogram with the bounded operation label.
func TestRepresentativeOperationsObserveDuration(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("OK")
	})
	defer cleanup()

	rec := &opDurationRecorder{}
	client.SetRecorder(rec, "c1", "ns1")
	ctx := context.Background()

	_ = client.PromoteStandby(ctx)
	_ = client.SetupExporterRole(ctx, "pw")
	_, _ = client.SetupPXFExtensions(ctx)
	_, _, _ = client.GetQueryHistory(ctx, QueryHistoryFilter{})

	ops := rec.operations()
	assert.Contains(t, ops, "PromoteStandby")
	assert.Contains(t, ops, "SetupExporterRole")
	assert.Contains(t, ops, "SetupPXFExtensions")
	assert.Contains(t, ops, "GetQueryHistory")
}

// ddlInvoke names a W3-C2 DDL method and a closure that invokes it on the
// client. The closure must succeed against an "OK"-returning mock pool.
type ddlInvoke struct {
	method string
	call   func(ctx context.Context, c *pgxClient) error
}

// ddlMethods is the canonical list of the 15 W3-C2 DDL methods, each paired
// with a representative successful invocation.
func ddlMethods() []ddlInvoke {
	return []ddlInvoke{
		{"SetParameter", func(ctx context.Context, c *pgxClient) error {
			return c.SetParameter(ctx, "work_mem", "64MB", ParameterScope{Level: "cluster"})
		}},
		{"ReloadConfig", func(ctx context.Context, c *pgxClient) error {
			return c.ReloadConfig(ctx)
		}},
		{"CreateRole", func(ctx context.Context, c *pgxClient) error {
			return c.CreateRole(ctx, RoleOptions{Name: "r1"})
		}},
		{"AlterRole", func(ctx context.Context, c *pgxClient) error {
			return c.AlterRole(ctx, RoleOptions{Name: "r1", Login: true})
		}},
		{"DropRole", func(ctx context.Context, c *pgxClient) error {
			return c.DropRole(ctx, "r1")
		}},
		{"Vacuum", func(ctx context.Context, c *pgxClient) error {
			return c.Vacuum(ctx, VacuumOptions{Table: "t1"})
		}},
		{"Analyze", func(ctx context.Context, c *pgxClient) error {
			return c.Analyze(ctx, "t1")
		}},
		{"Reindex", func(ctx context.Context, c *pgxClient) error {
			return c.Reindex(ctx, ReindexOptions{Table: "t1"})
		}},
		{"CreateResourceGroup", func(ctx context.Context, c *pgxClient) error {
			return c.CreateResourceGroup(ctx, ResourceGroupOptions{
				Name: "rg1", Concurrency: 10, CPUMaxPercent: 50,
			})
		}},
		{"AlterResourceGroup", func(ctx context.Context, c *pgxClient) error {
			return c.AlterResourceGroup(ctx, ResourceGroupOptions{
				Name: "rg1", Concurrency: 20, CPUMaxPercent: 60,
			})
		}},
		{"DropResourceGroup", func(ctx context.Context, c *pgxClient) error {
			return c.DropResourceGroup(ctx, "rg1")
		}},
		{"AssignRoleResourceGroup", func(ctx context.Context, c *pgxClient) error {
			return c.AssignRoleResourceGroup(ctx, "r1", "rg1")
		}},
		{"CreateResourceQueue", func(ctx context.Context, c *pgxClient) error {
			return c.CreateResourceQueue(ctx, ResourceQueueOptions{Name: "q1", ActiveStatements: 5})
		}},
		{"DropResourceQueue", func(ctx context.Context, c *pgxClient) error {
			return c.DropResourceQueue(ctx, "q1")
		}},
		{"MoveQueryToResourceGroup", func(ctx context.Context, c *pgxClient) error {
			return c.MoveQueryToResourceGroup(ctx, 42, "rg1")
		}},
	}
}

// ddlOKResponder returns "OK" for exec-style DDL and, for the
// MoveQueryToResourceGroup PID lookup, a single-row username so the subsequent
// ALTER ROLE proceeds.
func ddlOKResponder(query string) []byte {
	if strings.Contains(query, "pg_stat_activity") {
		return singleRowResponse([]string{"usename"}, []string{"analyst"})
	}
	return execResponse("OK")
}

// TestDDLMethodsObserveBoundedDurationLabel verifies the W3-C2 contract for all
// 15 DDL methods: each observes ObserveDBQueryDuration with the bounded Go
// method name as the operation label AND exports a "db.<Method>" named span
// (TASK 10). For one method it also asserts the per-statement "db.query" child
// span still nests (the QueryTracer was not removed), and the error path marks
// the span codes.Error with exactly one duration observation (no double-count).
func TestDDLMethodsObserveBoundedDurationLabel(t *testing.T) {
	for _, m := range ddlMethods() {
		m := m
		t.Run(m.method, func(t *testing.T) {
			sr, restore := telemetry.InstallSpanRecorder()
			defer restore()

			client, cleanup := newMockPgxClient(t, ddlOKResponder)
			defer cleanup()
			rec := &opDurationRecorder{}
			client.SetRecorder(rec, "c1", "ns1")

			require.NoError(t, m.call(context.Background(), client))

			// Bounded duration label == the Go method name, observed once.
			ops := rec.operations()
			count := 0
			for _, op := range ops {
				if op == m.method {
					count++
				}
			}
			assert.Equal(t, 1, count,
				"duration must be observed exactly once with the bounded method label, got %v", ops)

			// A "db.<Method>" named span was exported.
			assert.Contains(t, spanNames(sr.Ended()), "db."+m.method,
				"missing named operation span db.%s", m.method)

			// db spans must never carry SQL text or credentials.
			telemetry.AssertNoPII(t, sr.Ended())
		})
	}
}

// TestDDLMethodNestsQueryChildSpan verifies the per-statement "db.query" child
// span still nests inside a DDL operation span (the pgx QueryTracer was not
// removed by the W3-C2 wrap).
func TestDDLMethodNestsQueryChildSpan(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClientWithTracer(t, ddlOKResponder)
	defer cleanup()

	require.NoError(t, client.DropResourceGroup(context.Background(), "rg1"))

	names := spanNames(sr.Ended())
	assert.Contains(t, names, "db.DropResourceGroup")
	assert.Contains(t, names, "db.query", "per-statement db.query child span must still nest")
}

// TestDDLMethodErrorPathObservesOnce verifies the error path of a W3-C2 DDL
// method marks the span codes.Error and still observes the duration exactly
// once (no double observation).
func TestDDLMethodErrorPathObservesOnce(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("resource group exists")
	})
	defer cleanup()
	rec := &opDurationRecorder{}
	client.SetRecorder(rec, "c1", "ns1")

	err := client.CreateResourceGroup(context.Background(), ResourceGroupOptions{Name: "rg1"})
	require.Error(t, err)

	// Span marked errored.
	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "db.CreateResourceGroup" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code)
		}
	}
	assert.True(t, found, "db.CreateResourceGroup span not exported")

	// Duration observed exactly once even on the error path.
	ops := rec.operations()
	count := 0
	for _, op := range ops {
		if op == "CreateResourceGroup" {
			count++
		}
	}
	assert.Equal(t, 1, count, "duration must be observed exactly once on error, got %v", ops)
}

// TestGetMaxConnections verifies the new max_connections query (C-5).
func TestGetMaxConnections(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{{name: "setting", oid: 23}}, []string{"250"})
	})
	defer cleanup()

	maxConns, err := client.GetMaxConnections(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(250), maxConns)
}

func TestGetMaxConnectionsError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("no such setting")
	})
	defer cleanup()

	_, err := client.GetMaxConnections(context.Background())
	require.Error(t, err)
}

// readInvoke names a Group-A read method, a closure that invokes it on the
// client, and a responder that returns a row-shaped success payload for it.
// Unlike the DDL methods (which exec and return "OK"), read methods scan
// result rows, so each needs its own row-description-shaped success response.
type readInvoke struct {
	method    string
	responder func(query string) []byte
	call      func(ctx context.Context, c *pgxClient) error
}

// readMethods is a representative subset of the 22 Group-A read methods, each
// paired with a row-shaped successful responder and an invocation. It mirrors
// ddlMethods() so the read wraps inherit the same observability contract.
func readMethods() []readInvoke {
	segFields := []fieldDesc{
		int4Field("content"), int4Field("dbid"), textField("role"), textField("preferred_role"),
		textField("mode"), textField("status"), textField("hostname"), textField("address"),
		int4Field("port"), textField("datadir"),
	}
	mirrorFields := []fieldDesc{
		int4Field("content_id"), boolField("is_synced"),
		int8Field("replication_lag"), textField("state"),
	}
	sessionFields := []fieldDesc{
		int4Field("pid"), textField("usename"), textField("datname"),
		textField("application_name"),
		textField("client_addr"), textField("state"),
		textField("wait_event_type"),
		textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"),
		textField("rsgname"),
	}
	bloatFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("n_dead_tup"), int8Field("dead_pct"),
	}

	return []readInvoke{
		{"GetSegmentConfiguration", func(string) []byte {
			return multiRowResponseTyped(segFields, [][]string{
				{"0", "1", "p", "p", "s", "u", "host1", "10.0.0.1", "6000", "/data/primary/gpseg0"},
			})
		}, func(ctx context.Context, c *pgxClient) error {
			_, err := c.GetSegmentConfiguration(ctx)
			return err
		}},
		{"GetMirrorSyncStatus", func(string) []byte {
			return multiRowResponseTyped(mirrorFields, [][]string{
				{"0", "t", "0", "streaming"},
			})
		}, func(ctx context.Context, c *pgxClient) error {
			_, err := c.GetMirrorSyncStatus(ctx)
			return err
		}},
		{"GetReplicationLag", func(string) []byte {
			return singleRowResponseTyped([]fieldDesc{int8Field("lag")}, []string{"1024"})
		}, func(ctx context.Context, c *pgxClient) error {
			_, err := c.GetReplicationLag(ctx)
			return err
		}},
		{"ListSessionsWithResourceGroup", func(string) []byte {
			return multiRowResponseTyped(sessionFields, [][]string{
				{"123", "admin", "postgres", "psql", "10.0.0.1", "active", "", "SELECT 1", "2025-01-01 00:00:00+00", "00:01:30", "analytics"},
			})
		}, func(ctx context.Context, c *pgxClient) error {
			_, err := c.ListSessionsWithResourceGroup(ctx)
			return err
		}},
		{"TriggerFTSProbe", func(string) []byte {
			return execResponse("SELECT 1")
		}, func(ctx context.Context, c *pgxClient) error {
			return c.TriggerFTSProbe(ctx)
		}},
		{"GetBloatRecommendations", func(string) []byte {
			return multiRowResponseTyped(bloatFields, [][]string{
				{"public", "users", "50000", "25"},
			})
		}, func(ctx context.Context, c *pgxClient) error {
			_, err := c.GetBloatRecommendations(ctx, RecommendationThresholds{})
			return err
		}},
	}
}

// TestReadMethodsObserveBoundedDurationLabel verifies the Group-A observability
// contract for the representative read methods: each observes
// ObserveDBQueryDuration exactly once with the bounded Go method name as the
// operation label AND exports a "db.<Method>" named span. This guards against a
// dropped startOperation wrap or defer end(err) on any read method.
func TestReadMethodsObserveBoundedDurationLabel(t *testing.T) {
	for _, m := range readMethods() {
		m := m
		t.Run(m.method, func(t *testing.T) {
			sr, restore := telemetry.InstallSpanRecorder()
			defer restore()

			client, cleanup := newMockPgxClient(t, m.responder)
			defer cleanup()
			rec := &opDurationRecorder{}
			client.SetRecorder(rec, "c1", "ns1")

			require.NoError(t, m.call(context.Background(), client))

			// Bounded duration label == the Go method name, observed once.
			ops := rec.operations()
			count := 0
			for _, op := range ops {
				if op == m.method {
					count++
				}
			}
			assert.Equal(t, 1, count,
				"duration must be observed exactly once with the bounded method label, got %v", ops)

			// A "db.<Method>" named span was exported.
			assert.Contains(t, spanNames(sr.Ended()), "db."+m.method,
				"missing named operation span db.%s", m.method)

			// db spans must never carry SQL text or credentials.
			telemetry.AssertNoPII(t, sr.Ended())
		})
	}
}

// TestReadMethodErrorPathObservesOnce verifies the error path of each Group-A
// read method marks the span codes.Error and still observes the duration
// exactly once (no double observation).
func TestReadMethodErrorPathObservesOnce(t *testing.T) {
	for _, m := range readMethods() {
		m := m
		t.Run(m.method, func(t *testing.T) {
			sr, restore := telemetry.InstallSpanRecorder()
			defer restore()

			client, cleanup := newMockPgxClient(t, func(_ string) []byte {
				return errorResponseMsg("boom")
			})
			defer cleanup()
			rec := &opDurationRecorder{}
			client.SetRecorder(rec, "c1", "ns1")

			err := m.call(context.Background(), client)
			require.Error(t, err)

			// Span marked errored.
			var found bool
			for _, s := range sr.Ended() {
				if s.Name() == "db."+m.method {
					found = true
					assert.Equal(t, codes.Error, s.Status().Code)
				}
			}
			assert.True(t, found, "db.%s span not exported", m.method)

			// Duration observed exactly once even on the error path.
			ops := rec.operations()
			count := 0
			for _, op := range ops {
				if op == m.method {
					count++
				}
			}
			assert.Equal(t, 1, count, "duration must be observed exactly once on error, got %v", ops)
		})
	}
}
