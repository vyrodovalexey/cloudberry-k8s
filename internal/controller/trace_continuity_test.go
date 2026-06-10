package controller

// E-5: the literal cross-package trace-continuity scenario — a controller
// Reconcile (public entrypoint) that performs a REAL db.Client operation must
// produce ONE trace: HA "Reconcile" root → db.PromoteStandby child → db.query
// grandchild, sharing a single trace ID. The db client is the production
// pgx-backed implementation talking to a minimal wire-protocol mock server
// (no SQL is faked at the Go layer). The same span set is run through the
// shared telemetry.AssertNoPII gate.

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// startWirePG starts a minimal PostgreSQL wire-protocol server that answers
// the startup handshake plus both simple and extended protocol queries with a
// CommandComplete. It exists so the production db.NewClient (extended
// protocol, pool ping) can connect for the cross-package trace test.
func startWirePG(t *testing.T) (host string, port int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go serveWirePGConn(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	hostStr, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	portInt, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return hostStr, int32(portInt)
}

func wireEncode(msg pgproto3.Message) []byte {
	buf, err := msg.Encode(nil)
	if err != nil {
		panic(err)
	}
	return buf
}

func serveWirePGConn(c net.Conn) {
	defer func() { _ = c.Close() }()
	backend := pgproto3.NewBackend(c, c)

	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}
	switch startupMsg.(type) {
	case *pgproto3.StartupMessage:
		buf := wireEncode(&pgproto3.AuthenticationOk{})
		buf = append(buf, wireEncode(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})...)
		buf = append(buf, wireEncode(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})...)
		buf = append(buf, wireEncode(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})...)
		buf = append(buf, wireEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
		if _, writeErr := c.Write(buf); writeErr != nil {
			return
		}
	case *pgproto3.SSLRequest:
		_, _ = c.Write([]byte("N"))
		return
	default:
		return
	}

	for {
		msg, recvErr := backend.Receive()
		if recvErr != nil {
			return
		}
		var buf []byte
		switch msg.(type) {
		case *pgproto3.Query:
			buf = wireEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			buf = append(buf, wireEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
		case *pgproto3.Parse:
			buf = wireEncode(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			buf = wireEncode(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			buf = wireEncode(&pgproto3.NoData{})
		case *pgproto3.Execute:
			buf = wireEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case *pgproto3.Sync:
			buf = wireEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Close:
			buf = wireEncode(&pgproto3.CloseComplete{})
		case *pgproto3.Terminate:
			return
		default:
			continue
		}
		if _, writeErr := c.Write(buf); writeErr != nil {
			return
		}
	}
}

func TestTraceContinuity_ControllerReconcileToDBSpan(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	host, port := startWirePG(t)
	dbClient, err := db.NewClient(context.Background(), db.Config{
		Host:     host,
		Port:     port,
		Database: "postgres",
		Username: "gpadmin",
		Password: "not-logged",
		SSLMode:  "disable",
		MaxConns: 2,
		RetryOpts: util.RetryOptions{
			MaxRetries: 1, InitialBackoff: time.Millisecond,
			MaxBackoff: time.Millisecond, Multiplier: 1,
		},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err, "the production db client must connect to the wire mock")
	defer dbClient.Close()

	// Cluster with a pending activate-standby action drives Reconcile through
	// handleStandbyActivation → dbClient.PromoteStandby.
	cluster := newStandbyCluster()

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewHAReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		&mockDBClientFactory{client: dbClient}, nil, &metrics.NoopRecorder{}, nil)

	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err)

	spans := sr.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}

	root, ok := byName["Reconcile"]
	require.True(t, ok, "HA Reconcile root span missing")
	dbSpan, ok := byName["db.PromoteStandby"]
	require.True(t, ok, "db.PromoteStandby span missing — db client not traced through the reconcile ctx")
	querySpan, ok := byName["db.query"]
	require.True(t, ok, "per-statement db.query span missing")

	// SINGLE trace: every span shares the Reconcile root's trace ID.
	assert.Equal(t, root.SpanContext().TraceID(), dbSpan.SpanContext().TraceID(),
		"db operation span must be in the SAME trace as the controller Reconcile")
	assert.Equal(t, root.SpanContext().TraceID(), querySpan.SpanContext().TraceID(),
		"per-statement span must be in the SAME trace as the controller Reconcile")
	// Parentage: db.query → db.PromoteStandby; db.PromoteStandby descends
	// from the Reconcile root (possibly through intermediate phase spans).
	assert.Equal(t, dbSpan.SpanContext().SpanID(), querySpan.Parent().SpanID())
	assert.True(t, dbSpan.Parent().IsValid(),
		"db.PromoteStandby must have a parent inside the reconcile trace")

	// E-5 PII gate over EVERY span of the cross-package trace: the database
	// password ("not-logged") and connection credentials must not appear.
	telemetry.AssertNoPII(t, spans)
	for _, s := range spans {
		for _, attr := range s.Attributes() {
			assert.NotContains(t, attr.Value.Emit(), "not-logged",
				"the db password must never appear in span attributes")
		}
	}
}
