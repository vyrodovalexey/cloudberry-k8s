package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// mockPGServer creates a minimal PostgreSQL mock server that handles
// the startup handshake and responds to queries via the provided responder.
// It returns the listener address and a cleanup function.
func mockPGServer(t *testing.T, responder func(query string) []byte) (string, func()) {
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
			go handleConn(conn, responder)
		}
	}()

	cleanup := func() {
		_ = ln.Close()
		<-done
	}

	return ln.Addr().String(), cleanup
}

// handleConn performs the startup handshake and then services queries.
func handleConn(c net.Conn, responder func(query string) []byte) {
	defer func() { _ = c.Close() }()

	backend := pgproto3.NewBackend(c, c)

	startupMsg, startupErr := backend.ReceiveStartupMessage()
	if startupErr != nil {
		return
	}

	switch startupMsg.(type) {
	case *pgproto3.StartupMessage:
		buf := mustEncode(&pgproto3.AuthenticationOk{})
		buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})...)
		buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"})...)
		buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})...)
		buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})...)
		buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})...)
		buf = append(buf, mustEncode(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})...)
		buf = append(buf, mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
		if _, writeErr := c.Write(buf); writeErr != nil {
			return
		}
	case *pgproto3.SSLRequest:
		_, _ = c.Write([]byte("N"))
		return
	default:
		return
	}

	handleSimpleQueries(backend, c, responder)
}

func mustEncode(msg pgproto3.Message) []byte {
	buf, err := msg.Encode(nil)
	if err != nil {
		panic(err)
	}
	return buf
}

// handleSimpleQueries handles simple query protocol messages.
// We force pgx to use simple protocol via QueryExecModeSimpleProtocol.
func handleSimpleQueries(backend *pgproto3.Backend, conn net.Conn, responder func(query string) []byte) {
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			response := responder(m.String)
			response = append(response, mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
			if _, writeErr := conn.Write(response); writeErr != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		}
	}
}

// newMockConn creates a raw *pgx.Conn connected to a mock PostgreSQL server
// using simple protocol. It returns the connection and a cleanup function.
func newMockConn(t *testing.T, responder func(query string) []byte) (*pgx.Conn, func()) {
	t.Helper()

	addr, cleanup := mockPGServer(t, responder)

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	connStr := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable"
	cfg, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)

	return conn, func() {
		_ = conn.Close(context.Background())
		cleanup()
	}
}

// --- pgproto3 response builders ---

type fieldDesc struct {
	name string
	oid  uint32
}

func textField(name string) fieldDesc   { return fieldDesc{name: name, oid: 25} }
func int8Field(name string) fieldDesc   { return fieldDesc{name: name, oid: 20} }
func float8Field(name string) fieldDesc { return fieldDesc{name: name, oid: 701} }

func buildRowDesc(fields []fieldDesc) *pgproto3.RowDescription {
	rd := &pgproto3.RowDescription{}
	for _, f := range fields {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:         []byte(f.name),
			DataTypeOID:  f.oid,
			DataTypeSize: -1,
			Format:       0,
		})
	}
	return rd
}

// rowsResponseTyped builds a response for a result set with typed fields.
func rowsResponseTyped(fields []fieldDesc, rows [][]string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	for _, row := range rows {
		dr := &pgproto3.DataRow{}
		for _, v := range row {
			dr.Values = append(dr.Values, []byte(v))
		}
		buf = append(buf, mustEncode(dr)...)
	}
	buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})...)
	return buf
}

// singleRowTyped builds a single-row response with typed fields.
func singleRowTyped(fields []fieldDesc, values []string) []byte {
	return rowsResponseTyped(fields, [][]string{values})
}

// execResponse returns an exec-style command complete response.
func execResponse(tag string) []byte {
	return mustEncode(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// errorResponseMsg returns a PostgreSQL error response.
func errorResponseMsg(msg string) []byte {
	return mustEncode(&pgproto3.ErrorResponse{Severity: "ERROR", Message: msg})
}

// rowsThenError builds a response that starts a result set, delivers the given
// rows, and then fails with an error INSTEAD of CommandComplete — driving the
// rows.Err() branch in collectors that iterate result sets.
func rowsThenError(fields []fieldDesc, rows [][]string, errMsg string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	for _, row := range rows {
		dr := &pgproto3.DataRow{}
		for _, v := range row {
			dr.Values = append(dr.Values, []byte(v))
		}
		buf = append(buf, mustEncode(dr)...)
	}
	buf = append(buf, errorResponseMsg(errMsg)...)
	return buf
}
