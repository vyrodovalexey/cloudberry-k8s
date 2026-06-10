package vault

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine-leak verification for the whole package: the
// Vault client spawns a background token lifetime watcher per client, and
// every test must join it (Close / context cancel) before exiting (E-3).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// net/http keep-alive connections owned by shared transports are
		// closed lazily by the runtime, not by the code under test.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}
