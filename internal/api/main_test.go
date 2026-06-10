package api

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"

	"go.uber.org/goleak"
)

// trackedServers collects every Server constructed by tests so TestMain can
// release their rate-limiter cleanup goroutines before the leak check. Test
// fixtures are built in helpers without *testing.T, so per-test Cleanup is
// not available; tracking at construction keeps the goleak gate strict
// without ignoring our own production goroutines.
var (
	trackedServersMu sync.Mutex
	trackedServers   []*Server
)

// trackServer registers a test Server for end-of-run closure and returns it.
func trackServer(s *Server) *Server {
	trackedServersMu.Lock()
	defer trackedServersMu.Unlock()
	trackedServers = append(trackedServers, s)
	return s
}

func closeTrackedServers() {
	trackedServersMu.Lock()
	defer trackedServersMu.Unlock()
	for _, s := range trackedServers {
		s.Close()
	}
	trackedServers = nil
}

// TestMain enforces goroutine-leak verification for the API package: every
// Server owns a rate-limiter cleanup goroutine (Close must stop it), and the
// streaming/StartServer tests spawn real HTTP servers that must shut down
// cleanly (E-3).
func TestMain(m *testing.M) {
	code := m.Run()

	// Release fixture-owned goroutines that have no per-test owner, then
	// drop idle keep-alive connections of the shared default transport.
	closeTrackedServers()
	http.DefaultClient.CloseIdleConnections()

	if code == 0 {
		if err := goleak.Find(
			// klog's background flush daemon is process-wide and started
			// lazily by client-go; it is intentional third-party behavior.
			goleak.IgnoreTopFunction("k8s.io/klog/v2.(*flushDaemon).run"),
			// net/http keep-alive connections owned by shared transports are
			// closed lazily by the runtime, not by the code under test.
			goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
			goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}
