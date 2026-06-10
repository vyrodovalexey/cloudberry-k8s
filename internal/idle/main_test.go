package idle

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine-leak verification: the idle daemon runs a
// background scan loop per cluster and every test must stop it (E-3).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
