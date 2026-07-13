package cron_test

// Tests pinning the L-6 Vixie star-rule behavior change (handoff gap UT-29):
// day-field restriction is a property of the RAW field text ("*"/"*/n" =
// unrestricted), so an explicit full range like "1-31" or "0-6" is RESTRICTED
// and flips dayMatches to OR semantics. Under the previous len()-based
// heuristic these rows would produce different next-run times — a revert
// fails this table.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/cron"
)

func TestNextAfter_VixieStarRule(t *testing.T) {
	// 2026-06-10 is a Wednesday; June 2026 Fridays: 12, 19, 26.
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	require.Equal(t, time.Wednesday, from.Weekday())

	tests := []struct {
		name string
		expr string
		want time.Time
	}{
		{
			// Explicit full DOM range is RESTRICTED (unlike "*"): with Friday
			// also restricted the semantics are OR, so the very next day
			// (Thu 06-11, dom=11 in 1-31) matches. The old len() heuristic
			// treated "1-31" as unrestricted (AND) and yielded Fri 06-12.
			name: "explicit 1-31 DOM range restricts: OR with Friday -> next day",
			expr: "0 0 1-31 * 5",
			want: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC),
		},
		{
			// "*/2" starts with '*': the DOM stays UNRESTRICTED, so AND
			// semantics apply — the run must be a Friday AND an odd day.
			// Fridays 06-12 (even) and 06-26 fail the odd-day set; 06-19 is
			// the first odd Friday.
			name: "star-step */2 DOM stays unrestricted: AND with Friday -> first odd Friday",
			expr: "0 0 */2 * 5",
			want: time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		},
		{
			// Explicit full DOW range "0-6" is RESTRICTED: OR semantics with
			// the 15th, and since every weekday matches, EVERY day matches —
			// the next midnight (06-11), not the 15th.
			name: "explicit 0-6 DOW range restricts: OR with the 15th -> every day",
			expr: "0 0 15 * 0-6",
			want: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC),
		},
		{
			// Baseline: DOW "*" is unrestricted -> AND path -> DOM-only
			// schedule fires on the 15th.
			name: "baseline star DOW: DOM-only schedule fires on the 15th",
			expr: "0 0 15 * *",
			want: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			// Symmetric baseline: DOM "*" unrestricted -> AND path -> pure
			// weekday schedule fires on the next Friday.
			name: "baseline star DOM: DOW-only schedule fires on Friday",
			expr: "0 0 * * 5",
			want: time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, ok := cron.NextAfter(tt.expr, from)

			require.True(t, ok, "schedule %q must produce a next run", tt.expr)
			assert.Equal(t, tt.want, next)
		})
	}
}
