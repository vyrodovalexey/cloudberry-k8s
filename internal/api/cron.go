// Package api: cron.go delegates cron-schedule evaluation to the shared
// internal/cron engine (L-2: one cron implementation for the whole operator).
package api

import (
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/cron"
)

// computeNextCron returns the next time at or after `from` that matches the
// standard 5-field cron `schedule`. The second return value is false when the
// schedule cannot be parsed or no match is found within the search window.
func computeNextCron(schedule string, from time.Time) (time.Time, bool) {
	return cron.NextAfter(schedule, from)
}
