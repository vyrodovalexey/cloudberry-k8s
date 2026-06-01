// Package api: cron.go implements a minimal standard 5-field cron schedule
// evaluator used to compute the next backup CronJob run time. It supports the
// common field syntax (numbers, "*", ranges "a-b", lists "a,b" and steps "*/n")
// which covers the schedules accepted by Kubernetes CronJobs.
package api

import (
	"strconv"
	"strings"
	"time"
)

// cronFieldBounds describes the inclusive range of each cron field.
type cronFieldBounds struct {
	min int
	max int
}

// cronSchedule holds the per-field allowed-value sets of a parsed cron schedule.
type cronSchedule struct {
	minutes  map[int]bool
	hours    map[int]bool
	days     map[int]bool
	months   map[int]bool
	weekdays map[int]bool
}

// maxCronSearchMinutes bounds the forward search for the next matching minute to
// avoid unbounded loops for impossible schedules (e.g. Feb 30).
const maxCronSearchMinutes = 366 * 24 * 60

// computeNextCron returns the next time at or after `from` that matches the
// standard 5-field cron `schedule`. The second return value is false when the
// schedule cannot be parsed or no match is found within the search window.
func computeNextCron(schedule string, from time.Time) (time.Time, bool) {
	parsed, ok := parseCron(schedule)
	if !ok {
		return time.Time{}, false
	}

	// Start from the next whole minute after `from`.
	t := from.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < maxCronSearchMinutes; i++ {
		if parsed.matches(t) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

// matches reports whether t satisfies all fields of the schedule. Following the
// cron convention, when both day-of-month and day-of-week are restricted the
// match succeeds if either matches.
func (c *cronSchedule) matches(t time.Time) bool {
	if !c.minutes[t.Minute()] || !c.hours[t.Hour()] || !c.months[int(t.Month())] {
		return false
	}
	return c.dayMatches(t)
}

// dayMatches applies the day-of-month / day-of-week OR semantics.
func (c *cronSchedule) dayMatches(t time.Time) bool {
	domRestricted := len(c.days) != 31
	dowRestricted := len(c.weekdays) != 7
	dom := c.days[t.Day()]
	dow := c.weekdays[int(t.Weekday())]

	if domRestricted && dowRestricted {
		return dom || dow
	}
	return dom && dow
}

// parseCron parses a standard 5-field cron expression.
func parseCron(schedule string) (*cronSchedule, bool) {
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return nil, false
	}

	bounds := []cronFieldBounds{
		{0, 59}, // minute
		{0, 23}, // hour
		{1, 31}, // day of month
		{1, 12}, // month
		{0, 6},  // day of week (0=Sunday)
	}

	sets := make([]map[int]bool, len(fields))
	for i, field := range fields {
		set, ok := parseCronField(field, bounds[i])
		if !ok {
			return nil, false
		}
		sets[i] = set
	}

	return &cronSchedule{
		minutes:  sets[0],
		hours:    sets[1],
		days:     sets[2],
		months:   sets[3],
		weekdays: sets[4],
	}, true
}

// parseCronField parses a single cron field (which may be a comma-separated list)
// into the set of allowed integer values.
func parseCronField(field string, b cronFieldBounds) (map[int]bool, bool) {
	result := make(map[int]bool)
	for _, part := range strings.Split(field, ",") {
		if !parseCronPart(part, b, result) {
			return nil, false
		}
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

// parseCronPart parses one comma-separated component into result.
func parseCronPart(part string, b cronFieldBounds, result map[int]bool) bool {
	rangeSpec, step, ok := splitStep(part)
	if !ok {
		return false
	}

	lo, hi, ok := parseRange(rangeSpec, b)
	if !ok {
		return false
	}

	for v := lo; v <= hi; v += step {
		result[v] = true
	}
	return true
}

// splitStep splits "a-b/n" into ("a-b", n). When no step is present, n is 1.
func splitStep(part string) (rangeSpec string, step int, ok bool) {
	spec, stepStr, hasStep := strings.Cut(part, "/")
	if !hasStep {
		return spec, 1, true
	}
	n, err := strconv.Atoi(stepStr)
	if err != nil || n <= 0 {
		return "", 0, false
	}
	return spec, n, true
}

// parseRange resolves a range specifier ("*", "a", or "a-b") to its bounds.
func parseRange(spec string, b cronFieldBounds) (lo, hi int, ok bool) {
	if spec == "*" {
		return b.min, b.max, true
	}

	loStr, hiStr, hasRange := strings.Cut(spec, "-")
	if !hasRange {
		v, valid := parseBounded(loStr, b)
		return v, v, valid
	}

	loVal, validLo := parseBounded(loStr, b)
	if !validLo {
		return 0, 0, false
	}
	hiVal, validHi := parseBounded(hiStr, b)
	if !validHi || hiVal < loVal {
		return 0, 0, false
	}
	return loVal, hiVal, true
}

// parseBounded parses an integer and validates it against the field bounds.
func parseBounded(s string, b cronFieldBounds) (int, bool) {
	v, err := strconv.Atoi(s)
	if err != nil || v < b.min || v > b.max {
		return 0, false
	}
	return v, true
}
