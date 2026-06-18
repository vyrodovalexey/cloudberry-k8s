// Package cron implements the single shared standard 5-field cron engine
// (minute hour day-of-month month day-of-week) used across the operator:
// the REST API computes next-run times for backup CronJobs and the admission
// webhook validates schedules. Centralizing parsing prevents the two engines
// from drifting (code review L-2). Supported field syntax: numbers, "*",
// ranges "a-b", lists "a,b" and steps "*/n" / "a-b/n" — the subset accepted
// by Kubernetes CronJobs.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// fieldBounds describes one standard cron field with its inclusive bounds.
type fieldBounds struct {
	name string
	min  int
	max  int
}

// bounds lists the five standard cron fields in order.
//
//nolint:gochecknoglobals // immutable field-definition table
var bounds = []fieldBounds{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day-of-month", min: 1, max: 31},
	{name: "month", min: 1, max: 12},
	// Day-of-week accepts 0-7 where both 0 and 7 mean Sunday, matching standard
	// and Kubernetes CronJob semantics. The value 7 is normalized to 0 when the
	// allowed-value set is populated (see parsePart).
	{name: "day-of-week", min: 0, max: 7},
}

// dayOfWeekFieldName identifies the day-of-week field for Sunday (7→0)
// normalization.
const dayOfWeekFieldName = "day-of-week"

// Schedule holds the per-field allowed-value sets of a parsed cron schedule.
type Schedule struct {
	minutes  map[int]bool
	hours    map[int]bool
	days     map[int]bool
	months   map[int]bool
	weekdays map[int]bool
}

// maxSearchMinutes bounds the forward search for the next matching minute to
// avoid unbounded loops for impossible schedules (e.g. Feb 30).
const maxSearchMinutes = 366 * 24 * 60

// Parse parses a standard 5-field cron expression. The error message names
// the offending field for actionable validation feedback.
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != len(bounds) {
		return nil, fmt.Errorf("cron expression must have 5 space-separated fields, got %d", len(fields))
	}

	sets := make([]map[int]bool, len(fields))
	for i, field := range fields {
		set, err := parseField(field, bounds[i])
		if err != nil {
			return nil, fmt.Errorf("invalid %s field %q: %w", bounds[i].name, field, err)
		}
		sets[i] = set
	}

	return &Schedule{
		minutes:  sets[0],
		hours:    sets[1],
		days:     sets[2],
		months:   sets[3],
		weekdays: sets[4],
	}, nil
}

// Validate reports whether expr is a parseable 5-field cron expression.
func Validate(expr string) error {
	_, err := Parse(expr)
	return err
}

// Next returns the next time at or after `from` that matches the schedule.
// The second return value is false when no match is found within the search
// window (impossible schedules).
func (s *Schedule) Next(from time.Time) (time.Time, bool) {
	// Start from the next whole minute after `from`.
	t := from.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < maxSearchMinutes; i++ {
		if s.matches(t) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

// NextAfter is a convenience wrapper: parse expr and compute the next run at
// or after `from`. ok is false when the expression is invalid or no match is
// found within the search window.
func NextAfter(expr string, from time.Time) (next time.Time, ok bool) {
	schedule, err := Parse(expr)
	if err != nil {
		return time.Time{}, false
	}
	return schedule.Next(from)
}

// matches reports whether t satisfies all fields of the schedule. Following the
// cron convention, when both day-of-month and day-of-week are restricted the
// match succeeds if either matches.
func (s *Schedule) matches(t time.Time) bool {
	if !s.minutes[t.Minute()] || !s.hours[t.Hour()] || !s.months[int(t.Month())] {
		return false
	}
	return s.dayMatches(t)
}

// dayMatches applies the day-of-month / day-of-week OR semantics.
func (s *Schedule) dayMatches(t time.Time) bool {
	domRestricted := len(s.days) != 31
	dowRestricted := len(s.weekdays) != 7
	dom := s.days[t.Day()]
	dow := s.weekdays[int(t.Weekday())]

	if domRestricted && dowRestricted {
		return dom || dow
	}
	return dom && dow
}

// parseField parses a single cron field (which may be a comma-separated list)
// into the set of allowed integer values.
func parseField(field string, b fieldBounds) (map[int]bool, error) {
	if field == "" {
		return nil, fmt.Errorf("field is empty")
	}
	result := make(map[int]bool)
	for _, part := range strings.Split(field, ",") {
		if err := parsePart(part, b, result); err != nil {
			return nil, err
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("field selects no values")
	}
	return result, nil
}

// parsePart parses one comma-separated component into result.
func parsePart(part string, b fieldBounds, result map[int]bool) error {
	rangeSpec, step, err := splitStep(part)
	if err != nil {
		return err
	}

	lo, hi, err := parseRange(rangeSpec, b)
	if err != nil {
		return err
	}

	for v := lo; v <= hi; v += step {
		// Sunday is representable as both 0 and 7 in day-of-week. Normalize 7 to 0
		// so NextAfter/matches (which compare against time.Weekday Sunday==0) treat
		// the two spellings identically. This also handles ranges such as "5-7"
		// (Fri,Sat,Sun → {5,6,0}).
		if b.name == dayOfWeekFieldName && v == 7 {
			result[0] = true
			continue
		}
		result[v] = true
	}
	return nil
}

// splitStep splits "a-b/n" into ("a-b", n). When no step is present, n is 1.
func splitStep(part string) (rangeSpec string, step int, err error) {
	spec, stepStr, hasStep := strings.Cut(part, "/")
	if !hasStep {
		return spec, 1, nil
	}
	n, convErr := strconv.Atoi(stepStr)
	if convErr != nil || n <= 0 {
		return "", 0, fmt.Errorf("step must be a positive integer, got %q", stepStr)
	}
	return spec, n, nil
}

// parseRange resolves a range specifier ("*", "a", or "a-b") to its bounds.
func parseRange(spec string, b fieldBounds) (lo, hi int, err error) {
	if spec == "*" {
		return b.min, b.max, nil
	}

	loStr, hiStr, hasRange := strings.Cut(spec, "-")
	if !hasRange {
		v, valErr := parseBounded(loStr, b)
		if valErr != nil {
			return 0, 0, valErr
		}
		return v, v, nil
	}

	loVal, loErr := parseBounded(loStr, b)
	if loErr != nil {
		return 0, 0, loErr
	}
	hiVal, hiErr := parseBounded(hiStr, b)
	if hiErr != nil {
		return 0, 0, hiErr
	}
	if hiVal < loVal {
		return 0, 0, fmt.Errorf("range start %d is greater than end %d", loVal, hiVal)
	}
	return loVal, hiVal, nil
}

// parseBounded parses an integer and validates it against the field bounds.
func parseBounded(s string, b fieldBounds) (int, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid integer", s)
	}
	if v < b.min || v > b.max {
		return 0, fmt.Errorf("value %d out of range [%d-%d]", v, b.min, b.max)
	}
	return v, nil
}
