package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/cron"
)

func TestValidate_Valid(t *testing.T) {
	valid := []string{
		"* * * * *",
		"0 3 * * *",
		"*/15 * * * *",
		"0 0 1 1 0",
		"5,10,15 1-5 * * 1-5",
		"0-30/5 * * * *",
		"0 0 * * 7", // Sunday-as-7 (Kubernetes/standard cron parity)
		"0 0 * * 5-7",
	}
	for _, expr := range valid {
		assert.NoError(t, cron.Validate(expr), expr)
	}
}

func TestSundayAsSevenEquivalence(t *testing.T) {
	from := time.Date(2026, 6, 10, 2, 30, 0, 0, time.UTC) // Wednesday

	// "0 0 * * 0" and "0 0 * * 7" must produce identical next-run times.
	next0, ok0 := cron.NextAfter("0 0 * * 0", from)
	require.True(t, ok0)
	next7, ok7 := cron.NextAfter("0 0 * * 7", from)
	require.True(t, ok7)
	assert.Equal(t, next0, next7)
	assert.Equal(t, time.Sunday, next7.Weekday())

	// A range including 7 must match Sunday too.
	nextRange, okRange := cron.NextAfter("0 0 * * 5-7", from)
	require.True(t, okRange)
	// The next match after Wed the 10th is Friday the 12th (weekday 5).
	assert.Equal(t, time.Friday, nextRange.Weekday())
}

// TestCronRange5To7IncludesSunday verifies the W1-A2 normalization: the
// day-of-week range "5-7" matches Friday(5), Saturday(6) AND Sunday(0). Walking
// NextAfter forward from a Thursday must land on Fri, then Sat, then Sun (TASK 13).
func TestCronRange5To7IncludesSunday(t *testing.T) {
	const expr = "0 0 * * 5-7"
	// 2026-06-11 is a Thursday.
	thursday := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	require.Equal(t, time.Thursday, thursday.Weekday())

	fri, ok := cron.NextAfter(expr, thursday)
	require.True(t, ok)
	assert.Equal(t, time.Friday, fri.Weekday(), "5-7 must match Friday")

	sat, ok := cron.NextAfter(expr, fri)
	require.True(t, ok)
	assert.Equal(t, time.Saturday, sat.Weekday(), "5-7 must match Saturday")

	sun, ok := cron.NextAfter(expr, sat)
	require.True(t, ok)
	assert.Equal(t, time.Sunday, sun.Weekday(),
		"5-7 must include Sunday via the 7->0 normalization")
}

// TestCronDayOfWeek8Rejected verifies the updated [0-7] day-of-week bound: 7 is a
// valid Sunday but 8 is rejected, and the error references the [0-7] bound (not
// the old [0-6]) (TASK 13).
func TestCronDayOfWeek8Rejected(t *testing.T) {
	err := cron.Validate("0 0 * * 8")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[0-7]",
		"the bound message must reflect the updated [0-7] day-of-week range")
	assert.NotContains(t, err.Error(), "[0-6]",
		"the message must not reference the pre-normalization [0-6] bound")

	// And 7 itself remains valid (Sunday).
	assert.NoError(t, cron.Validate("0 0 * * 7"))
}

func TestValidate_Invalid(t *testing.T) {
	invalid := []string{
		"",
		"* * * *",     // 4 fields
		"* * * * * *", // 6 fields
		"60 * * * *",  // minute out of range
		"* 24 * * *",  // hour out of range
		"* * 0 * *",   // day-of-month out of range
		"* * * 13 *",  // month out of range
		"* * * * 8",   // day-of-week out of range (7 is valid Sunday; 8 is not)
		"a * * * *",   // non-numeric
		"5-1 * * * *", // inverted range
		"*/0 * * * *", // zero step
		"*/x * * * *", // non-numeric step
		"1,, * * * *", // empty list item
	}
	for _, expr := range invalid {
		assert.Error(t, cron.Validate(expr), "expected error for %q", expr)
	}
}

func TestNextAfter(t *testing.T) {
	from := time.Date(2026, 6, 10, 2, 30, 0, 0, time.UTC)

	next, ok := cron.NextAfter("0 3 * * *", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 6, 10, 3, 0, 0, 0, time.UTC), next)

	// Every 15 minutes.
	next, ok = cron.NextAfter("*/15 * * * *", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 6, 10, 2, 45, 0, 0, time.UTC), next)

	// Weekday match (2026-06-14 is a Sunday).
	next, ok = cron.NextAfter("0 3 * * 0", from)
	require.True(t, ok)
	assert.Equal(t, time.Weekday(0), next.Weekday())

	// Invalid expression.
	_, ok = cron.NextAfter("nope", from)
	assert.False(t, ok)
}

func TestNextAfter_DayOfMonthDayOfWeekOrSemantics(t *testing.T) {
	// Both DOM and DOW restricted: match when EITHER matches (cron convention).
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC) // Wednesday the 10th
	next, ok := cron.NextAfter("0 0 15 * 5", from)       // 15th OR Friday
	require.True(t, ok)
	// 2026-06-12 is a Friday — earlier than the 15th.
	assert.Equal(t, time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC), next)
}

func TestNextAfter_ImpossibleSchedule(t *testing.T) {
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	// Feb 30 never exists: bounded search returns ok=false.
	_, ok := cron.NextAfter("0 0 30 2 *", from)
	assert.False(t, ok)
}

func TestParseAndNext(t *testing.T) {
	schedule, err := cron.Parse("30 2 * * *")
	require.NoError(t, err)
	next, ok := schedule.Next(time.Date(2026, 6, 10, 2, 30, 0, 0, time.UTC))
	require.True(t, ok)
	// Already at 2:30 — next run is tomorrow (search starts at next minute).
	assert.Equal(t, time.Date(2026, 6, 11, 2, 30, 0, 0, time.UTC), next)
}
