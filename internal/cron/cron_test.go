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
	}
	for _, expr := range valid {
		assert.NoError(t, cron.Validate(expr), expr)
	}
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
		"* * * * 7",   // day-of-week out of range
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
