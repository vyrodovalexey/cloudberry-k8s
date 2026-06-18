package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeNextCron(t *testing.T) {
	from := time.Date(2026, 5, 19, 2, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		schedule string
		want     time.Time
	}{
		{
			name:     "daily at 3am",
			schedule: "0 3 * * *",
			want:     time.Date(2026, 5, 19, 3, 0, 0, 0, time.UTC),
		},
		{
			name:     "every 15 minutes",
			schedule: "*/15 * * * *",
			want:     time.Date(2026, 5, 19, 2, 45, 0, 0, time.UTC),
		},
		{
			name:     "specific minute and hour list",
			schedule: "0 0,12 * * *",
			want:     time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "range of hours",
			schedule: "0 4-6 * * *",
			want:     time.Date(2026, 5, 19, 4, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next, ok := computeNextCron(tc.schedule, from)
			require.True(t, ok)
			assert.Equal(t, tc.want, next)
		})
	}
}

func TestComputeNextCron_Weekday(t *testing.T) {
	// 2026-05-19 is a Tuesday. Next Sunday 03:00 is 2026-05-24.
	from := time.Date(2026, 5, 19, 4, 0, 0, 0, time.UTC)
	next, ok := computeNextCron("0 3 * * 0", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 5, 24, 3, 0, 0, 0, time.UTC), next)
}

func TestComputeNextCron_Invalid(t *testing.T) {
	from := time.Now()
	cases := []string{
		"",
		"0 3 * *",     // too few fields
		"0 3 * * * *", // too many fields
		"60 3 * * *",  // minute out of range
		"0 24 * * *",  // hour out of range
		"0 3 * * 8",   // weekday out of range (7 is valid Sunday; 8 is not)
		"0 3 */0 * *", // zero step
		"abc 3 * * *", // non-numeric
		"0 3 32 * *",  // day out of range
		"0 3 5-2 * *", // inverted range
	}
	for _, c := range cases {
		_, ok := computeNextCron(c, from)
		assert.Falsef(t, ok, "schedule %q should be invalid", c)
	}
}

func TestIsValidCronSchedule(t *testing.T) {
	assert.True(t, isValidCronSchedule("0 3 * * *"))
	assert.False(t, isValidCronSchedule("nope"))
}
