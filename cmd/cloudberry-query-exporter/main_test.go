package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "days 30d", input: "30d", want: 720 * time.Hour},
		{name: "days 90d", input: "90d", want: 2160 * time.Hour},
		{name: "weeks 2w", input: "2w", want: 336 * time.Hour},
		{name: "go duration hours", input: "720h", want: 720 * time.Hour},
		{name: "go duration ms", input: "1000ms", want: 1000 * time.Millisecond},
		{name: "zero days", input: "0d", want: 0},
		{name: "empty defaults", input: "", want: defaultHistoryRetention},
		{name: "invalid abc", input: "abc", wantErr: true},
		{name: "invalid unit 30x", input: "30x", wantErr: true},
		{name: "negative days", input: "-5d", wantErr: true},
		{name: "negative duration", input: "-5h", wantErr: true},
		{name: "non-numeric days", input: "abd", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseRetention(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
