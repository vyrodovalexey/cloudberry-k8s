package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCron(t *testing.T) {
	tests := []struct {
		name      string
		expr      string
		expectErr bool
	}{
		{name: "all wildcards", expr: "* * * * *", expectErr: false},
		{name: "daily at 2am", expr: "0 2 * * *", expectErr: false},
		{name: "leading/trailing spaces", expr: "  0 2 * * *  ", expectErr: false},
		{name: "range", expr: "0 9-17 * * 1-5", expectErr: false},
		{name: "list", expr: "0,15,30,45 * * * *", expectErr: false},
		{name: "step over wildcard", expr: "*/15 * * * *", expectErr: false},
		{name: "step over range", expr: "0 0-12/2 * * *", expectErr: false},
		{name: "weekly sunday", expr: "0 3 * * 0", expectErr: false},
		{name: "combined list and range", expr: "0 1,2,10-12 1 1 0", expectErr: false},

		{name: "empty", expr: "", expectErr: true},
		{name: "too few fields", expr: "* * * *", expectErr: true},
		{name: "too many fields", expr: "* * * * * *", expectErr: true},
		{name: "minute out of range", expr: "60 * * * *", expectErr: true},
		{name: "hour out of range", expr: "* 24 * * *", expectErr: true},
		{name: "dom out of range low", expr: "* * 0 * *", expectErr: true},
		{name: "dom out of range high", expr: "* * 32 * *", expectErr: true},
		{name: "month out of range", expr: "* * * 13 *", expectErr: true},
		{name: "sunday as seven", expr: "* * * * 7", expectErr: false},
		{name: "dow out of range", expr: "* * * * 8", expectErr: true},
		{name: "non-numeric", expr: "a * * * *", expectErr: true},
		{name: "inverted range", expr: "30-10 * * * *", expectErr: true},
		{name: "zero step", expr: "*/0 * * * *", expectErr: true},
		{name: "negative step", expr: "*/-1 * * * *", expectErr: true},
		{name: "non-numeric step", expr: "*/x * * * *", expectErr: true},
		{name: "range bound out of range", expr: "0 0-30 * * *", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCron(tt.expr)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
