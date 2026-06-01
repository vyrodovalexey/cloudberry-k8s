package webhook

import (
	"fmt"
	"strconv"
	"strings"
)

// cronFieldBound describes the inclusive numeric bounds of a standard cron field.
type cronFieldBound struct {
	name string
	min  int
	max  int
}

// cronFieldBounds lists the five standard cron fields in order:
// minute, hour, day-of-month, month, day-of-week.
var cronFieldBounds = []cronFieldBound{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day-of-month", min: 1, max: 31},
	{name: "month", min: 1, max: 12},
	{name: "day-of-week", min: 0, max: 6},
}

// validateCron validates a standard 5-field cron expression
// (minute hour day-of-month month day-of-week). It is self-contained and does
// not depend on any external cron library. Supported syntax per field:
//   - "*"            wildcard
//   - "a"            single value within the field bounds
//   - "a-b"          inclusive range within the field bounds
//   - "a,b,c"        comma-separated list of any of the above
//   - "*/n"          step over the whole range
//   - "a-b/n"        step over a sub-range
func validateCron(expr string) error {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != len(cronFieldBounds) {
		return fmt.Errorf("cron expression must have 5 space-separated fields, got %d", len(fields))
	}
	for i, field := range fields {
		if err := validateCronField(field, cronFieldBounds[i]); err != nil {
			return fmt.Errorf("invalid %s field %q: %w", cronFieldBounds[i].name, field, err)
		}
	}
	return nil
}

// validateCronField validates a single cron field, which may be a comma list.
func validateCronField(field string, bound cronFieldBound) error {
	if field == "" {
		return fmt.Errorf("field is empty")
	}
	for _, part := range strings.Split(field, ",") {
		if err := validateCronListItem(part, bound); err != nil {
			return err
		}
	}
	return nil
}

// validateCronListItem validates a single list item (range/step/value/wildcard).
func validateCronListItem(item string, bound cronFieldBound) error {
	rangePart, stepPart, hasStep := strings.Cut(item, "/")
	if hasStep {
		if err := validateCronStep(stepPart); err != nil {
			return err
		}
	}
	return validateCronRange(rangePart, bound, hasStep)
}

// validateCronStep validates the numeric step value after a "/".
func validateCronStep(step string) error {
	n, err := strconv.Atoi(step)
	if err != nil {
		return fmt.Errorf("step must be a positive integer, got %q", step)
	}
	if n < 1 {
		return fmt.Errorf("step must be >= 1, got %d", n)
	}
	return nil
}

// validateCronRange validates the range part of a cron item, which may be "*",
// a single value, or "a-b". When a step is present, a bare "*" is allowed.
func validateCronRange(rangePart string, bound cronFieldBound, hasStep bool) error {
	if rangePart == "*" {
		return nil
	}
	lo, hi, isRange := strings.Cut(rangePart, "-")
	if !isRange {
		if hasStep {
			// "a/n" is treated as start value a stepping to the field maximum.
			return validateCronValue(rangePart, bound)
		}
		return validateCronValue(rangePart, bound)
	}
	low, err := cronValue(lo, bound)
	if err != nil {
		return err
	}
	high, err := cronValue(hi, bound)
	if err != nil {
		return err
	}
	if low > high {
		return fmt.Errorf("range start %d is greater than end %d", low, high)
	}
	return nil
}

// validateCronValue validates a single numeric value within the field bounds.
func validateCronValue(value string, bound cronFieldBound) error {
	_, err := cronValue(value, bound)
	return err
}

// cronValue parses and bounds-checks a single numeric cron value.
func cronValue(value string, bound cronFieldBound) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid integer", value)
	}
	if n < bound.min || n > bound.max {
		return 0, fmt.Errorf("value %d out of range [%d-%d]", n, bound.min, bound.max)
	}
	return n, nil
}
