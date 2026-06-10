package webhook

import "github.com/cloudberry-contrib/cloudberry-k8s/internal/cron"

// validateCron validates a standard 5-field cron expression by delegating to
// the shared internal/cron engine (L-2: the validator and the next-run
// evaluator can never drift because they share one parser).
func validateCron(expr string) error {
	return cron.Validate(expr)
}
