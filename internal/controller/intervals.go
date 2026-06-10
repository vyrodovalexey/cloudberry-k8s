package controller

import "time"

// reconcileIntervals carries the configurable requeue interval and
// long-operation timeout injected from the operator configuration
// (CLOUDBERRY_RECONCILE_INTERVAL / CLOUDBERRY_OPERATION_TIMEOUT, B-2/M-2).
// Zero values preserve the historical hardcoded defaults exactly, so
// reconcilers constructed without explicit configuration behave unchanged.
type reconcileIntervals struct {
	// reconcileInterval overrides requeueAfterDefault when > 0.
	reconcileInterval time.Duration
	// operationTimeout overrides the long-operation deadlines (scale,
	// upgrade phase) when > 0. The mirroring-enable timeout keeps its own
	// (longer) default unless the configured timeout exceeds it, because
	// mirror base-backups are routinely slower than other operations.
	operationTimeout time.Duration
}

// requeueDefault returns the periodic requeue interval.
func (i *reconcileIntervals) requeueDefault() time.Duration {
	if i.reconcileInterval > 0 {
		return i.reconcileInterval
	}
	return requeueAfterDefault
}

// opTimeout returns the timeout for a long-running operation, defaulting to
// the per-operation hardcoded value when no override is configured.
//
// parameter keeps per-operation defaults independent by design.
//
//nolint:unparam // both current call sites share a 10m default; the fallback
func (i *reconcileIntervals) opTimeout(fallback time.Duration) time.Duration {
	if i.operationTimeout > 0 {
		return i.operationTimeout
	}
	return fallback
}

// SetIntervals injects the configured reconcile interval and operation
// timeout. Zero values keep the built-in defaults. It returns nothing and is
// called once during operator startup (before the manager starts), so no
// synchronization is required.
func (i *reconcileIntervals) SetIntervals(reconcileInterval, operationTimeout time.Duration) {
	i.reconcileInterval = reconcileInterval
	i.operationTimeout = operationTimeout
}
