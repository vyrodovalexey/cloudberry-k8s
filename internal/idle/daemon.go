// Package idle provides the idle session enforcement daemon.
// The daemon periodically scans database sessions and terminates those
// that exceed configured idle timeouts per resource group.
package idle

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// idleTracerName is the tracer name for idle daemon spans.
const idleTracerName = "idle-daemon"

// DefaultScanInterval is the default interval between idle session scans.
const DefaultScanInterval = 30 * time.Second

// Session state constants used for idle detection.
const (
	stateIdle                   = "idle"
	stateActive                 = "active"
	stateIdleInTransaction      = "idle in transaction"
	stateIdleInTransactionAbort = "idle in transaction (aborted)"
)

// IdleRule represents a parsed idle session rule ready for enforcement.
type IdleRule struct {
	// Name is the rule name.
	Name string
	// Enabled controls whether the rule is active.
	Enabled bool
	// ResourceGroup is the target resource group.
	ResourceGroup string
	// IdleTimeout is the idle timeout duration.
	IdleTimeout time.Duration
	// ExcludeInTransaction excludes sessions that are in a transaction.
	ExcludeInTransaction bool
	// TerminateMessage is the message logged on termination.
	TerminateMessage string
}

// reconnectBackoff defines the exponential backoff parameters for DB reconnection.
const (
	reconnectInitialBackoff = 1 * time.Second
	reconnectMaxBackoff     = 60 * time.Second
	reconnectMultiplier     = 2
	healthCheckInterval     = 60 * time.Second
)

// DBClientFactory defines the interface for creating database clients.
// This allows the daemon to reconnect when the connection drops.
type DBClientFactory interface {
	NewClient(ctx context.Context) (db.Client, error)
}

// Config holds daemon configuration.
type Config struct {
	// ClusterName is the name of the Cloudberry cluster.
	ClusterName string
	// Namespace is the Kubernetes namespace.
	Namespace string
	// ScanInterval is how often to scan sessions (default: 30s).
	ScanInterval time.Duration
	// DBClient is the database client used to list and terminate sessions.
	DBClient db.Client
	// DBClientFactory is an optional factory for reconnecting the DB client.
	// When set, the daemon will attempt to reconnect on connection failures.
	DBClientFactory DBClientFactory
	// Metrics is the metrics recorder for idle session termination events.
	Metrics metrics.Recorder
	// Logger is the structured logger.
	Logger *slog.Logger
}

// Daemon enforces idle session rules by periodically scanning database sessions
// and terminating those that exceed the configured idle timeout.
type Daemon struct {
	config           Config
	rules            []IdleRule
	mu               sync.RWMutex
	cancel           context.CancelFunc
	done             chan struct{}
	consecutiveFails int
}

// New creates a new idle session daemon with the given configuration.
// If ScanInterval is zero, DefaultScanInterval is used.
// If Logger is nil, slog.Default() is used.
func New(cfg Config) *Daemon {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = DefaultScanInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Daemon{
		config: cfg,
	}
}

// Start begins the daemon's scan loop in a background goroutine.
// It is safe to call Start multiple times; subsequent calls are no-ops
// if the daemon is already running.
func (d *Daemon) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Already running — no-op.
	if d.cancel != nil {
		return
	}

	scanCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.done = make(chan struct{})

	go d.scanLoop(scanCtx)
}

// Stop gracefully stops the daemon and waits for the scan loop to exit.
// It is safe to call Stop without a prior Start (no-op).
func (d *Daemon) Stop() {
	d.mu.Lock()
	cancelFn := d.cancel
	doneCh := d.done
	d.cancel = nil
	d.mu.Unlock()

	if cancelFn == nil {
		// Not running — no-op.
		return
	}

	cancelFn()
	if doneCh != nil {
		<-doneCh
	}
}

// UpdateRules replaces the current idle rules atomically.
func (d *Daemon) UpdateRules(rules []IdleRule) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rules = make([]IdleRule, len(rules))
	copy(d.rules, rules)
}

// Rules returns a copy of the current idle rules (for testing).
func (d *Daemon) Rules() []IdleRule {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]IdleRule, len(d.rules))
	copy(result, d.rules)
	return result
}

// scanLoop runs the periodic scan cycle until the context is canceled.
// It includes a health check mechanism that detects connection failures
// and attempts to reconnect with exponential backoff.
func (d *Daemon) scanLoop(ctx context.Context) {
	defer close(d.done)

	ticker := time.NewTicker(d.config.ScanInterval)
	defer ticker.Stop()

	healthTicker := time.NewTicker(healthCheckInterval)
	defer healthTicker.Stop()

	d.setUpMetric(true)
	// The daemon liveness gauge drops to 0 on ANY loop exit (Stop or fatal).
	defer d.setUpMetric(false)

	d.config.Logger.Info("idle session daemon started",
		"cluster", d.config.ClusterName,
		"namespace", d.config.Namespace,
		"scanInterval", d.config.ScanInterval,
	)

	for {
		select {
		case <-ctx.Done():
			d.config.Logger.Info("idle session daemon stopped",
				"cluster", d.config.ClusterName,
				"namespace", d.config.Namespace,
			)
			return
		case <-healthTicker.C:
			d.healthCheck(ctx)
		case <-ticker.C:
			d.runScanCycle(ctx)
		}
	}
}

// setUpMetric publishes the daemon liveness gauge (nil-safe).
func (d *Daemon) setUpMetric(up bool) {
	if d.config.Metrics != nil {
		d.config.Metrics.SetIdleDaemonUp(d.config.ClusterName, d.config.Namespace, up)
	}
}

// runScanCycle executes one scan cycle inside an "idle.scan" span (D-3).
// Scan failures increment the failure counter and — after 3 consecutive
// failures — trigger a reconnect WITHIN the span context, so reconnect
// attempts appear as span events on the failing scan.
func (d *Daemon) runScanCycle(ctx context.Context) {
	ctx, span := telemetry.StartSpan(ctx, idleTracerName, "idle.scan",
		trace.WithAttributes(
			attribute.String("k8s.cluster", d.config.ClusterName),
			attribute.String("k8s.namespace", d.config.Namespace),
		),
	)
	defer span.End()

	evaluated, terminated, err := d.scanAndEnforce(ctx)
	span.SetAttributes(
		attribute.Int("idle.sessions_evaluated", evaluated),
		attribute.Int("idle.sessions_terminated", terminated),
	)
	if err == nil {
		d.consecutiveFails = 0
		return
	}

	telemetry.SetSpanError(span, err)
	d.consecutiveFails++
	if d.config.Metrics != nil {
		d.config.Metrics.RecordIdleScanFailure(d.config.ClusterName, d.config.Namespace)
	}
	d.config.Logger.Error("idle session scan failed",
		"cluster", d.config.ClusterName,
		"namespace", d.config.Namespace,
		"consecutiveFailures", d.consecutiveFails,
		"error", err,
	)
	// Attempt reconnection after consecutive failures.
	if d.consecutiveFails >= 3 {
		d.attemptReconnect(ctx)
	}
}

// healthCheck pings the database to verify the connection is alive.
// If the ping fails, it attempts to reconnect.
func (d *Daemon) healthCheck(ctx context.Context) {
	if d.config.DBClient == nil {
		return
	}
	if err := d.config.DBClient.Ping(ctx); err != nil {
		d.config.Logger.Warn("idle daemon health check failed, attempting reconnect",
			"cluster", d.config.ClusterName,
			"namespace", d.config.Namespace,
			"error", err,
		)
		d.attemptReconnect(ctx)
	}
}

// reconnectMaxAttempts bounds one reconnect cycle (initial try + retries).
const reconnectMaxAttempts = 5

// attemptReconnect tries to create a new DB client using the factory via the
// shared util.RetryWithBackoff helper (exponential backoff WITH jitter —
// thundering-herd safe, L-10). Each attempt is recorded on the
// cloudberry_idle_reconnect_attempts_total counter and as a span event on the
// active span (reconnects are events, not separate spans — D-3). If the
// factory is not configured, reconnection is skipped.
func (d *Daemon) attemptReconnect(ctx context.Context) {
	if d.config.DBClientFactory == nil {
		d.config.Logger.Debug("no DB client factory configured, skipping reconnect",
			"cluster", d.config.ClusterName,
			"namespace", d.config.Namespace,
		)
		return
	}

	span := trace.SpanFromContext(ctx)
	retryOpts := util.RetryOptions{
		MaxRetries:     reconnectMaxAttempts - 1,
		InitialBackoff: reconnectInitialBackoff,
		MaxBackoff:     reconnectMaxBackoff,
		Multiplier:     reconnectMultiplier,
		JitterFraction: 0.1,
	}

	attempt := 0
	err := util.RetryWithBackoff(ctx, retryOpts, func(retryCtx context.Context) error {
		attempt++
		d.config.Logger.Info("attempting DB reconnection",
			"cluster", d.config.ClusterName,
			"namespace", d.config.Namespace,
			"attempt", attempt,
			"maxAttempts", reconnectMaxAttempts,
		)

		newClient, connErr := d.config.DBClientFactory.NewClient(retryCtx)
		d.recordReconnectAttempt(span, attempt, connErr)
		if connErr != nil {
			d.config.Logger.Warn("DB reconnection attempt failed",
				"cluster", d.config.ClusterName,
				"namespace", d.config.Namespace,
				"attempt", attempt,
				"error", connErr,
			)
			return connErr
		}

		// Close the old client and replace with the new one.
		oldClient := d.config.DBClient
		d.config.DBClient = newClient
		if oldClient != nil {
			oldClient.Close()
		}
		d.consecutiveFails = 0
		return nil
	})
	if err != nil {
		d.config.Logger.Error("DB reconnection failed after all attempts",
			"cluster", d.config.ClusterName,
			"namespace", d.config.Namespace,
			"maxAttempts", reconnectMaxAttempts,
			"error", err,
		)
		return
	}

	d.config.Logger.Info("DB reconnection successful",
		"cluster", d.config.ClusterName,
		"namespace", d.config.Namespace,
		"attempt", attempt,
	)
}

// recordReconnectAttempt records one reconnect attempt on the metrics counter
// and as a span event (nil-safe for both).
func (d *Daemon) recordReconnectAttempt(span trace.Span, attempt int, connErr error) {
	result := "success"
	if connErr != nil {
		result = "error"
	}
	if d.config.Metrics != nil {
		d.config.Metrics.RecordIdleReconnectAttempt(
			d.config.ClusterName, d.config.Namespace, result)
	}
	telemetry.AddSpanEvent(span, "idle.reconnect",
		attribute.Int("attempt", attempt),
		attribute.String("result", result),
	)
}

// scanAndEnforce performs one scan cycle:
//  1. Get current rules (under read lock).
//  2. Call config.DBClient.ListSessionsWithResourceGroup(ctx).
//  3. For each session, check each enabled rule.
//  4. If session matches rule's resource group AND is idle beyond timeout, terminate.
//  5. Record metric and log.
//
// It returns the number of sessions evaluated and terminated (idle.scan span
// attributes) plus the scan error, if any.
func (d *Daemon) scanAndEnforce(ctx context.Context) (evaluated, terminated int, err error) {
	// Get a snapshot of current rules under read lock.
	d.mu.RLock()
	rules := make([]IdleRule, len(d.rules))
	copy(rules, d.rules)
	d.mu.RUnlock()

	if len(rules) == 0 {
		return 0, 0, nil
	}

	// Check if any rule is enabled before querying the database.
	hasEnabled := false
	for i := range rules {
		if rules[i].Enabled {
			hasEnabled = true
			break
		}
	}
	if !hasEnabled {
		return 0, 0, nil
	}

	sessions, err := d.config.DBClient.ListSessionsWithResourceGroup(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("listing sessions with resource group: %w", err)
	}

	now := time.Now()
	evaluated = len(sessions)

	for i := range sessions {
		for j := range rules {
			if !rules[j].Enabled {
				continue
			}

			if !isSessionIdle(sessions[i], rules[j], now) {
				continue
			}

			// Session is idle beyond timeout — terminate it. A session can
			// die only once: stop evaluating further rules for it after a
			// successful termination (L-6) so overlapping rules do not issue
			// redundant terminate calls (and inflate the metric/log noise).
			if d.terminateSession(ctx, sessions[i], rules[j]) {
				terminated++
				break
			}
		}
	}

	return evaluated, terminated, nil
}

// terminateSession terminates an idle session inside an "idle.terminate"
// child span (D-3) and records the event. It returns true when the session
// was actually terminated.
func (d *Daemon) terminateSession(ctx context.Context, session db.SessionWithGroup, rule IdleRule) bool {
	idleDuration := time.Since(session.QueryStart)

	// PID and rule name are bounded attributes (PIDs recycle; rule names come
	// from the CR spec). Usernames are NOT recorded on the span.
	ctx, span := telemetry.StartSpan(ctx, idleTracerName, "idle.terminate",
		trace.WithAttributes(
			attribute.Int("idle.pid", int(session.PID)),
			attribute.String("idle.rule", rule.Name),
		),
	)
	defer span.End()

	terminated, err := d.config.DBClient.TerminateSession(ctx, session.PID)
	if err != nil {
		telemetry.SetSpanError(span, err)
		d.config.Logger.Error("failed to terminate idle session",
			"pid", session.PID,
			"username", session.Username,
			"resourceGroup", session.ResourceGroup,
			"rule", rule.Name,
			"idleDuration", idleDuration.Round(time.Second),
			"error", err,
		)
		return false
	}

	if !terminated {
		d.config.Logger.Warn("idle session termination returned false (session may have already ended)",
			"pid", session.PID,
			"username", session.Username,
			"rule", rule.Name,
		)
		return false
	}

	// Record metric.
	if d.config.Metrics != nil {
		d.config.Metrics.RecordIdleSessionTermination(
			d.config.ClusterName,
			d.config.Namespace,
			rule.Name,
		)
	}

	// Log the termination with the rule's terminate message.
	logMsg := "idle session terminated"
	if rule.TerminateMessage != "" {
		logMsg = rule.TerminateMessage
	}

	d.config.Logger.Info(logMsg,
		"pid", session.PID,
		"username", session.Username,
		"resourceGroup", session.ResourceGroup,
		"state", session.State,
		"rule", rule.Name,
		"idleTimeout", rule.IdleTimeout,
		"idleDuration", idleDuration.Round(time.Second),
		"cluster", d.config.ClusterName,
		"namespace", d.config.Namespace,
	)
	return true
}

// isSessionIdle determines if a session should be terminated based on the rule.
//
// Returns true if:
//   - Session state is "idle" AND idle duration exceeds rule timeout.
//   - Session state is "idle in transaction" AND rule.ExcludeInTransaction is false
//     AND idle duration exceeds rule timeout.
//   - Session state is "idle in transaction (aborted)" AND rule.ExcludeInTransaction is false
//     AND idle duration exceeds rule timeout.
//
// Returns false if:
//   - Rule is not enabled.
//   - Session's resource group does not match the rule's resource group (when rule specifies one).
//   - Session state is "active" (never terminated by idle rules).
//   - Session state is "idle in transaction" or "idle in transaction (aborted)"
//     AND rule.ExcludeInTransaction is true.
//   - Idle duration has not exceeded the rule timeout.
//   - Session state is empty or unrecognized.
func isSessionIdle(session db.SessionWithGroup, rule IdleRule, now time.Time) bool {
	if !rule.Enabled {
		return false
	}

	// Match resource group: if the rule specifies a resource group,
	// the session must belong to that group.
	if rule.ResourceGroup != "" && session.ResourceGroup != rule.ResourceGroup {
		return false
	}

	// Determine eligibility based on session state.
	switch session.State {
	case stateActive:
		// Active sessions are never terminated by idle rules.
		return false

	case stateIdleInTransaction, stateIdleInTransactionAbort:
		if rule.ExcludeInTransaction {
			return false
		}
		// Fall through to check idle duration.

	case stateIdle:
		// Eligible — check idle duration below.

	default:
		// Unknown or empty state — not eligible.
		return false
	}

	// Calculate idle duration from the last query start time.
	idleDuration := now.Sub(session.QueryStart)
	return idleDuration > rule.IdleTimeout
}

// ParseIdleRules converts CRD idle rules to daemon rules.
// Returns an error if any rule has an invalid idle timeout duration string.
func ParseIdleRules(crdRules []cbv1alpha1.IdleSessionRule) ([]IdleRule, error) {
	if len(crdRules) == 0 {
		return []IdleRule{}, nil
	}

	rules := make([]IdleRule, 0, len(crdRules))
	for _, cr := range crdRules {
		timeout, err := time.ParseDuration(cr.IdleTimeout)
		if err != nil {
			return nil, fmt.Errorf("parsing idle timeout %q for rule %q: %w",
				cr.IdleTimeout, cr.Name, err)
		}

		rules = append(rules, IdleRule{
			Name:                 cr.Name,
			Enabled:              cr.Enabled,
			ResourceGroup:        cr.ResourceGroup,
			IdleTimeout:          timeout,
			ExcludeInTransaction: cr.ExcludeInTransaction,
			TerminateMessage:     cr.TerminateMessage,
		})
	}

	return rules, nil
}
