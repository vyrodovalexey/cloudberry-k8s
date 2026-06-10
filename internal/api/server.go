// Package api provides the REST API server for the cloudberry operator.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/httpjson"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/planchecker"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	apiPrefix       = "/api/v1alpha1"
	defaultPageSize = 50
	maxPageSize     = 100

	// maxBodySize is the maximum allowed request body size (1 MiB).
	maxBodySize = 1 << 20

	// httpReadTimeout is the maximum duration for reading the entire request, including the body.
	httpReadTimeout = 30 * time.Second
	// httpWriteTimeout is the maximum duration before timing out writes of the response.
	httpWriteTimeout = 60 * time.Second
	// httpIdleTimeout is the maximum amount of time to wait for the next request when keep-alives are enabled.
	httpIdleTimeout = 120 * time.Second
	// httpShutdownTimeout is the maximum time to wait for the HTTP server to
	// gracefully shut down (finish in-flight requests) before forcing closure.
	httpShutdownTimeout = 5 * time.Second

	responseKeyStatus  = "status"
	responseKeyTotal   = "total"
	responseKeyJob     = "job"
	responseKeyCluster = "cluster"
	responseKeyEnabled = "enabled"
	responseKeyPID     = "pid"
	responseKeyMessage = "message"
	responseKeyItems   = "items"

	// msgDBNotAvailable is the message returned when the database factory is not configured.
	msgDBNotAvailable = "database connection not available"

	responseKeyGroup     = "group"
	responseKeyName      = "name"
	responseKeyNamespace = "namespace"
	responseKeyCanceled  = "canceled"

	statusDeleted  = "deleted"
	statusPending  = "pending"
	statusUpdated  = "updated"
	statusCreated  = "created"
	statusRotated  = "rotated"
	statusCanceled = "canceled"
	statusMoved    = "moved"

	responseKeyMemoryLimit = "memoryLimit"
	responseKeyConcurrency = "concurrency"
	responseKeyCPUMaxPct   = "cpuMaxPercent"
	responseKeyCPUWeight   = "cpuWeight"
	responseKeyRule        = "rule"

	responseKeyActiveQueries  = "activeQueries"
	responseKeyQueuedQueries  = "queuedQueries"
	responseKeyBlockedQueries = "blockedQueries"

	statusPaused  = "paused"
	statusResumed = "resumed"

	// resultSuccess / resultError are the shared metric result label values.
	resultSuccess = "success"
	resultError   = "error"

	// errCodeClusterNotFound is the error code for cluster-not-found responses.
	errCodeClusterNotFound = "CLUSTER_NOT_FOUND"

	// errCodeInternal is the generic internal-error code.
	errCodeInternal = "INTERNAL_ERROR"

	// errCodeInvalidRequest is the error code for malformed/invalid requests.
	errCodeInvalidRequest = "INVALID_REQUEST"

	// errCodeNotImplemented is the error code for endpoints that are not
	// implemented yet (HTTP 501 responses).
	errCodeNotImplemented = "NOT_IMPLEMENTED"

	// jobLogsFlushInterval is how often streamed log output is flushed to the
	// client when following a Job's logs.
	jobLogsFlushInterval = 500 * time.Millisecond

	// labelJobName is the legacy label set by the Job controller on its pods.
	labelJobName = "job-name"
	// labelJobNameBatch is the current label set by the Job controller on its pods.
	labelJobNameBatch = "batch.kubernetes.io/job-name"

	// adminUsername is the default admin username for the credential store.
	adminUsername = "admin"
)

// dns1123SubdomainRegex validates DNS-1123 subdomain names used for cluster and namespace names.
// Must consist of lower case alphanumeric characters, '-' or '.', and must start and end with
// an alphanumeric character.
var dns1123SubdomainRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`)

// identifierRegex validates safe SQL identifiers for database operations.
// Allows alphanumeric characters and underscores, must start with a letter or underscore.
// Maximum length 63 (PostgreSQL identifier limit).
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// isValidDNS1123Name validates that a name conforms to DNS-1123 subdomain format.
func isValidDNS1123Name(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	return dns1123SubdomainRegex.MatchString(name)
}

// writeClusterNotFound writes a standardized cluster-not-found error response.
func writeClusterNotFound(w http.ResponseWriter, name string) {
	writeErrorJSON(w, http.StatusNotFound, errCodeClusterNotFound,
		fmt.Sprintf("cluster %q not found", name))
}

// isValidIdentifier validates that a name is a safe SQL identifier.
// Allows alphanumeric characters, underscores, and must start with
// a letter or underscore. Max length 63 (PostgreSQL limit).
func isValidIdentifier(name string) bool {
	return identifierRegex.MatchString(name)
}

// limitBody wraps the request body with a size-limited reader to prevent oversized payloads.
func limitBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
}

// monitorState tracks the pause/resume state for a cluster's query monitor.
//
// VOLATILITY (L-13, documented limitation): this state is held IN MEMORY
// only. It is lost on pod restart and is NOT shared between operator
// replicas (the API server runs on every replica regardless of leader
// election), so a pause issued against one replica is invisible to the
// others. Persisting the state via a cluster annotation is tracked as a
// follow-up feature; operators should treat monitor pause as a best-effort,
// single-replica convenience.
type monitorState struct {
	Paused   bool                   `json:"paused"`
	PausedAt *time.Time             `json:"pausedAt,omitempty"`
	Snapshot map[string]interface{} `json:"snapshot,omitempty"`
}

// Server is the REST API server for the cloudberry operator.
type Server struct {
	k8sClient client.Client
	// clientset is the typed Kubernetes clientset used for operations that the
	// controller-runtime client cannot perform, such as streaming pod logs.
	// It may be nil in test/non-live setups; handlers must guard against nil.
	clientset   kubernetes.Interface
	authMW      *auth.AuthMiddleware
	rateLimiter *RateLimiter
	dbFactory   db.DBClientFactory
	credStore   *auth.InMemoryCredentialStore
	metrics     metrics.Recorder
	// builder is the ResourceBuilder interface (not the concrete
	// DefaultBuilder) so handler-level guards (e.g. a nil backup Job from a
	// builder that refuses to render a broken command) stay testable.
	builder       builder.ResourceBuilder
	logger        *slog.Logger
	mux           *http.ServeMux
	monitorStates map[string]*monitorState // key: "namespace/cluster"
	monitorMu     sync.RWMutex
}

// WithClientset injects a typed Kubernetes clientset into the Server and returns
// the same Server for fluent configuration. The clientset is required for
// endpoints that stream pod logs; when it is not provided, those endpoints
// return a clear "not available" error instead of panicking. A nil clientset is
// ignored so existing call sites that do not stream logs remain unaffected.
func (s *Server) WithClientset(clientset kubernetes.Interface) *Server {
	if clientset != nil {
		s.clientset = clientset
	}
	return s
}

// NewServer creates a new API server.
// rateLimit controls the per-IP request rate limit (requests per minute).
// Use 0 to disable rate limiting (useful for performance testing).
// credStore is optional; when provided, the server can rotate admin passwords in-memory.
func NewServer(
	k8sClient client.Client,
	authMW *auth.AuthMiddleware,
	dbFactory db.DBClientFactory,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
	rateLimit int,
	credStore ...*auth.InMemoryCredentialStore,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	if rateLimit <= 0 {
		rateLimit = defaultRateLimit
	}

	s := &Server{
		k8sClient:     k8sClient,
		authMW:        authMW,
		dbFactory:     dbFactory,
		metrics:       metricsRecorder,
		builder:       builder.NewBuilder(),
		logger:        logger.With("component", "api-server"),
		mux:           http.NewServeMux(),
		monitorStates: make(map[string]*monitorState),
	}
	// The rejection callback records the 429 counter with the matched route
	// template (bounded label cardinality).
	s.rateLimiter = NewRateLimiter(rateLimit, defaultRateInterval, logger,
		WithRejectionCallback(func(r *http.Request) {
			if s.metrics != nil {
				s.metrics.RecordRateLimitRejection(s.routePattern(r))
			}
		}))

	if len(credStore) > 0 && credStore[0] != nil {
		s.credStore = credStore[0]
	}

	s.registerRoutes()
	return s
}

// Close releases resources held by the server, including stopping the rate limiter.
func (s *Server) Close() {
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
}

// Handler returns the HTTP handler for the API server.
func (s *Server) Handler() http.Handler {
	// Middleware order (outermost first): tracing opens the root span and
	// installs the shared statusRecorder; the metrics middleware reuses that
	// recorder for the status-code label; security headers apply to every
	// response. Both observability middlewares are no-ops when telemetry /
	// metrics are not configured.
	return s.tracingMiddleware(s.metricsMiddleware(auth.SecurityHeaders()(s.mux)))
}

// registerRoutes registers all API routes.
func (s *Server) registerRoutes() {
	// Health endpoints (no auth required).
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Auth management.
	s.mux.Handle("POST "+apiPrefix+"/auth/rotate-password",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleRotatePassword)))

	// Cluster management.
	s.mux.Handle("GET "+apiPrefix+"/clusters",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListClusters)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetCluster)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/status",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetClusterStatus)))
	s.mux.Handle("POST "+apiPrefix+"/clusters",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleCreateCluster)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleDeleteCluster)))

	// Scale status.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/scale/status",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetScaleStatus)))

	// Cluster operations.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/start",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleStartCluster)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/stop",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleStopCluster)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/restart",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleRestartCluster)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/reload",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleReloadConfig)))

	// Configuration.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/config",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleGetConfig)))
	s.mux.Handle("PUT "+apiPrefix+"/clusters/{name}/config",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleUpdateConfig)))

	// Segments.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/segments",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListSegments)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/mirroring",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetMirroring)))

	// Sessions.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/sessions",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleListSessions)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/sessions/{pid}/cancel",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCancelQuery)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/sessions/{pid}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleTerminateSession)))

	// Maintenance.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/maintenance/vacuum",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleVacuum)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/maintenance/analyze",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleAnalyze)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/maintenance/reindex",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleReindex)))

	// Standby.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/standby",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetStandby)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/standby/activate",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleActivateStandby)))

	// Recovery.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/recovery",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleStartRecovery)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/rebalance",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleRebalance)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/rebalance/status",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetRebalanceStatus)))

	// Workload, resource group, rule, and queue management.
	s.registerWorkloadRoutes()

	// Query monitoring, pause/resume, history, plan-check, cancel, move, export.
	s.registerQueryRoutes()

	// Exporter health — read endpoint supports guest access.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/metrics/exporters",
		s.withGuestAuth(s.withPermission(auth.PermissionBasic, s.handleGetExporterHealth)))

	// Backup and restore.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/backups",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListBackups)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/backups",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCreateBackup)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/backups/jobs",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListBackupJobs)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/backups/jobs/{job}/logs",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleBackupJobLogs)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/backups/schedule",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetBackupSchedule)))
	s.mux.Handle("PATCH "+apiPrefix+"/clusters/{name}/backups/schedule",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleUpdateBackupSchedule)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/backups/{timestamp}",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetBackup)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/backups/{timestamp}",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleDeleteBackup)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/backups/{timestamp}/restore",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleRestoreBackup)))

	// Cross-cluster migration.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/migrate",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleMigrate)))

	// PVC listing.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/pvcs",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListPVCs)))

	// Storage and recommendations.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/disk-usage",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetDiskUsage)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/tables",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListTables)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/tables/{schema}/{table}",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetTableDetail)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/recommendations",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListRecommendations)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/storage/recommendations/scan",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleTriggerRecommendationScan)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/storage/usage-report",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetUsageReport)))

	// Data loading.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/data-loading/jobs",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListDataLoadingJobs)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/data-loading/jobs",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCreateDataLoadingJob)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/data-loading/jobs/{job}",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetDataLoadingJob)))
	s.mux.Handle("PUT "+apiPrefix+"/clusters/{name}/data-loading/jobs/{job}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleUpdateDataLoadingJob)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/data-loading/jobs/{job}",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleDeleteDataLoadingJob)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/data-loading/jobs/{job}/start",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleStartDataLoadingJob)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/data-loading/jobs/{job}/stop",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleStopDataLoadingJob)))
}

// registerWorkloadRoutes registers workload, resource group, rule, and queue routes.
func (s *Server) registerWorkloadRoutes() {
	// Workload management.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetWorkload)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload/resource-groups",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListResourceGroups)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload/rules",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListWorkloadRules)))

	// Resource group management.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/workload/resource-groups",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCreateResourceGroup)))
	s.mux.Handle("PUT "+apiPrefix+"/clusters/{name}/workload/resource-groups/{groupName}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleUpdateResourceGroup)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/workload/resource-groups/{groupName}",
		s.withAuth(s.withPermission(auth.PermissionAdmin, s.handleDeleteResourceGroup)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/workload/resource-groups/{groupName}/assign",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleAssignResourceGroup)))

	// Workload rule management.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/workload/rules",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCreateWorkloadRule)))
	s.mux.Handle("PUT "+apiPrefix+"/clusters/{name}/workload/rules/{ruleName}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleUpdateWorkloadRule)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/workload/rules/{ruleName}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleDeleteWorkloadRule)))

	// Resource queue management.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload/resource-queues",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListResourceQueues)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/workload/resource-queues",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCreateResourceQueue)))
	s.mux.Handle("DELETE "+apiPrefix+"/clusters/{name}/workload/resource-queues/{queueName}",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleDeleteResourceQueue)))
}

// registerQueryRoutes registers query monitoring, pause/resume, history,
// plan-check, cancel, move, and export routes.
func (s *Server) registerQueryRoutes() {
	// Query monitoring — read endpoints support guest access.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries",
		s.withGuestAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleGetQueryMonitoring)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/active",
		s.withGuestAuth(s.withPermission(auth.PermissionBasic, s.handleGetActiveQueries)))

	// Monitor pause/resume.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/monitor/pause",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handlePauseMonitor)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/monitor/resume",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleResumeMonitor)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/monitor/state",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetMonitorState)))

	// Query history — MUST be registered before queries/{pid} to avoid path conflicts.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/history",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleGetQueryHistory)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/history/{qid}",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleGetQueryHistoryDetail)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/history/export",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleExportQueryHistory)))

	// Active query export.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/export",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleExportActiveQueries)))

	// Plan analysis — MUST be registered before queries/{pid} to avoid path conflicts.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/plan-check",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handlePlanCheck)))

	// Query cancel and move — POST routes with {pid} sub-path, no conflict with GET queries/{pid}.
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/{pid}/cancel",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleCancelQueryByPID)))
	s.mux.Handle("POST "+apiPrefix+"/clusters/{name}/queries/{pid}/move",
		s.withAuth(s.withPermission(auth.PermissionOperator, s.handleMoveQuery)))

	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/{pid}",
		s.withAuth(s.withPermission(auth.PermissionOperatorBasic, s.handleGetQueryDetail)))
}

// withAuth wraps a handler with rate limiting and authentication middleware.
// Rate limiting is applied before authentication to protect against brute-force attacks.
func (s *Server) withAuth(handler http.Handler) http.Handler {
	if s.authMW == nil {
		return handler
	}
	// Apply rate limiting before auth to prevent brute-force credential attacks.
	return s.rateLimiter.Middleware(s.authMW.Handler()(handler))
}

// withPermission wraps a handler function with permission checking.
func (s *Server) withPermission(level auth.PermissionLevel, fn http.HandlerFunc) http.Handler {
	return auth.RequirePermission(level, s.logger)(fn)
}

// isGuestAccessEnabled checks if guestAccess is enabled for the cluster in the request.
// It looks up the cluster by name and namespace from the request path and query parameters.
// Returns false if the cluster cannot be found or guestAccess is not enabled.
func (s *Server) isGuestAccessEnabled(r *http.Request) bool {
	clusterName := r.PathValue("name")
	namespace := r.URL.Query().Get("namespace")
	if clusterName == "" {
		return false
	}

	// When namespace is empty, search across all namespaces.
	if namespace == "" {
		clusterList := &cbv1alpha1.CloudberryClusterList{}
		if err := s.k8sClient.List(r.Context(), clusterList); err != nil {
			s.logger.Debug("guest access check: failed to list clusters", "error", err)
			return false
		}
		for i := range clusterList.Items {
			if clusterList.Items[i].Name == clusterName {
				enabled := clusterList.Items[i].Spec.QueryMonitoring != nil &&
					clusterList.Items[i].Spec.QueryMonitoring.GuestAccess
				if s.metrics != nil {
					s.metrics.RecordGuestAccess(clusterName, clusterList.Items[i].Namespace, enabled)
				}
				return enabled
			}
		}
		s.logger.Debug("guest access check: cluster not found", "cluster", clusterName)
		return false
	}

	cluster := &cbv1alpha1.CloudberryCluster{}
	key := types.NamespacedName{Name: clusterName, Namespace: namespace}
	if err := s.k8sClient.Get(r.Context(), key, cluster); err != nil {
		s.logger.Debug("guest access check: failed to get cluster",
			"cluster", clusterName, "namespace", namespace, "error", err)
		return false
	}

	enabled := cluster.Spec.QueryMonitoring != nil && cluster.Spec.QueryMonitoring.GuestAccess
	if s.metrics != nil {
		s.metrics.RecordGuestAccess(clusterName, namespace, enabled)
	}
	return enabled
}

// withGuestAuth wraps a handler with guest-aware authentication.
// When guestAccess is enabled for the cluster, unauthenticated GET requests are allowed
// with a guest identity that has PermissionBasic.
func (s *Server) withGuestAuth(handler http.Handler) http.Handler {
	if s.authMW == nil {
		return handler
	}
	return s.rateLimiter.Middleware(
		s.authMW.GuestHandler(s.isGuestAccessEnabled)(handler),
	)
}

// handleHealthz handles the health check endpoint.

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "ok"})
}

// handleReadyz handles the readiness check endpoint.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "ready"})
}

// handleRotatePassword rotates the operator admin password.
// It generates a new random password, updates the K8s Secret, and refreshes
// the in-memory credential store so the new password takes effect immediately
// without requiring a pod restart.
func (s *Server) handleRotatePassword(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Verify the credential store is available for in-memory rotation.
	if s.credStore == nil {
		s.logger.Error("password rotation requested but credential store not available")
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"credential store not configured")
		return
	}

	// Generate a new cryptographically secure random password.
	newPassword, err := util.GenerateRandomPassword()
	if err != nil {
		s.logger.Error("failed to generate new password", "error", err)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to generate new password")
		return
	}

	// Determine the operator namespace.
	operatorNS := os.Getenv("POD_NAMESPACE")
	if operatorNS == "" {
		operatorNS = util.OperatorNamespace
	}

	// Read the existing K8s Secret.
	secretKey := types.NamespacedName{
		Name:      util.OperatorAdminPasswordSecretName,
		Namespace: operatorNS,
	}
	existing := &corev1.Secret{}
	getErr := s.k8sClient.Get(ctx, secretKey, existing)
	switch {
	case getErr == nil:
		// Secret exists — update it with the new password.
		existing.Data[util.PasswordSecretKey] = []byte(newPassword)
		if updateErr := s.k8sClient.Update(ctx, existing); updateErr != nil {
			s.logger.Error("failed to update admin password secret",
				"error", updateErr)
			writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
				"failed to update admin password secret")
			return
		}
	case apierrors.IsNotFound(getErr):
		// Secret genuinely does not exist — create it.
		if !s.createAdminPasswordSecret(ctx, w, operatorNS, newPassword) {
			return
		}
	default:
		// Transient API-server error: do NOT attempt Create (it would fail
		// with AlreadyExists and mask the real cause, mirroring
		// resolveAdminPassword's discrimination).
		s.logger.Error("failed to read admin password secret", "error", getErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to read admin password secret: %v", getErr))
		return
	}

	// Update the in-memory credential store so the new password works immediately.
	s.credStore.SetCredentials(adminUsername, newPassword, auth.PermissionAdmin)

	s.logger.Info("admin password rotated successfully",
		"secret", util.OperatorAdminPasswordSecretName, "namespace", operatorNS)

	if s.metrics != nil {
		s.metrics.RecordPasswordRotation()
	}

	writeJSON(w, http.StatusOK, map[string]string{
		responseKeyStatus:  statusRotated,
		responseKeyMessage: "Admin password rotated successfully",
	})
}

// createAdminPasswordSecret creates the admin password Secret with the given
// password, handling the AlreadyExists race (another replica created it
// concurrently) by updating the existing Secret instead. Returns false when an
// error response has been written.
func (s *Server) createAdminPasswordSecret(
	ctx context.Context,
	w http.ResponseWriter,
	operatorNS, newPassword string,
) bool {
	s.logger.Info("admin password secret not found, creating new secret",
		"secret", util.OperatorAdminPasswordSecretName, "namespace", operatorNS)
	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.OperatorAdminPasswordSecretName,
			Namespace: operatorNS,
			Labels: map[string]string{
				util.LabelManagedBy: util.LabelManagedByValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			util.PasswordSecretKey: []byte(newPassword),
		},
	}
	createErr := s.k8sClient.Create(ctx, newSecret)
	if createErr == nil {
		return true
	}
	if apierrors.IsAlreadyExists(createErr) {
		// Race: another replica created the Secret between Get and Create —
		// re-read and update it with our new password.
		raced := &corev1.Secret{}
		key := types.NamespacedName{
			Name:      util.OperatorAdminPasswordSecretName,
			Namespace: operatorNS,
		}
		if getErr := s.k8sClient.Get(ctx, key, raced); getErr == nil {
			if raced.Data == nil {
				raced.Data = map[string][]byte{}
			}
			raced.Data[util.PasswordSecretKey] = []byte(newPassword)
			if updateErr := s.k8sClient.Update(ctx, raced); updateErr == nil {
				return true
			}
		}
	}
	s.logger.Error("failed to create admin password secret", "error", createErr)
	writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
		"failed to create admin password secret")
	return false
}

// handleListClusters lists all CloudberryCluster resources.
func (s *Server) handleListClusters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterList := &cbv1alpha1.CloudberryClusterList{}
	if err := s.k8sClient.List(ctx, clusterList); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to list clusters")
		return
	}

	type clusterSummary struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Phase     string `json:"phase"`
		Version   string `json:"version"`
	}

	items := make([]clusterSummary, 0, len(clusterList.Items))
	for i := range clusterList.Items {
		items = append(items, clusterSummary{
			Name:      clusterList.Items[i].Name,
			Namespace: clusterList.Items[i].Namespace,
			Phase:     string(clusterList.Items[i].Status.Phase),
			Version:   clusterList.Items[i].Spec.Version,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyItems: items,
		responseKeyTotal: len(items),
	})
}

// handleGetCluster gets a specific CloudberryCluster resource.
func (s *Server) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}
	writeJSON(w, http.StatusOK, cluster)
}

// handleGetClusterStatus gets the status of a specific cluster.
func (s *Server) handleGetClusterStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyName:      cluster.Name,
		responseKeyNamespace: cluster.Namespace,
		"status":             cluster.Status,
	})
}

// handleGetScaleStatus returns the current scaling state of a cluster.
func (s *Server) handleGetScaleStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	scaling := cluster.Status.Phase == "Scaling"
	response := map[string]interface{}{
		responseKeyName:      cluster.Name,
		responseKeyNamespace: cluster.Namespace,
		"scaling":            scaling,
		"phase":              string(cluster.Status.Phase),
		"segmentsReady":      cluster.Status.SegmentsReady,
		"segmentsTotal":      cluster.Status.SegmentsTotal,
	}

	// Include DataRedistribution condition if present.
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == string(cbv1alpha1.ConditionDataRedistribution) {
			response["redistribution"] = map[string]interface{}{
				responseKeyStatus:  string(cond.Status),
				"reason":           cond.Reason,
				responseKeyMessage: cond.Message,
			}
			break
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// handleCreateCluster creates a new CloudberryCluster.
func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := json.NewDecoder(r.Body).Decode(cluster); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if !isValidDNS1123Name(cluster.Name) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"cluster name must be a valid DNS-1123 subdomain")
		return
	}

	if err := s.k8sClient.Create(r.Context(), cluster); err != nil {
		s.logger.Error("failed to create cluster", "error", err)
		s.recordClusterOperation("create", err)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to create cluster")
		return
	}
	s.recordClusterOperation("create", nil)

	writeJSON(w, http.StatusCreated, cluster)
}

// recordClusterOperation records a cluster CRUD attempt outcome on
// cloudberry_api_cluster_operations_total (nil-safe).
func (s *Server) recordClusterOperation(operation string, err error) {
	if s.metrics == nil {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}
	s.metrics.RecordAPIClusterOperation(operation, result)
}

// handleDeleteCluster deletes a CloudberryCluster.
func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if err := s.k8sClient.Delete(r.Context(), cluster); err != nil {
		s.logger.Error("failed to delete cluster", "cluster", name, "error", err)
		s.recordClusterOperation("delete", err)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to delete cluster")
		return
	}
	s.recordClusterOperation("delete", nil)

	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "deleting"})
}

// handleStartCluster starts a cluster.
func (s *Server) handleStartCluster(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "start")
}

// handleStopCluster stops a cluster.
func (s *Server) handleStopCluster(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "stop")
}

// handleRestartCluster restarts a cluster.
func (s *Server) handleRestartCluster(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "restart")
}

// handleReloadConfig reloads cluster configuration.
func (s *Server) handleReloadConfig(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "reload")
}

// handleGetConfig gets the cluster configuration.
func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}
	writeJSON(w, http.StatusOK, cluster.Spec.Config)
}

// handleUpdateConfig updates the cluster configuration.
func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var configUpdate cbv1alpha1.ConfigSpec
	if decodeErr := json.NewDecoder(r.Body).Decode(&configUpdate); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			latest.Spec.Config = &configUpdate
			return nil
		}); updateErr != nil {
		s.logger.Error("failed to update config", "cluster", name, "error", updateErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to update config: %v", updateErr))
		return
	}

	// Audit log: config change
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		s.logger.Info("config changed",
			"cluster", name,
			"username", identity.Username,
			"method", identity.AuthMethod,
			"source_ip", r.RemoteAddr,
		)
	}

	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: statusUpdated})
}

// handleListSegments lists cluster segments.
func (s *Server) handleListSegments(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"segmentsReady":  cluster.Status.SegmentsReady,
		"segmentsTotal":  cluster.Status.SegmentsTotal,
		"failedSegments": cluster.Status.FailedSegments,
	})
}

// handleGetMirroring gets the mirroring status.
func (s *Server) handleGetMirroring(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": cluster.Status.MirroringStatus,
	})
}

// handleListSessions lists active sessions by querying pg_stat_activity
// through a short-lived database connection created via the DBClientFactory.
//
// Supports optional query parameter filters:
//   - status: filter by session state ("running", "queued", "blocked", "idle")
//   - database: filter by database name (datname)
//   - user: filter by username (usename)
//   - resource_group: filter by resource group name
//   - since: filter by query_start within the last N minutes (e.g. "5m", "30m")
func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.sessions.list")
	defer span.End()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("session list requested but database factory not available",
			"cluster", cluster.Name)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"sessions":         []interface{}{},
			responseKeyTotal:   0,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, err := s.dbFactory.NewClient(ctx, cluster)
	if err != nil {
		s.logger.Error("failed to create database client for session listing",
			"cluster", cluster.Name, "error", err)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	// Use ListSessionsWithResourceGroup for richer data including resource group info.
	sessions, err := dbClient.ListSessionsWithResourceGroup(ctx)
	if err != nil {
		s.logger.Error("failed to list sessions",
			"cluster", cluster.Name, "error", err)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to list sessions")
		return
	}

	// Apply filters from query parameters.
	filtered := filterSessions(sessions, r.URL.Query())

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions":       filtered,
		responseKeyTotal: len(filtered),
	})
}

// filterSessions applies query parameter filters to a list of sessions.
// All filters are ANDed together. An empty filter value means no filtering on that field.
func filterSessions(sessions []db.SessionWithGroup, params url.Values) []db.SessionWithGroup {
	status := params.Get("status")
	database := params.Get("database")
	user := params.Get("user")
	resGroup := params.Get("resource_group")
	since := params.Get("since")

	var result []db.SessionWithGroup
	for _, sess := range sessions {
		if status != "" && !matchStatus(sess, status) {
			continue
		}
		if database != "" && sess.Database != database {
			continue
		}
		if user != "" && sess.Username != user {
			continue
		}
		if resGroup != "" && sess.ResourceGroup != resGroup {
			continue
		}
		if since != "" && !matchSince(sess, since) {
			continue
		}
		result = append(result, sess)
	}
	if result == nil {
		result = []db.SessionWithGroup{}
	}
	return result
}

// matchStatus checks whether a session matches the given status filter.
// Supported values: "running" (active), "queued" (idle in transaction),
// "blocked" (wait_event_type=Lock), "idle".
// Unknown status values are ignored (match all).
func matchStatus(s db.SessionWithGroup, status string) bool {
	switch status {
	case "running":
		return s.State == "active"
	case "queued":
		return s.State == "idle in transaction"
	case "blocked":
		return s.WaitEventType == "Lock"
	case "idle":
		return s.State == "idle"
	default:
		return true
	}
}

// matchSince checks whether a session's query started within the given duration.
// The since parameter should be a Go duration string (e.g. "5m", "30m", "1h").
// Returns true if the since value is invalid (no filtering on parse error).
func matchSince(s db.SessionWithGroup, since string) bool {
	d, err := time.ParseDuration(since)
	if err != nil {
		// Invalid duration format — do not filter.
		return true
	}
	cutoff := time.Now().Add(-d)
	return s.QueryStart.After(cutoff)
}

// backendSignalMode selects the backend signal sent by cancelBackendByPID:
// pg_cancel_backend ("cancel") or pg_terminate_backend ("terminate").
type backendSignalMode string

const (
	// backendSignalCancel cancels the running query (pg_cancel_backend).
	backendSignalCancel backendSignalMode = "cancel"
	// backendSignalTerminate terminates the whole session (pg_terminate_backend).
	backendSignalTerminate backendSignalMode = "terminate"
)

// cancelBackendOptions configures cancelBackendByPID per endpoint so the
// three cancel/terminate handlers share one implementation while preserving
// their exact response contracts.
type cancelBackendOptions struct {
	// mode selects cancel vs terminate.
	mode backendSignalMode
	// parseReason parses the optional {"reason": "..."} request body and
	// echoes it in the response (cancel endpoints only).
	parseReason bool
	// includeStatus adds "status": "canceled" to the success response
	// (queries-API cancel endpoint only).
	includeStatus bool
	// operation is the human-readable operation name used in log messages
	// and error bodies (e.g. "cancel query", "terminate session").
	operation string
	// resultKey is the response key carrying the boolean backend result
	// ("canceled" or "terminated").
	resultKey string
}

// backendSpanName returns the static child-span name for the backend-signal
// helper (bounded: two modes only).
func backendSpanName(mode backendSignalMode) string {
	if mode == backendSignalTerminate {
		return "api.session.terminate"
	}
	return "api.query.cancel"
}

// cancelBackendByPID is the shared implementation behind the query-cancel and
// session-terminate endpoints. It parses and validates the PID, resolves the
// cluster, opens a short-lived DB connection, sends the requested backend
// signal and writes the endpoint-specific response. Being the single call
// site, it also records the cancel/termination metrics for ALL cancel and
// terminate endpoints (C-4/M-16).
func (s *Server) cancelBackendByPID(w http.ResponseWriter, r *http.Request, opts cancelBackendOptions) {
	// Static span name per mode; the PID stays out of the name (cardinality).
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, backendSpanName(opts.mode))
	defer span.End()

	pidStr := r.PathValue("pid")
	pid, err := strconv.ParseInt(pidStr, 10, 32)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid PID")
		return
	}
	if pid <= 0 {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "PID must be a positive integer")
		return
	}

	// Parse optional reason from the request body when supported.
	var cancelReq struct {
		Reason string `json:"reason"`
	}
	if opts.parseReason {
		// Ignore decode errors — reason is optional.
		_ = json.NewDecoder(r.Body).Decode(&cancelReq)
	}

	name := r.PathValue("name")
	cluster, clusterErr := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if clusterErr != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn(opts.operation+" requested but database factory not available",
			"cluster", cluster.Name, responseKeyPID, pid)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyPID:     pid,
			opts.resultKey:     false,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for "+opts.operation,
			"cluster", cluster.Name, responseKeyPID, pid, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	var result bool
	var sigErr error
	if opts.mode == backendSignalTerminate {
		result, sigErr = dbClient.TerminateSession(ctx, int32(pid))
	} else {
		result, sigErr = dbClient.CancelQuery(ctx, int32(pid))
	}
	s.recordBackendSignal(cluster, opts.mode, sigErr)
	if sigErr != nil {
		telemetry.SetSpanError(span, sigErr)
		s.logger.Error("failed to "+opts.operation,
			"cluster", cluster.Name, responseKeyPID, pid, "error", sigErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to "+opts.operation)
		return
	}

	response := map[string]interface{}{
		responseKeyPID: pid,
		opts.resultKey: result,
	}
	if opts.includeStatus {
		response[responseKeyStatus] = statusCanceled
	}
	if opts.parseReason && cancelReq.Reason != "" {
		response["reason"] = cancelReq.Reason
		s.logger.Info(opts.operation+" requested with reason",
			"cluster", cluster.Name, responseKeyPID, pid,
			opts.resultKey, result, "reason", cancelReq.Reason)
	} else {
		s.logger.Info(opts.operation+" requested",
			"cluster", cluster.Name, responseKeyPID, pid, opts.resultKey, result)
	}

	writeJSON(w, http.StatusOK, response)
}

// recordBackendSignal records the cancel/terminate metrics from the single
// shared helper: query cancels (both the queries API and the sessions API)
// increment cloudberry_query_cancel_total on success; terminations increment
// cloudberry_session_terminations_total with a success/error result.
func (s *Server) recordBackendSignal(
	cluster *cbv1alpha1.CloudberryCluster,
	mode backendSignalMode,
	sigErr error,
) {
	if s.metrics == nil {
		return
	}
	if mode == backendSignalTerminate {
		result := resultSuccess
		if sigErr != nil {
			result = resultError
		}
		s.metrics.RecordSessionTermination(cluster.Name, cluster.Namespace, result)
		return
	}
	if sigErr == nil {
		s.metrics.RecordQueryCancel(cluster.Name, cluster.Namespace)
	}
}

// handleCancelQuery cancels a running query by calling pg_cancel_backend
// through a short-lived database connection created via the DBClientFactory.
// Accepts an optional JSON body with a "reason" field for audit logging.
func (s *Server) handleCancelQuery(w http.ResponseWriter, r *http.Request) {
	s.cancelBackendByPID(w, r, cancelBackendOptions{
		mode:        backendSignalCancel,
		parseReason: true,
		operation:   "cancel query",
		resultKey:   responseKeyCanceled,
	})
}

// handleTerminateSession terminates a session by calling pg_terminate_backend
// through a short-lived database connection created via the DBClientFactory.
func (s *Server) handleTerminateSession(w http.ResponseWriter, r *http.Request) {
	s.cancelBackendByPID(w, r, cancelBackendOptions{
		mode:      backendSignalTerminate,
		operation: "terminate session",
		resultKey: "terminated",
	})
}

// handleVacuum runs a vacuum operation.
func (s *Server) handleVacuum(w http.ResponseWriter, r *http.Request) {
	s.setMaintenanceAnnotation(w, r, "vacuum")
}

// handleAnalyze runs an analyze operation.
func (s *Server) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	s.setMaintenanceAnnotation(w, r, "analyze")
}

// handleReindex runs a reindex operation.
func (s *Server) handleReindex(w http.ResponseWriter, r *http.Request) {
	s.setMaintenanceAnnotation(w, r, "reindex")
}

// handleGetStandby gets the standby status.
func (s *Server) handleGetStandby(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"enabled": cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled,
		"ready":   cluster.Status.StandbyReady,
	})
}

// handleActivateStandby activates the standby coordinator.
func (s *Server) handleActivateStandby(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "activate-standby")
}

// handleStartRecovery starts a recovery operation.
func (s *Server) handleStartRecovery(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	var req struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	// Validate recovery type against known values.
	validRecoveryTypes := map[string]bool{
		util.RecoveryIncremental:  true,
		util.RecoveryFull:         true,
		util.RecoveryDifferential: true,
	}
	if !validRecoveryTypes[req.Type] {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid recovery type %q; valid types: incremental, full, differential", req.Type))
		return
	}

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Annotation-only mutation: use a MergeFrom patch (conflict-safe).
	if patchErr := s.patchClusterAnnotation(r.Context(), cluster,
		util.AnnotationRecovery, req.Type); patchErr != nil {
		s.logger.Error("failed to start recovery", "cluster", name, "error", patchErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to start recovery")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: "recovery started", "type": req.Type})
}

// handleRebalance starts a rebalance operation.
func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "rebalance")
}

// handleGetRebalanceStatus returns the rebalance status for a cluster.
func (s *Server) handleGetRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	response := map[string]interface{}{
		responseKeyName:      cluster.Name,
		responseKeyNamespace: cluster.Namespace,
	}

	// Include rebalance configuration if present.
	if cluster.Spec.Segments.Rebalance != nil {
		response["config"] = map[string]interface{}{
			"skewThreshold": cluster.Spec.Segments.Rebalance.SkewThreshold,
			"parallelism":   cluster.Spec.Segments.Rebalance.Parallelism,
			"excludeTables": cluster.Spec.Segments.Rebalance.ExcludeTables,
		}
	}

	// Include DataRedistribution condition if present.
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == string(cbv1alpha1.ConditionDataRedistribution) {
			response["redistribution"] = map[string]interface{}{
				responseKeyStatus:  string(cond.Status),
				"reason":           cond.Reason,
				responseKeyMessage: cond.Message,
				"lastTransition":   cond.LastTransitionTime.Time,
			}
			break
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// handleGetWorkload gets the workload management configuration.
func (s *Server) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if cluster.Spec.Workload == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled": false,
		})
		return
	}

	writeJSON(w, http.StatusOK, cluster.Spec.Workload)
}

// handleListResourceGroups lists resource groups. When a database connection is
// available via dbFactory, groups are queried from gp_toolkit.gp_resgroup_status.
// Otherwise, the CRD spec is used as a fallback.
func (s *Server) handleListResourceGroups(w http.ResponseWriter, r *http.Request) {
	// Static-named child span (D-6); downstream db spans nest under it.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.resourceGroups.list")
	defer span.End()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Try to list from the database when dbFactory is available.
	if s.dbFactory != nil {
		dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
		if dbErr == nil {
			defer dbClient.Close()
			dbGroups, listErr := dbClient.ListResourceGroups(ctx)
			if listErr == nil {
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"resourceGroups": dbGroups,
					responseKeyTotal: len(dbGroups),
				})
				return
			}
			s.logger.Warn("failed to list resource groups from database, falling back to CRD spec",
				"cluster", cluster.Name, "error", listErr)
		} else {
			s.logger.Warn("failed to create database client for resource group listing, falling back to CRD spec",
				"cluster", cluster.Name, "error", dbErr)
		}
	}

	// Fallback: read from CRD spec.
	var groups []interface{}
	if cluster.Spec.Workload != nil {
		for i := range cluster.Spec.Workload.ResourceGroups {
			groups = append(groups, cluster.Spec.Workload.ResourceGroups[i])
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"resourceGroups": groups,
		responseKeyTotal: len(groups),
	})
}

// handleCreateResourceGroup creates a new resource group in the database.
func (s *Server) handleCreateResourceGroup(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req struct {
		Name          string `json:"name"`
		Concurrency   int32  `json:"concurrency"`
		CPUMaxPercent int32  `json:"cpuMaxPercent"`
		CPUWeight     int32  `json:"cpuWeight"`
		MemoryLimit   int32  `json:"memoryLimit"`
		MinCost       int32  `json:"minCost"`
	}
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "resource group name is required")
		return
	}

	if !isValidIdentifier(req.Name) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource group name: must be a valid SQL identifier")
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("create resource group requested but database factory not available",
			"cluster", cluster.Name, "group", req.Name)
		writeJSON(w, http.StatusCreated, map[string]interface{}{
			responseKeyName:        req.Name,
			responseKeyConcurrency: req.Concurrency,
			responseKeyCPUMaxPct:   req.CPUMaxPercent,
			responseKeyCPUWeight:   req.CPUWeight,
			responseKeyMemoryLimit: req.MemoryLimit,
			responseKeyMessage:     "resource group creation pending; database connection not available",
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource group creation",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	opts := db.ResourceGroupOptions{
		Name:          req.Name,
		Concurrency:   req.Concurrency,
		CPUMaxPercent: req.CPUMaxPercent,
		CPUWeight:     req.CPUWeight,
		MemoryLimit:   req.MemoryLimit,
		MinCost:       req.MinCost,
	}
	if createErr := dbClient.CreateResourceGroup(r.Context(), opts); createErr != nil {
		s.logger.Error("failed to create resource group",
			"cluster", cluster.Name, "group", req.Name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to create resource group")
		return
	}

	s.logger.Info("resource group created",
		"cluster", cluster.Name, "group", req.Name)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		responseKeyName:        req.Name,
		responseKeyConcurrency: req.Concurrency,
		responseKeyCPUMaxPct:   req.CPUMaxPercent,
		responseKeyMemoryLimit: req.MemoryLimit,
		responseKeyStatus:      statusCreated,
	})
}

// handleDeleteResourceGroup deletes a resource group from the database.
func (s *Server) handleDeleteResourceGroup(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	groupName := r.PathValue("groupName")

	if !isValidIdentifier(groupName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource group name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("delete resource group requested but database factory not available",
			"cluster", cluster.Name, "group", groupName)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyGroup:   groupName,
			responseKeyStatus:  statusPending,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource group deletion",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	if dropErr := dbClient.DropResourceGroup(r.Context(), groupName); dropErr != nil {
		s.logger.Error("failed to drop resource group",
			"cluster", cluster.Name, responseKeyGroup, groupName, "error", dropErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to drop resource group")
		return
	}

	s.logger.Info("resource group dropped",
		"cluster", cluster.Name, responseKeyGroup, groupName)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyGroup:  groupName,
		responseKeyStatus: statusDeleted,
	})
}

// handleUpdateResourceGroup updates an existing resource group's parameters.
func (s *Server) handleUpdateResourceGroup(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	groupName := r.PathValue("groupName")

	if !isValidIdentifier(groupName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource group name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req struct {
		Concurrency   int32 `json:"concurrency"`
		CPUMaxPercent int32 `json:"cpuMaxPercent"`
		CPUWeight     int32 `json:"cpuWeight"`
		MemoryLimit   int32 `json:"memoryLimit"`
		MinCost       int32 `json:"minCost"`
	}
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("update resource group requested but database factory not available",
			"cluster", cluster.Name, responseKeyGroup, groupName)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyGroup:       groupName,
			responseKeyConcurrency: req.Concurrency,
			responseKeyCPUMaxPct:   req.CPUMaxPercent,
			responseKeyCPUWeight:   req.CPUWeight,
			responseKeyStatus:      statusPending,
			responseKeyMessage:     msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource group update",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	opts := db.ResourceGroupOptions{
		Name:          groupName,
		Concurrency:   req.Concurrency,
		CPUMaxPercent: req.CPUMaxPercent,
		CPUWeight:     req.CPUWeight,
		MemoryLimit:   req.MemoryLimit,
		MinCost:       req.MinCost,
	}
	if alterErr := dbClient.AlterResourceGroup(r.Context(), opts); alterErr != nil {
		s.logger.Error("failed to alter resource group",
			"cluster", cluster.Name, responseKeyGroup, groupName, "error", alterErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to alter resource group")
		return
	}

	s.logger.Info("resource group updated",
		"cluster", cluster.Name, responseKeyGroup, groupName)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyGroup:       groupName,
		responseKeyConcurrency: req.Concurrency,
		responseKeyCPUMaxPct:   req.CPUMaxPercent,
		responseKeyCPUWeight:   req.CPUWeight,
		responseKeyStatus:      statusUpdated,
	})
}

// handleAssignResourceGroup assigns a role to a resource group.
func (s *Server) handleAssignResourceGroup(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	groupName := r.PathValue("groupName")

	if !isValidIdentifier(groupName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource group name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req struct {
		Role string `json:"role"`
	}
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if req.Role == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "role is required")
		return
	}

	if !isValidIdentifier(req.Role) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid role name: must be a valid SQL identifier")
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("assign resource group requested but database factory not available",
			"cluster", cluster.Name, responseKeyGroup, groupName, "role", req.Role)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyGroup:   groupName,
			"role":             req.Role,
			responseKeyStatus:  statusPending,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource group assignment",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	if assignErr := dbClient.AssignRoleResourceGroup(r.Context(), req.Role, groupName); assignErr != nil {
		s.logger.Error("failed to assign role to resource group",
			"cluster", cluster.Name, responseKeyGroup, groupName, "role", req.Role, "error", assignErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to assign role to resource group")
		return
	}

	// Audit log: role management
	identity := auth.IdentityFromContext(r.Context())
	userName := ""
	authMethod := ""
	if identity != nil {
		userName = identity.Username
		authMethod = identity.AuthMethod
	}
	s.logger.Info("role assigned to resource group",
		"cluster", cluster.Name,
		responseKeyGroup, groupName,
		"role", req.Role,
		"username", userName,
		"method", authMethod,
		"source_ip", r.RemoteAddr,
	)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyGroup:  groupName,
		"role":            req.Role,
		responseKeyStatus: "assigned",
	})
}

// handleListResourceQueues lists resource queues from the database.
func (s *Server) handleListResourceQueues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("list resource queues requested but database factory not available",
			"cluster", cluster.Name)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"resourceQueues":   []interface{}{},
			responseKeyTotal:   0,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource queue listing",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	queues, listErr := dbClient.ListResourceQueues(r.Context())
	if listErr != nil {
		s.logger.Error("failed to list resource queues",
			"cluster", cluster.Name, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to list resource queues")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"resourceQueues": queues,
		responseKeyTotal: len(queues),
	})
}

// handleCreateResourceQueue creates a new resource queue in the database.
func (s *Server) handleCreateResourceQueue(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req struct {
		Name             string  `json:"name"`
		ActiveStatements int32   `json:"activeStatements"`
		MemoryLimit      string  `json:"memoryLimit"`
		Priority         string  `json:"priority"`
		MaxCost          float64 `json:"maxCost"`
		MinCost          float64 `json:"minCost"`
	}
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "resource queue name is required")
		return
	}

	if !isValidIdentifier(req.Name) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource queue name: must be a valid SQL identifier")
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("create resource queue requested but database factory not available",
			"cluster", cluster.Name, "queue", req.Name)
		writeJSON(w, http.StatusCreated, map[string]interface{}{
			responseKeyName:        req.Name,
			"activeStatements":     req.ActiveStatements,
			responseKeyMemoryLimit: req.MemoryLimit,
			"priority":             req.Priority,
			responseKeyMessage:     "resource queue creation pending; database connection not available",
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource queue creation",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	opts := db.ResourceQueueOptions{
		Name:             req.Name,
		ActiveStatements: req.ActiveStatements,
		MemoryLimit:      req.MemoryLimit,
		Priority:         req.Priority,
		MaxCost:          req.MaxCost,
		MinCost:          req.MinCost,
	}
	if createErr := dbClient.CreateResourceQueue(r.Context(), opts); createErr != nil {
		s.logger.Error("failed to create resource queue",
			"cluster", cluster.Name, "queue", req.Name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to create resource queue")
		return
	}

	s.logger.Info("resource queue created",
		"cluster", cluster.Name, "queue", req.Name)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		responseKeyName:        req.Name,
		"activeStatements":     req.ActiveStatements,
		responseKeyMemoryLimit: req.MemoryLimit,
		"priority":             req.Priority,
		responseKeyStatus:      statusCreated,
	})
}

// handleDeleteResourceQueue deletes a resource queue from the database.
func (s *Server) handleDeleteResourceQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	queueName := r.PathValue("queueName")

	if !isValidIdentifier(queueName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid resource queue name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("delete resource queue requested but database factory not available",
			"cluster", cluster.Name, "queue", queueName)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyName:    queueName,
			responseKeyStatus:  statusPending,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for resource queue deletion",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	if dropErr := dbClient.DropResourceQueue(r.Context(), queueName); dropErr != nil {
		s.logger.Error("failed to drop resource queue",
			"cluster", cluster.Name, "queue", queueName, "error", dropErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to drop resource queue")
		return
	}

	s.logger.Info("resource queue dropped",
		"cluster", cluster.Name, "queue", queueName)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyName:   queueName,
		responseKeyStatus: statusDeleted,
	})
}

// handleListWorkloadRules lists workload rules.
func (s *Server) handleListWorkloadRules(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var rules []interface{}
	if cluster.Spec.Workload != nil {
		for i := range cluster.Spec.Workload.Rules {
			rules = append(rules, cluster.Spec.Workload.Rules[i])
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"rules":          rules,
		responseKeyTotal: len(rules),
	})
}

// handleCreateWorkloadRule creates a new workload rule by patching the cluster CRD.
func (s *Server) handleCreateWorkloadRule(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var rule cbv1alpha1.WorkloadRule
	if decodeErr := json.NewDecoder(r.Body).Decode(&rule); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if rule.Name == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "rule name is required")
		return
	}

	if !isValidIdentifier(rule.Name) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid rule name: must be a valid SQL identifier")
		return
	}

	// Check for duplicate rule name on the fetched object for a fast 400.
	if cluster.Spec.Workload != nil {
		for i := range cluster.Spec.Workload.Rules {
			if cluster.Spec.Workload.Rules[i].Name == rule.Name {
				writeErrorJSON(w, http.StatusBadRequest, "DUPLICATE_RULE",
					fmt.Sprintf("workload rule %q already exists", rule.Name))
				return
			}
		}
	}

	// Spec mutation: re-apply on a fresh object inside conflict retry.
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.Workload == nil {
				latest.Spec.Workload = &cbv1alpha1.WorkloadSpec{}
			}
			for i := range latest.Spec.Workload.Rules {
				if latest.Spec.Workload.Rules[i].Name == rule.Name {
					return errDuplicateWorkloadRule
				}
			}
			latest.Spec.Workload.Rules = append(latest.Spec.Workload.Rules, rule)
			return nil
		})
	if errors.Is(updateErr, errDuplicateWorkloadRule) {
		writeErrorJSON(w, http.StatusBadRequest, "DUPLICATE_RULE",
			fmt.Sprintf("workload rule %q already exists", rule.Name))
		return
	}
	if updateErr != nil {
		s.logger.Error("failed to create workload rule",
			"cluster", name, "rule", rule.Name, "error", updateErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to create workload rule: %v", updateErr))
		return
	}

	s.logger.Info("workload rule created", "cluster", name, responseKeyRule, rule.Name)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		responseKeyRule:   rule,
		responseKeyStatus: statusCreated,
	})
}

// applyWorkloadRuleUpdates applies partial updates from a JSON map to a WorkloadRule.
func applyWorkloadRuleUpdates(rule *cbv1alpha1.WorkloadRule, updates map[string]interface{}) {
	if v, ok := updates["enabled"]; ok {
		if b, isBool := v.(bool); isBool {
			rule.Enabled = b
		}
	}
	applyStringField(updates, "resourceGroup", &rule.ResourceGroup)
	applyStringField(updates, "action", &rule.Action)
	applyStringField(updates, "threshold", &rule.Threshold)
	applyStringField(updates, "thresholdType", &rule.ThresholdType)
	applyStringField(updates, "moveTarget", &rule.MoveTarget)
	applyStringField(updates, "queryTag", &rule.QueryTag)
	if v, ok := updates["priority"]; ok {
		if f, isFloat := v.(float64); isFloat {
			rule.Priority = int32(f)
		}
	}
}

// applyStringField applies a string field update from a JSON map.
func applyStringField(updates map[string]interface{}, key string, target *string) {
	if v, ok := updates[key]; ok {
		if s, isStr := v.(string); isStr {
			*target = s
		}
	}
}

// handleUpdateWorkloadRule updates an existing workload rule by patching the cluster CRD.
func (s *Server) handleUpdateWorkloadRule(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	ruleName := r.PathValue("ruleName")

	if !isValidIdentifier(ruleName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid rule name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var updates map[string]interface{}
	if decodeErr := json.NewDecoder(r.Body).Decode(&updates); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if cluster.Spec.Workload == nil {
		writeErrorJSON(w, http.StatusNotFound, "RULE_NOT_FOUND",
			fmt.Sprintf("workload rule %q not found", ruleName))
		return
	}

	ruleIdx := -1
	for i := range cluster.Spec.Workload.Rules {
		if cluster.Spec.Workload.Rules[i].Name == ruleName {
			ruleIdx = i
			break
		}
	}

	if ruleIdx < 0 {
		writeErrorJSON(w, http.StatusNotFound, "RULE_NOT_FOUND",
			fmt.Sprintf("workload rule %q not found", ruleName))
		return
	}

	// Spec mutation: re-apply the partial update on a fresh object inside
	// conflict retry so concurrent controller updates do not surface as 500.
	var updated cbv1alpha1.WorkloadRule
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.Workload == nil {
				return errWorkloadRuleNotFound
			}
			for i := range latest.Spec.Workload.Rules {
				if latest.Spec.Workload.Rules[i].Name == ruleName {
					applyWorkloadRuleUpdates(&latest.Spec.Workload.Rules[i], updates)
					updated = latest.Spec.Workload.Rules[i]
					return nil
				}
			}
			return errWorkloadRuleNotFound
		})
	if errors.Is(updateErr, errWorkloadRuleNotFound) {
		writeErrorJSON(w, http.StatusNotFound, "RULE_NOT_FOUND",
			fmt.Sprintf("workload rule %q not found", ruleName))
		return
	}
	if updateErr != nil {
		s.logger.Error("failed to update workload rule",
			"cluster", name, "rule", ruleName, "error", updateErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to update workload rule: %v", updateErr))
		return
	}

	s.logger.Info("workload rule updated", "cluster", name, responseKeyRule, ruleName)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyRule:   updated,
		responseKeyStatus: statusUpdated,
	})
}

// handleDeleteWorkloadRule deletes a workload rule by patching the cluster CRD.
func (s *Server) handleDeleteWorkloadRule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ruleName := r.PathValue("ruleName")

	if !isValidIdentifier(ruleName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"invalid rule name: must be a valid SQL identifier")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if cluster.Spec.Workload == nil {
		writeErrorJSON(w, http.StatusNotFound, "RULE_NOT_FOUND",
			fmt.Sprintf("workload rule %q not found", ruleName))
		return
	}

	ruleIdx := -1
	for i := range cluster.Spec.Workload.Rules {
		if cluster.Spec.Workload.Rules[i].Name == ruleName {
			ruleIdx = i
			break
		}
	}

	if ruleIdx < 0 {
		writeErrorJSON(w, http.StatusNotFound, "RULE_NOT_FOUND",
			fmt.Sprintf("workload rule %q not found", ruleName))
		return
	}

	// Spec mutation: remove the rule from a fresh object inside conflict
	// retry. A rule that disappeared concurrently is treated as deleted
	// (idempotent delete).
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.Workload == nil {
				return nil
			}
			for i := range latest.Spec.Workload.Rules {
				if latest.Spec.Workload.Rules[i].Name == ruleName {
					latest.Spec.Workload.Rules = append(
						latest.Spec.Workload.Rules[:i],
						latest.Spec.Workload.Rules[i+1:]...,
					)
					return nil
				}
			}
			return nil
		})
	if updateErr != nil {
		s.logger.Error("failed to delete workload rule",
			"cluster", name, "rule", ruleName, "error", updateErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to delete workload rule: %v", updateErr))
		return
	}

	s.logger.Info("workload rule deleted", "cluster", name, responseKeyRule, ruleName)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyRule:   ruleName,
		responseKeyStatus: statusDeleted,
	})
}

// monitoringDisabledResponse is the standard message returned when query monitoring
// is not enabled for a cluster.
const monitoringDisabledMessage = "query monitoring is not enabled for this cluster"

// isMonitoringEnabled checks if query monitoring is enabled for the given cluster.
// Returns true if monitoring is enabled, false otherwise.
func isMonitoringEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.QueryMonitoring != nil && cluster.Spec.QueryMonitoring.Enabled
}

// writeMonitoringDisabled writes a standard HTTP 200 response indicating that
// query monitoring is disabled for the cluster, and records the access metric.
func (s *Server) writeMonitoringDisabled(w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster) {
	if s.metrics != nil {
		s.metrics.RecordMonitoringDisabledAccess(cluster.Name, cluster.Namespace)
	}
	s.logger.Debug("monitoring disabled access attempt",
		"cluster", cluster.Name, "namespace", cluster.Namespace)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"monitoringEnabled": false,
		responseKeyMessage:  monitoringDisabledMessage,
	})
}

// handleGetQueryMonitoring gets the query monitoring configuration and status.
// When the monitor is paused, returns the cached snapshot with stale flag.
func (s *Server) handleGetQueryMonitoring(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	// Check if monitor is paused — return cached snapshot if so.
	if state, paused := s.isMonitorPaused(cluster.Namespace, cluster.Name); paused {
		response := make(map[string]interface{})
		for k, v := range state.Snapshot {
			response[k] = v
		}
		response["stale"] = true
		if state.PausedAt != nil {
			response["pausedAt"] = state.PausedAt.Format(time.RFC3339)
		}
		writeJSON(w, http.StatusOK, response)
		return
	}

	response := map[string]interface{}{
		responseKeyActiveQueries:  cluster.Status.ActiveQueries,
		responseKeyQueuedQueries:  cluster.Status.QueuedQueries,
		responseKeyBlockedQueries: cluster.Status.BlockedQueries,
	}

	if cluster.Spec.QueryMonitoring != nil {
		response["config"] = cluster.Spec.QueryMonitoring
	}

	writeJSON(w, http.StatusOK, response)
}

// handleGetActiveQueries gets the active query counts.
// When the monitor is paused, returns the cached snapshot with stale flag.
func (s *Server) handleGetActiveQueries(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	// Check if monitor is paused — return cached snapshot if so.
	if state, paused := s.isMonitorPaused(cluster.Namespace, cluster.Name); paused {
		response := map[string]interface{}{
			responseKeyActiveQueries:  state.Snapshot[responseKeyActiveQueries],
			responseKeyQueuedQueries:  state.Snapshot[responseKeyQueuedQueries],
			responseKeyBlockedQueries: state.Snapshot[responseKeyBlockedQueries],
			"stale":                   true,
		}
		if state.PausedAt != nil {
			response["pausedAt"] = state.PausedAt.Format(time.RFC3339)
		}
		writeJSON(w, http.StatusOK, response)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyActiveQueries:  cluster.Status.ActiveQueries,
		responseKeyQueuedQueries:  cluster.Status.QueuedQueries,
		responseKeyBlockedQueries: cluster.Status.BlockedQueries,
	})
}

// monitorStateKey builds the map key for monitor state lookups.
func monitorStateKey(namespace, cluster string) string {
	return namespace + "/" + cluster
}

// isMonitorPaused checks whether the query monitor is paused for the given cluster.
// Returns the monitor state and true if paused, nil and false otherwise.
func (s *Server) isMonitorPaused(namespace, cluster string) (*monitorState, bool) {
	s.monitorMu.RLock()
	defer s.monitorMu.RUnlock()
	key := monitorStateKey(namespace, cluster)
	state, ok := s.monitorStates[key]
	return state, ok && state.Paused
}

// handlePauseMonitor pauses the query monitor for a cluster.
// It takes a snapshot of the current query data and stores it in memory.
// Subsequent requests to the query monitoring endpoints will return the
// cached snapshot with a stale flag until the monitor is resumed.
func (s *Server) handlePauseMonitor(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	key := monitorStateKey(cluster.Namespace, cluster.Name)

	// Check if already paused.
	s.monitorMu.RLock()
	if existing, ok := s.monitorStates[key]; ok && existing.Paused {
		s.monitorMu.RUnlock()
		s.logger.Info("monitor already paused",
			"cluster", cluster.Name, "namespace", cluster.Namespace)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyStatus:  statusPaused,
			"pausedAt":         existing.PausedAt,
			responseKeyMessage: "Query monitor is already paused",
		})
		return
	}
	s.monitorMu.RUnlock()

	// Take a snapshot of the current query data.
	snapshot := map[string]interface{}{
		responseKeyActiveQueries:  cluster.Status.ActiveQueries,
		responseKeyQueuedQueries:  cluster.Status.QueuedQueries,
		responseKeyBlockedQueries: cluster.Status.BlockedQueries,
	}
	if cluster.Spec.QueryMonitoring != nil {
		snapshot["config"] = cluster.Spec.QueryMonitoring
	}

	now := time.Now().UTC()
	s.monitorMu.Lock()
	s.monitorStates[key] = &monitorState{
		Paused:   true,
		PausedAt: &now,
		Snapshot: snapshot,
	}
	s.monitorMu.Unlock()

	// Record metric.
	if s.metrics != nil {
		s.metrics.RecordMonitorPause(cluster.Name, cluster.Namespace)
	}

	// Audit log.
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		s.logger.Info("query monitor paused",
			"cluster", cluster.Name, "namespace", cluster.Namespace,
			"username", identity.Username, "source_ip", r.RemoteAddr)
	} else {
		s.logger.Info("query monitor paused",
			"cluster", cluster.Name, "namespace", cluster.Namespace)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusPaused,
		"pausedAt":         now.Format(time.RFC3339),
		responseKeyMessage: "Query monitor paused",
	})
}

// handleResumeMonitor resumes the query monitor for a cluster.
// It removes the cached snapshot so subsequent requests return fresh data.
func (s *Server) handleResumeMonitor(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	key := monitorStateKey(cluster.Namespace, cluster.Name)

	s.monitorMu.Lock()
	_, wasPaused := s.monitorStates[key]
	delete(s.monitorStates, key)
	s.monitorMu.Unlock()

	// Record metric only if it was actually paused.
	if wasPaused {
		if s.metrics != nil {
			s.metrics.RecordMonitorResume(cluster.Name, cluster.Namespace)
		}
	}

	// Audit log.
	if identity := auth.IdentityFromContext(r.Context()); identity != nil {
		s.logger.Info("query monitor resumed",
			"cluster", cluster.Name, "namespace", cluster.Namespace,
			"username", identity.Username, "source_ip", r.RemoteAddr)
	} else {
		s.logger.Info("query monitor resumed",
			"cluster", cluster.Name, "namespace", cluster.Namespace)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusResumed,
		responseKeyMessage: "Query monitor resumed",
	})
}

// handleGetMonitorState returns the current pause/resume state of the query monitor.
func (s *Server) handleGetMonitorState(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	state, paused := s.isMonitorPaused(cluster.Namespace, cluster.Name)
	response := map[string]interface{}{
		"paused": paused,
		"stale":  paused,
	}
	if paused && state != nil && state.PausedAt != nil {
		response["pausedAt"] = state.PausedAt.Format(time.RFC3339)
	}

	writeJSON(w, http.StatusOK, response)
}

// handleGetQueryDetail returns detailed execution information for a specific query by PID.
// It queries pg_stat_activity, pg_locks, and pg_stat_user_tables through a short-lived
// database connection created via the DBClientFactory.
func (s *Server) handleGetQueryDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pidStr := r.PathValue("pid")

	pid, err := strconv.ParseInt(pidStr, 10, 32)
	if err != nil || pid <= 0 {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid PID")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_NOT_AVAILABLE",
			"database connection not configured")
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for query detail",
			"cluster", cluster.Name, responseKeyPID, pid, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_CONNECTION_FAILED",
			"failed to connect to database")
		return
	}
	defer dbClient.Close()

	detail, detailErr := dbClient.GetQueryDetail(r.Context(), int32(pid))
	if detailErr != nil {
		s.logger.Warn("query detail not found",
			"cluster", cluster.Name, responseKeyPID, pid, "error", detailErr)
		writeErrorJSON(w, http.StatusNotFound, "QUERY_NOT_FOUND",
			fmt.Sprintf("query with PID %d not found", pid))
		return
	}

	writeJSON(w, http.StatusOK, detail)
}

// handleCancelQueryByPID cancels a running query by PID via the queries API.
// This is the query-monitoring-specific cancel endpoint (POST /queries/{pid}/cancel)
// that records query cancel metrics, distinct from the session cancel endpoint.
// It shares the cancelBackendByPID implementation with the session endpoints.
func (s *Server) handleCancelQueryByPID(w http.ResponseWriter, r *http.Request) {
	s.cancelBackendByPID(w, r, cancelBackendOptions{
		mode:          backendSignalCancel,
		parseReason:   true,
		includeStatus: true,
		operation:     "cancel query",
		resultKey:     responseKeyCanceled,
	})
}

// handleMoveQuery moves a running query to a different resource group.
// It parses the target group from the request body and calls MoveQueryToResourceGroup.
func (s *Server) handleMoveQuery(w http.ResponseWriter, r *http.Request) {
	pidStr := r.PathValue("pid")
	pid, err := strconv.ParseInt(pidStr, 10, 32)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid PID")
		return
	}
	if pid <= 0 {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "PID must be a positive integer")
		return
	}

	// Parse request body for targetGroup.
	var moveReq struct {
		TargetGroup string `json:"targetGroup"`
	}
	if r.Body == nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "request body is required")
		return
	}
	if decErr := json.NewDecoder(r.Body).Decode(&moveReq); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}
	if moveReq.TargetGroup == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "targetGroup is required")
		return
	}
	if !isValidIdentifier(moveReq.TargetGroup) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"targetGroup must be a valid SQL identifier (alphanumeric and underscores, max 63 chars)")
		return
	}

	name := r.PathValue("name")
	cluster, clusterErr := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if clusterErr != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("move query requested but database factory not available",
			"cluster", cluster.Name, responseKeyPID, pid)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyPID:     pid,
			responseKeyStatus:  statusPending,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for move query",
			"cluster", cluster.Name, responseKeyPID, pid, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	if moveErr := dbClient.MoveQueryToResourceGroup(r.Context(), int32(pid), moveReq.TargetGroup); moveErr != nil {
		s.logger.Error("failed to move query to resource group",
			"cluster", cluster.Name, responseKeyPID, pid,
			"targetGroup", moveReq.TargetGroup, "error", moveErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			fmt.Sprintf("failed to move query: %s", moveErr.Error()))
		return
	}

	// Record query move metric.
	if s.metrics != nil {
		s.metrics.RecordQueryMove(cluster.Name, cluster.Namespace)
	}

	s.logger.Info("query moved to resource group",
		"cluster", cluster.Name, responseKeyPID, pid,
		"targetGroup", moveReq.TargetGroup)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyPID:    pid,
		"targetGroup":     moveReq.TargetGroup,
		responseKeyStatus: statusMoved,
	})
}

// ExporterStatus represents the health status of a monitoring exporter.
type ExporterStatus struct {
	Name           string `json:"name"`
	Port           int32  `json:"port"`
	Status         string `json:"status"` // "up", "down", "unknown"
	ContainerReady bool   `json:"containerReady"`
	Endpoint       string `json:"endpoint"`
	LastCheck      string `json:"lastCheck"`
}

// handleGetExporterHealth returns the health status of configured monitoring exporters.
// It inspects the coordinator pod's container statuses via the K8s API.
func (s *Server) handleGetExporterHealth(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	// Record exporter health check metric.
	if s.metrics != nil {
		s.metrics.RecordExporterHealthCheck(cluster.Name, cluster.Namespace)
	}

	// Check if query monitoring is configured with exporters.
	if cluster.Spec.QueryMonitoring == nil || cluster.Spec.QueryMonitoring.Exporters == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"exporters":        []ExporterStatus{},
			responseKeyTotal:   0,
			responseKeyMessage: "query monitoring exporters not configured",
		})
		return
	}

	exporters := cluster.Spec.QueryMonitoring.Exporters
	now := time.Now().UTC().Format(time.RFC3339)

	// List coordinator pods.
	podList := &corev1.PodList{}
	if listErr := s.k8sClient.List(r.Context(), podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			util.LabelCluster:   cluster.Name,
			util.LabelComponent: "coordinator",
		},
	); listErr != nil {
		s.logger.Error("failed to list coordinator pods for exporter health",
			"cluster", name, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to list coordinator pods")
		return
	}

	// Build a map of container name -> ready status from coordinator pods.
	containerReady := make(map[string]bool)
	for i := range podList.Items {
		pod := &podList.Items[i]
		for j := range pod.Status.ContainerStatuses {
			cs := &pod.Status.ContainerStatuses[j]
			containerReady[cs.Name] = cs.Ready
		}
	}

	hasPods := len(podList.Items) > 0
	statuses := buildExporterStatuses(exporters, containerReady, hasPods, now)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"exporters":      statuses,
		responseKeyTotal: len(statuses),
	})
}

// exporterEntry defines a candidate exporter to check.
type exporterEntry struct {
	name          string
	containerName string
	defaultPort   int32
	spec          *cbv1alpha1.ExporterSpec
}

// buildExporterStatuses builds the list of ExporterStatus from the CRD exporter config
// and the container readiness map from coordinator pods.
func buildExporterStatuses(
	exporters *cbv1alpha1.QueryMonitoringExportersSpec,
	containerReady map[string]bool,
	hasPods bool,
	now string,
) []ExporterStatus {
	candidates := []exporterEntry{
		{"postgres-exporter", "postgres-exporter", 9187, exporters.PostgresExporter},
		{"cloudberry-query-exporter", "cloudberry-query-exporter", 9188,
			exporters.CloudberryQueryExporter},
		{"node-exporter", "node-exporter", 9100, exporters.NodeExporter},
	}

	var statuses []ExporterStatus
	for _, c := range candidates {
		if c.spec == nil || !c.spec.Enabled {
			continue
		}
		port := c.spec.Port
		if port == 0 {
			port = c.defaultPort
		}
		ready := containerReady[c.containerName]
		statuses = append(statuses, ExporterStatus{
			Name:           c.name,
			Port:           port,
			Status:         exporterStatusFromReady(ready, hasPods),
			ContainerReady: ready,
			Endpoint:       fmt.Sprintf(":%d/metrics", port),
			LastCheck:      now,
		})
	}
	return statuses
}

// exporterStatusFromReady derives the exporter status string from container readiness.
// Returns "up" if the container is ready, "down" if the pod exists but container is not ready,
// or "unknown" if no coordinator pod was found.
func exporterStatusFromReady(ready, podExists bool) string {
	if !podExists {
		return "unknown"
	}
	if ready {
		return "up"
	}
	return "down"
}

// handleGetQueryHistory returns paginated query history with optional filters.
func (s *Server) handleGetQueryHistory(w http.ResponseWriter, r *http.Request) {
	// Static-named child span (D-6); downstream db spans nest under it.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.queryHistory.search")
	defer span.End()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	if s.dbFactory == nil {
		s.logger.Warn("query history requested but database factory not available",
			"cluster", cluster.Name)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyItems:   []interface{}{},
			responseKeyTotal:   0,
			"limit":            defaultPageSize,
			"offset":           0,
			responseKeyMessage: msgDBNotAvailable,
		})
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for query history",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE",
			"cannot connect to database")
		return
	}
	defer dbClient.Close()

	// Parse query parameters into filter.
	filter, parseErr := parseQueryHistoryFilter(r)
	if parseErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, parseErr.Error())
		return
	}

	start := time.Now()
	entries, total, queryErr := dbClient.GetQueryHistory(ctx, filter)
	if queryErr != nil {
		s.logger.Error("failed to get query history",
			"cluster", cluster.Name, "error", queryErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to get query history")
		return
	}

	// Record search duration metric.
	if s.metrics != nil {
		s.metrics.ObserveQueryHistorySearchDuration(cluster.Name, cluster.Namespace, time.Since(start))
	}

	// Normalize limit/offset for response.
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	if entries == nil {
		entries = []db.QueryHistoryEntry{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyItems: entries,
		responseKeyTotal: total,
		"limit":          limit,
		"offset":         offset,
	})
}

// handleGetQueryHistoryDetail returns detailed information for a specific historical query.
func (s *Server) handleGetQueryHistoryDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	qid := r.PathValue("qid")

	if qid == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "query ID is required")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	if s.dbFactory == nil {
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_NOT_AVAILABLE",
			"database connection not configured")
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for query history detail",
			"cluster", cluster.Name, "queryId", qid, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_CONNECTION_FAILED",
			"failed to connect to database")
		return
	}
	defer dbClient.Close()

	entry, detailErr := dbClient.GetQueryHistoryDetail(r.Context(), qid)
	if detailErr != nil {
		s.logger.Warn("query history detail not found",
			"cluster", cluster.Name, "queryId", qid, "error", detailErr)
		writeErrorJSON(w, http.StatusNotFound, "QUERY_NOT_FOUND",
			fmt.Sprintf("historical query %q not found", qid))
		return
	}

	writeJSON(w, http.StatusOK, entry)
}

// handleExportQueryHistory exports query history as CSV.
func (s *Server) handleExportQueryHistory(w http.ResponseWriter, r *http.Request) {
	// Static-named child span (D-6); downstream db spans nest under it.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.queryHistory.export")
	defer span.End()

	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Check if monitoring is disabled for this cluster.
	if !isMonitoringEnabled(cluster) {
		s.writeMonitoringDisabled(w, cluster)
		return
	}

	if s.dbFactory == nil {
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_NOT_AVAILABLE",
			"database connection not configured")
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for query history export",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_CONNECTION_FAILED",
			"failed to connect to database")
		return
	}
	defer dbClient.Close()

	// Parse filter from request body (optional JSON).
	filter := parseQueryHistoryExportFilter(r)

	// Large CSV exports can exceed the global WriteTimeout; clear the write
	// deadline for this response (request context still bounds the export).
	s.clearWriteDeadline(w)

	// Set CSV response headers.
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="query-history.csv"`)
	w.WriteHeader(http.StatusOK)

	if exportErr := dbClient.ExportQueryHistoryCSV(ctx, filter, w); exportErr != nil {
		s.logger.Error("failed to export query history CSV",
			"cluster", cluster.Name, "error", exportErr)
		// Headers already sent, cannot write error JSON.
		return
	}

	// Record export metric.
	if s.metrics != nil {
		s.metrics.RecordQueryHistoryExport(cluster.Name, cluster.Namespace, "csv")
	}

	s.logger.Info("query history CSV exported", "cluster", cluster.Name)
}

// parseQueryHistoryExportFilter parses an optional JSON filter from the request
// body for query history CSV export. A missing or invalid body yields a
// zero-value filter (export all).
func parseQueryHistoryExportFilter(r *http.Request) db.QueryHistoryFilter {
	filter := db.QueryHistoryFilter{}
	if r.ContentLength <= 0 {
		return filter
	}

	var req struct {
		Pattern       string `json:"pattern"`
		PatternType   string `json:"patternType"`
		User          string `json:"user"`
		Database      string `json:"database"`
		ResourceGroup string `json:"resourceGroup"`
		State         string `json:"state"`
		Since         string `json:"since"`
		Until         string `json:"until"`
	}
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		return filter
	}

	filter.Pattern = req.Pattern
	filter.PatternType = req.PatternType
	filter.Username = req.User
	filter.Database = req.Database
	filter.ResourceGroup = req.ResourceGroup
	filter.State = req.State
	if req.Since != "" {
		filter.Since = parseSinceTime(req.Since)
	}
	if req.Until != "" {
		if t, parseErr := time.Parse(time.RFC3339, req.Until); parseErr == nil {
			filter.Until = t
		}
	}

	return filter
}

// handleExportActiveQueries exports active queries as CSV.
// It queries pg_stat_activity through a short-lived database connection
// and writes the results as CSV with headers: pid, username, database,
// state, query, duration, wait_event_type, resource_group.
func (s *Server) handleExportActiveQueries(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if s.dbFactory == nil {
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_NOT_AVAILABLE",
			"database connection not configured")
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(r.Context(), cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for active query export",
			"cluster", cluster.Name, "error", dbErr)
		writeErrorJSON(w, http.StatusServiceUnavailable, "DB_CONNECTION_FAILED",
			"failed to connect to database")
		return
	}
	defer dbClient.Close()

	sessions, listErr := dbClient.ListSessionsWithResourceGroup(r.Context())
	if listErr != nil {
		s.logger.Error("failed to list sessions for active query export",
			"cluster", cluster.Name, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to list sessions")
		return
	}

	// Large CSV exports can exceed the global WriteTimeout; clear the write
	// deadline for this response (request context still bounds the export).
	s.clearWriteDeadline(w)

	// Set CSV response headers.
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="active-queries.csv"`)
	w.WriteHeader(http.StatusOK)

	// Write CSV header.
	csvHeader := "pid,username,database,state,query,duration,wait_event_type,resource_group\n"
	if _, writeErr := w.Write([]byte(csvHeader)); writeErr != nil {
		s.logger.Error("failed to write CSV header for active query export",
			"cluster", cluster.Name, "error", writeErr)
		return
	}

	// Write CSV rows.
	for _, sess := range sessions {
		row := fmt.Sprintf("%d,%s,%s,%s,%s,%s,%s,%s\n",
			sess.PID,
			csvEscape(sess.Username),
			csvEscape(sess.Database),
			csvEscape(sess.State),
			csvEscape(sess.Query),
			csvEscape(sess.Duration),
			csvEscape(sess.WaitEventType),
			csvEscape(sess.ResourceGroup),
		)
		if _, writeErr := w.Write([]byte(row)); writeErr != nil {
			s.logger.Error("failed to write CSV row for active query export",
				"cluster", cluster.Name, "error", writeErr)
			return
		}
	}

	// Record export metric.
	if s.metrics != nil {
		s.metrics.RecordActiveQueryExport()
	}

	s.logger.Info("active queries CSV exported",
		"cluster", cluster.Name, "count", len(sessions))
}

// csvEscape escapes a string for safe inclusion in a CSV field.
// It wraps the value in double quotes if it contains commas, double quotes,
// or newlines, and doubles any internal double quotes per RFC 4180.
//
// Formula-injection hardening (L-8): cells beginning with '=', '+', '-' or
// '@' are prefixed with a single quote so spreadsheet applications render
// them as text instead of executing them as formulas (CSV injection).
func csvEscape(s string) string {
	s = csvGuardFormula(s)
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// csvGuardFormula neutralizes spreadsheet formula injection by prefixing
// cells that start with a formula trigger character with a single quote.
func csvGuardFormula(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@':
		return "'" + s
	default:
		return s
	}
}

// handlePlanCheck analyzes EXPLAIN ANALYZE output for performance issues.
// It accepts a JSON body with planText, runs the plan checker, records metrics,
// and returns the analysis result.
func (s *Server) handlePlanCheck(w http.ResponseWriter, r *http.Request) {
	// Static-named child span (D-6); downstream db spans nest under it.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.planCheck")
	defer span.End()

	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req planchecker.PlanCheckRequest
	if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if strings.TrimSpace(req.PlanText) == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "planText is required and must not be empty")
		return
	}

	start := time.Now()
	result, checkErr := planchecker.CheckPlan(req.PlanText)
	duration := time.Since(start)

	if checkErr != nil {
		s.logger.Error("plan check failed",
			"cluster", cluster.Name, "error", checkErr)
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("failed to analyze plan: %v", checkErr))
		return
	}

	// Record metrics.
	if s.metrics != nil {
		s.metrics.RecordPlanCheck(cluster.Name, cluster.Namespace)
		s.metrics.ObservePlanCheckDuration(cluster.Name, cluster.Namespace, duration)
		for _, issue := range result.Issues {
			s.metrics.RecordPlanCheckIssue(cluster.Name, cluster.Namespace, issue.Severity, issue.Category)
		}
	}

	s.logger.Info("plan check completed",
		"cluster", cluster.Name,
		"issues", len(result.Issues),
		"totalNodes", result.TotalNodes,
		"duration", duration)

	writeJSON(w, http.StatusOK, result)
}

// parseQueryHistoryFilter parses query parameters into a QueryHistoryFilter.
func parseQueryHistoryFilter(r *http.Request) (db.QueryHistoryFilter, error) {
	params := r.URL.Query()
	filter := db.QueryHistoryFilter{
		Pattern:       params.Get("pattern"),
		PatternType:   params.Get("patternType"),
		Username:      params.Get("user"),
		Database:      params.Get("database"),
		ResourceGroup: params.Get("resourceGroup"),
		State:         params.Get("state"),
	}

	if err := applyQueryHistoryPaging(&filter, params); err != nil {
		return filter, err
	}

	if err := applyQueryHistoryTimeRange(&filter, params); err != nil {
		return filter, err
	}

	// Validate regex pattern if provided.
	if filter.Pattern != "" && (filter.PatternType == "" || filter.PatternType == "regex") {
		if _, err := regexp.Compile(filter.Pattern); err != nil {
			return filter, fmt.Errorf("invalid regex pattern: %w", err)
		}
	}

	return filter, nil
}

// applyQueryHistoryPaging parses limit, offset and minDuration parameters.
func applyQueryHistoryPaging(filter *db.QueryHistoryFilter, params url.Values) error {
	if limitStr := params.Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit < 0 {
			return fmt.Errorf("invalid limit parameter: %q", limitStr)
		}
		filter.Limit = limit
	}

	if offsetStr := params.Get("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			return fmt.Errorf("invalid offset parameter: %q", offsetStr)
		}
		filter.Offset = offset
	}

	if minDurStr := params.Get("minDuration"); minDurStr != "" {
		minDur, err := strconv.ParseFloat(minDurStr, 64)
		if err != nil || minDur < 0 {
			return fmt.Errorf("invalid minDuration parameter: %q", minDurStr)
		}
		filter.MinDuration = minDur
	}

	return nil
}

// applyQueryHistoryTimeRange parses the since and until parameters.
func applyQueryHistoryTimeRange(filter *db.QueryHistoryFilter, params url.Values) error {
	// Parse since (supports both RFC3339 and Go duration like "24h").
	if sinceStr := params.Get("since"); sinceStr != "" {
		filter.Since = parseSinceTime(sinceStr)
	}

	// Parse until (RFC3339 only).
	if untilStr := params.Get("until"); untilStr != "" {
		t, err := time.Parse(time.RFC3339, untilStr)
		if err != nil {
			return fmt.Errorf("invalid until parameter: %q (expected RFC3339 format)", untilStr)
		}
		filter.Until = t
	}

	return nil
}

// parseSinceTime parses a "since" parameter that can be either an RFC3339 timestamp
// or a Go duration string (e.g., "24h", "30m").
func parseSinceTime(s string) time.Time {
	// Try RFC3339 first.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	// Try Go duration (relative to now).
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().Add(-d)
	}
	return time.Time{}
}

// backupEnabled reports whether backup is enabled for the cluster. It mirrors
// the gating used by handleCreateBackup (BACKUP_NOT_ENABLED) so the list and
// schedule endpoints surface the same disabled state.
func backupEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.Backup != nil && cluster.Spec.Backup.Enabled
}

// handleListBackups lists backups for a cluster from its status backup history.
func (s *Server) handleListBackups(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	history := cluster.Status.BackupHistory
	backups := make([]cbv1alpha1.BackupHistoryEntry, 0, len(history))
	backups = append(backups, history...)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyCluster: cluster.Name,
		// "enabled" surfaces whether backup is enabled for the cluster so clients
		// can detect the disabled state from the list endpoint (Scenario 88a).
		// Existing keys are preserved for back-compat and the status stays 200.
		responseKeyEnabled:    backupEnabled(cluster),
		"backups":             backups,
		responseKeyTotal:      len(backups),
		"lastBackupTime":      cluster.Status.LastBackupTime,
		"lastBackupTimestamp": cluster.Status.LastBackupTimestamp,
		"lastBackupStatus":    cluster.Status.LastBackupStatus,
	})
}

// handleCreateBackup creates a new on-demand backup Job for a cluster.
func (s *Server) handleCreateBackup(w http.ResponseWriter, r *http.Request) {
	// Static-named child span (D-6); downstream db spans nest under it.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "api.backup.create")
	defer span.End()

	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(ctx, name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		writeErrorJSON(w, http.StatusBadRequest, "BACKUP_NOT_ENABLED",
			"backup is not enabled for this cluster")
		return
	}

	var req CreateBackupRequest
	if decErr := decodeOptionalJSON(r, &req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}
	if !requireBackupDatabase(w, req.Databases) {
		return
	}
	if !validateBackupDatabases(w, req.Databases) {
		return
	}

	backupType := backupTypeOrDefault(req.Type)
	if backupType != util.BackupTypeFull && backupType != util.BackupTypeIncremental {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid backup type %q; valid types: full, incremental", backupType))
		return
	}

	timestamp := time.Now().UTC().Format(util.GpbackupTimestampLayout)
	opts := buildBackupJobOptions(cluster, &req, backupType, timestamp)

	job := s.builder.BuildBackupJob(cluster, opts)
	if job == nil {
		// Defense in depth: the builder refuses to render a broken gpbackup
		// command (e.g. no resolvable --dbname) instead of returning a Job
		// that fails at runtime.
		s.logger.Error("builder returned no backup job", "cluster", name)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to build backup job")
		return
	}
	if createErr := s.k8sClient.Create(ctx, job); createErr != nil {
		s.logger.Error("failed to create backup job", "cluster", name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to create backup job")
		return
	}

	if s.metrics != nil {
		s.metrics.RecordBackup(cluster.Name, cluster.Namespace, backupType, "started")
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:    "backup started",
		responseKeyCluster:   cluster.Name,
		responseKeyJob:       job.Name,
		responseKeyTimestamp: timestamp,
		"type":               backupType,
	})
}

// handleGetBackup gets a specific backup by gpbackup timestamp from status history.
func (s *Server) handleGetBackup(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	timestamp := r.PathValue("timestamp")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}
	if !isValidBackupTimestamp(timestamp) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid backup timestamp %q", timestamp))
		return
	}

	for i := range cluster.Status.BackupHistory {
		if cluster.Status.BackupHistory[i].Timestamp == timestamp {
			writeJSON(w, http.StatusOK, cluster.Status.BackupHistory[i])
			return
		}
	}

	writeErrorJSON(w, http.StatusNotFound, "BACKUP_NOT_FOUND",
		fmt.Sprintf("backup %q not found", timestamp))
}

// handleDeleteBackup deletes a backup by creating a retention/cleanup Job.
func (s *Server) handleDeleteBackup(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	timestamp := r.PathValue("timestamp")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}
	if !isValidBackupTimestamp(timestamp) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid backup timestamp %q", timestamp))
		return
	}

	job := s.builder.BuildRetentionCleanupJob(cluster, timestamp)
	if createErr := s.k8sClient.Create(r.Context(), job); createErr != nil {
		s.logger.Error("failed to create cleanup job", "cluster", name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to create cleanup job")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:    statusDeleted,
		responseKeyCluster:   cluster.Name,
		responseKeyJob:       job.Name,
		responseKeyTimestamp: timestamp,
	})
}

// handleRestoreBackup restores from a backup by creating a gprestore Job.
func (s *Server) handleRestoreBackup(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	namespace := r.URL.Query().Get("namespace")
	cluster, err := s.getCluster(r.Context(), name, namespace)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordRestore(name, namespace, "failed")
		}
		writeClusterNotFound(w, name)
		return
	}

	var req RestoreRequest
	if decErr := decodeOptionalJSON(r, &req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	// Prefer the path timestamp; fall back to the body timestamp.
	timestamp := r.PathValue("timestamp")
	if timestamp == "" {
		timestamp = req.Timestamp
	}
	if !isValidBackupTimestamp(timestamp) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid backup timestamp %q", timestamp))
		return
	}
	if !validateBackupDatabases(w, req.Databases) {
		return
	}
	if msg := restoreOptionsConflict(req.GprestoreOptions); msg != "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, msg)
		return
	}

	opts := buildRestoreJobOptions(cluster, &req, timestamp)
	job := s.builder.BuildRestoreJob(cluster, opts)
	if createErr := s.k8sClient.Create(r.Context(), job); createErr != nil {
		s.logger.Error("failed to create restore job", "cluster", name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to create restore job")
		return
	}

	if s.metrics != nil {
		s.metrics.RecordRestore(cluster.Name, cluster.Namespace, "started")
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:    "restore started",
		responseKeyCluster:   cluster.Name,
		responseKeyJob:       job.Name,
		responseKeyTimestamp: timestamp,
	})
}

// handleListBackupJobs lists backup/restore/cleanup Job statuses for a cluster.
func (s *Server) handleListBackupJobs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	jobList := &batchv1.JobList{}
	if listErr := s.k8sClient.List(r.Context(), jobList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); listErr != nil {
		s.logger.Error("failed to list backup jobs", "cluster", name, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to list backup jobs")
		return
	}

	jobs := make([]backupJobInfo, 0, len(jobList.Items))
	for i := range jobList.Items {
		job := &jobList.Items[i]
		operation := job.Labels[util.LabelBackupOperation]
		if !isBackupOperation(operation) {
			continue
		}
		jobs = append(jobs, newBackupJobInfo(job, operation))
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyCluster: cluster.Name,
		"jobs":             jobs,
		responseKeyTotal:   len(jobs),
	})
}

// handleBackupJobLogs streams the container logs of the pod backing a backup
// Job. It resolves the cluster, locates the Job's most recent pod via the
// job-name label, and copies the pod log stream to the response as plain text.
// Supported query parameters: ?follow=true and ?tailLines=N.
func (s *Server) handleBackupJobLogs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	job := r.PathValue("job")

	if !isValidDNS1123Name(job) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid job name %q", job))
		return
	}

	if s.clientset == nil {
		writeErrorJSON(w, http.StatusNotImplemented, "LOGS_NOT_AVAILABLE",
			"log streaming is not available: the operator API has no Kubernetes clientset configured")
		return
	}

	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get(responseKeyNamespace))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	podName, found, listErr := s.findJobPod(r.Context(), cluster.Namespace, job)
	if listErr != nil {
		s.logger.Error("failed to list job pods", "cluster", name, "job", job, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to list job pods")
		return
	}
	if !found {
		writeErrorJSON(w, http.StatusNotFound, "JOB_NOT_FOUND",
			fmt.Sprintf("no pods found for job %q", job))
		return
	}

	s.streamPodLogs(w, r, cluster.Namespace, podName, job)
}

// findJobPod returns the name of the most recently created pod owned by the
// given Job in the namespace. It tries the current and legacy job-name labels.
// The second return value is false when no pod is found.
func (s *Server) findJobPod(
	ctx context.Context,
	namespace, job string,
) (podName string, found bool, err error) {
	for _, labelKey := range []string{labelJobNameBatch, labelJobName} {
		podList := &corev1.PodList{}
		if listErr := s.k8sClient.List(ctx, podList,
			client.InNamespace(namespace),
			client.MatchingLabels{labelKey: job},
		); listErr != nil {
			return "", false, listErr
		}
		if name := mostRecentPodName(podList.Items); name != "" {
			return name, true, nil
		}
	}
	return "", false, nil
}

// mostRecentPodName returns the name of the pod with the latest creation
// timestamp, or an empty string when the slice is empty.
func mostRecentPodName(pods []corev1.Pod) string {
	var (
		latest    *corev1.Pod
		latestIdx int
	)
	for i := range pods {
		if latest == nil || pods[i].CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = &pods[i]
			latestIdx = i
		}
	}
	if latest == nil {
		return ""
	}
	return pods[latestIdx].Name
}

// streamPodLogs opens the pod log stream and copies it to the response writer as
// plain text, flushing periodically when the client supports it (for --follow).
func (s *Server) streamPodLogs(
	w http.ResponseWriter,
	r *http.Request,
	namespace, podName, job string,
) {
	opts := buildPodLogOptions(r.URL.Query())
	req := s.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	stream, err := req.Stream(r.Context())
	if err != nil {
		s.logger.Error("failed to open pod log stream",
			"namespace", namespace, "pod", podName, "job", job, "error", err)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to stream job logs")
		return
	}
	defer func() {
		if cErr := stream.Close(); cErr != nil {
			s.logger.Warn("failed to close pod log stream", "pod", podName, "error", cErr)
		}
	}()

	// Follow mode keeps the connection open indefinitely; exempt it from the
	// global WriteTimeout (the request context still bounds the stream).
	if opts.Follow {
		s.clearWriteDeadline(w)
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	written, copyErr := copyLogStream(r.Context(), w, stream, opts.Follow)
	s.recordLogStreamSession(written, copyErr)
}

// recordLogStreamSession records one completed log streaming session and the
// bytes delivered (C-8). A session that fails mid-stream (client write error)
// is labeled result="error"; normal termination — source EOF or client
// disconnect via context cancellation — is "success".
func (s *Server) recordLogStreamSession(written int64, copyErr error) {
	if s.metrics == nil {
		return
	}
	result := resultSuccess
	if copyErr != nil {
		result = resultError
	}
	s.metrics.RecordLogStreamSession(result)
	if written > 0 {
		s.metrics.AddLogStreamBytes(float64(written))
	}
}

// buildPodLogOptions builds the pod log options from the request query
// parameters: ?follow=true and ?tailLines=N (only applied when valid).
func buildPodLogOptions(query url.Values) *corev1.PodLogOptions {
	opts := &corev1.PodLogOptions{}
	if follow, err := strconv.ParseBool(query.Get("follow")); err == nil {
		opts.Follow = follow
	}
	if raw := query.Get("tailLines"); raw != "" {
		if tail, err := strconv.ParseInt(raw, 10, 64); err == nil && tail >= 0 {
			opts.TailLines = &tail
		}
	}
	return opts
}

// copyLogStream copies the pod log stream to the response writer. When
// following, it flushes periodically via http.NewResponseController so the
// client receives output incrementally. The ResponseController reaches the
// real writer through any wrapper that implements Unwrap (statusRecorder),
// and degrades gracefully (non-flushing copy) when the underlying writer does
// not support flushing.
//
// It returns the number of bytes delivered to the client and a non-nil error
// only when the CLIENT write failed mid-stream (the metrics hook labels such
// sessions result="error"); source EOF and context cancellation are normal
// session terminations.
func copyLogStream(
	ctx context.Context,
	w http.ResponseWriter,
	stream io.Reader,
	follow bool,
) (written int64, err error) {
	if !follow {
		return io.Copy(w, stream)
	}

	rc := http.NewResponseController(w)
	buf := make([]byte, 4096)
	// lastFlush starts at the zero time so the FIRST chunk is flushed
	// immediately — follow clients see output as soon as it exists instead
	// of waiting a full flush interval.
	var lastFlush time.Time
	for {
		if ctx.Err() != nil {
			// Client disconnect (context canceled) is a NORMAL stream
			// termination, not a delivery failure.
			return written, nil //nolint:nilerr // intentional: cancel == clean end
		}
		n, readErr := stream.Read(buf)
		if n > 0 {
			wn, wErr := w.Write(buf[:n])
			written += int64(wn)
			if wErr != nil {
				return written, wErr
			}
			if time.Since(lastFlush) >= jobLogsFlushInterval {
				// ErrNotSupported (or any other flush error) is non-fatal:
				// the copy continues without incremental delivery.
				_ = rc.Flush()
				lastFlush = time.Now()
			}
		}
		if readErr != nil {
			// Source EOF (or read error after delivery) ends the session
			// cleanly; only CLIENT write failures count as errors.
			_ = rc.Flush()
			return written, nil //nolint:nilerr // intentional: source EOF == clean end
		}
	}
}

// clearWriteDeadline exempts a streaming response (log follow mode, CSV
// export) from the server-wide WriteTimeout by clearing the per-connection
// write deadline. Without this, the global 60s WriteTimeout cuts long-lived
// streams mid-flight. The request context still bounds the stream lifetime,
// so canceled clients terminate promptly. Errors (e.g. the underlying writer
// not supporting deadlines, as in tests) are logged at debug and ignored —
// the stream then simply remains subject to the global timeout.
func (s *Server) clearWriteDeadline(w http.ResponseWriter) {
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
		s.logger.Debug("failed to clear write deadline for streaming response", "error", err)
	}
}

// handleGetBackupSchedule returns the backup CronJob status and next run time.
func (s *Server) handleGetBackupSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	cronJob := &batchv1.CronJob{}
	key := client.ObjectKey{Name: util.BackupCronJobName(cluster.Name), Namespace: cluster.Namespace}
	if getErr := s.k8sClient.Get(r.Context(), key, cronJob); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				responseKeyCluster: cluster.Name,
				// "enabled" lets the schedule endpoint also report the disabled
				// state when no CronJob exists (Scenario 88a).
				responseKeyEnabled: backupEnabled(cluster),
				"scheduled":        false,
			})
			return
		}
		s.logger.Error("failed to get backup schedule", "cluster", name, "error", getErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to get backup schedule")
		return
	}

	// Add "enabled" to the response without changing the shared helper's
	// signature, keeping backupScheduleResponse back-compatible.
	resp := backupScheduleResponse(cluster.Name, cronJob)
	resp[responseKeyEnabled] = backupEnabled(cluster)
	writeJSON(w, http.StatusOK, resp)
}

// handleUpdateBackupSchedule sets the backup schedule and/or suspends/resumes the
// backup CronJob. A non-nil `schedule` updates spec.backup.schedule (which the
// operator reconciles into the CronJob); a non-nil `suspend` patches the existing
// CronJob's .spec.suspend in place for an immediate effect.
func (s *Server) handleUpdateBackupSchedule(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var req UpdateBackupScheduleRequest
	if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}
	if req.Schedule != nil && *req.Schedule != "" && !isValidCronSchedule(*req.Schedule) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid cron schedule %q", *req.Schedule))
		return
	}

	result := map[string]interface{}{
		responseKeyStatus:  statusUpdated,
		responseKeyCluster: cluster.Name,
	}
	if !s.applyScheduleUpdate(w, r, cluster, &req, result) {
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// applyScheduleUpdate performs the schedule and suspend updates, writing an error
// response and returning false on failure.
func (s *Server) applyScheduleUpdate(
	w http.ResponseWriter,
	r *http.Request,
	cluster *cbv1alpha1.CloudberryCluster,
	req *UpdateBackupScheduleRequest,
	result map[string]interface{},
) bool {
	if req.Schedule != nil {
		if cluster.Spec.Backup == nil {
			writeErrorJSON(w, http.StatusBadRequest, "BACKUP_NOT_ENABLED",
				"backup is not configured for this cluster")
			return false
		}
		// Spec mutation: re-apply on a fresh object inside conflict retry.
		updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
			func(latest *cbv1alpha1.CloudberryCluster) error {
				if latest.Spec.Backup == nil {
					return errBackupNotConfigured
				}
				latest.Spec.Backup.Schedule = *req.Schedule
				return nil
			})
		if errors.Is(updateErr, errBackupNotConfigured) {
			writeErrorJSON(w, http.StatusBadRequest, "BACKUP_NOT_ENABLED",
				"backup is not configured for this cluster")
			return false
		}
		if updateErr != nil {
			s.logger.Error("failed to update backup schedule", "cluster", cluster.Name, "error", updateErr)
			writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
				fmt.Sprintf("failed to update backup schedule: %v", updateErr))
			return false
		}
		result["schedule"] = *req.Schedule
	}

	if req.Suspend != nil {
		if !s.patchCronJobSuspend(w, r, cluster, *req.Suspend) {
			return false
		}
		result["suspend"] = *req.Suspend
	}
	return true
}

// patchCronJobSuspend sets the backup CronJob's .spec.suspend field in place.
func (s *Server) patchCronJobSuspend(
	w http.ResponseWriter,
	r *http.Request,
	cluster *cbv1alpha1.CloudberryCluster,
	suspend bool,
) bool {
	cronJob := &batchv1.CronJob{}
	key := client.ObjectKey{Name: util.BackupCronJobName(cluster.Name), Namespace: cluster.Namespace}
	if getErr := s.k8sClient.Get(r.Context(), key, cronJob); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			writeErrorJSON(w, http.StatusNotFound, "SCHEDULE_NOT_FOUND",
				"backup schedule does not exist for this cluster")
			return false
		}
		s.logger.Error("failed to get backup schedule", "cluster", cluster.Name, "error", getErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to get backup schedule")
		return false
	}

	suspendCopy := suspend
	cronJob.Spec.Suspend = &suspendCopy
	if updateErr := s.k8sClient.Update(r.Context(), cronJob); updateErr != nil {
		s.logger.Error("failed to update backup schedule suspend", "cluster", cluster.Name, "error", updateErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to update backup schedule")
		return false
	}
	return true
}

// handleListDataLoadingJobs lists data loading jobs for a cluster.
func (s *Server) handleListDataLoadingJobs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	var jobs []interface{}
	if cluster.Spec.DataLoading != nil {
		for i := range cluster.Spec.DataLoading.Jobs {
			jobs = append(jobs, cluster.Spec.DataLoading.Jobs[i])
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs":           jobs,
		responseKeyTotal: len(jobs),
	})
}

// msgDataLoadingNotImplemented is the standard 501 message for the
// data-loading job mutation endpoints. The full implementation (patching
// spec.dataLoading.jobs, creating loader Jobs and wiring
// RecordDataLoadingRows) is tracked as a dedicated feature; until it lands,
// these endpoints honestly report NOT_IMPLEMENTED instead of fake successes.
const msgDataLoadingNotImplemented = "data loading job mutations are not implemented yet; " +
	"track the data-loading feature for availability. Read-only endpoints (list/get) remain available"

// writeDataLoadingNotImplemented validates that the target cluster exists
// (preserving the 404 contract) and then writes the standard 501
// NOT_IMPLEMENTED error envelope shared by all five data-loading mutation
// endpoints. No success metric or event is emitted from these stubs.
func (s *Server) writeDataLoadingNotImplemented(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace")); err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeErrorJSON(w, http.StatusNotImplemented, errCodeNotImplemented,
		msgDataLoadingNotImplemented)
}

// handleCreateDataLoadingJob is a 501 stub: data-loading job creation is not
// implemented yet (see msgDataLoadingNotImplemented).
func (s *Server) handleCreateDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	s.writeDataLoadingNotImplemented(w, r)
}

// handleGetDataLoadingJob gets a specific data loading job.
func (s *Server) handleGetDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	jobName := r.PathValue("job")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if cluster.Spec.DataLoading != nil {
		for i := range cluster.Spec.DataLoading.Jobs {
			if cluster.Spec.DataLoading.Jobs[i].Name == jobName {
				writeJSON(w, http.StatusOK, cluster.Spec.DataLoading.Jobs[i])
				return
			}
		}
	}

	writeErrorJSON(w, http.StatusNotFound, "JOB_NOT_FOUND",
		fmt.Sprintf("data loading job %q not found", jobName))
}

// handleUpdateDataLoadingJob is a 501 stub: data-loading job updates are not
// implemented yet (see msgDataLoadingNotImplemented).
func (s *Server) handleUpdateDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	s.writeDataLoadingNotImplemented(w, r)
}

// handleDeleteDataLoadingJob is a 501 stub: data-loading job deletion is not
// implemented yet (see msgDataLoadingNotImplemented).
func (s *Server) handleDeleteDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	s.writeDataLoadingNotImplemented(w, r)
}

// handleStartDataLoadingJob is a 501 stub: starting data-loading jobs is not
// implemented yet (see msgDataLoadingNotImplemented).
//
// NOTE: the cloudberry_data_loading_rows_total metric
// (Recorder.RecordDataLoadingRows) is intentionally NOT recorded here: there
// is no rows-loaded signal available. The recorder call must be wired at the
// point where a data-loading Job actually completes and reports its
// loaded-row count, which does not exist yet. Recording a value here would
// fabricate data, so it is deliberately omitted.
func (s *Server) handleStartDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	s.writeDataLoadingNotImplemented(w, r)
}

// handleStopDataLoadingJob is a 501 stub: stopping data-loading jobs is not
// implemented yet (see msgDataLoadingNotImplemented).
func (s *Server) handleStopDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	s.writeDataLoadingNotImplemented(w, r)
}

// handleListPVCs lists all PVCs for a cluster with their sizes.
func (s *Server) handleListPVCs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if listErr := s.k8sClient.List(r.Context(), pvcList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{util.LabelCluster: cluster.Name},
	); listErr != nil {
		s.logger.Error("failed to list PVCs", "cluster", name, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to list PVCs")
		return
	}

	type pvcInfo struct {
		Name      string `json:"name"`
		Component string `json:"component"`
		Size      string `json:"size"`
		Phase     string `json:"phase"`
	}

	items := make([]pvcInfo, 0, len(pvcList.Items))
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		size := ""
		if qty, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			size = qty.String()
		}
		items = append(items, pvcInfo{
			Name:      pvc.Name,
			Component: pvc.Labels[util.LabelComponent],
			Size:      size,
			Phase:     string(pvc.Status.Phase),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pvcs":           items,
		responseKeyTotal: len(items),
	})
}

// handleGetDiskUsage returns disk usage information for a cluster. When a
// database factory is available the per-database usage is queried live and
// recorded on the disk_usage_bytes gauge; otherwise the cached status value
// is returned with an empty per-database breakdown.
func (s *Server) handleGetDiskUsage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	usage := s.collectDiskUsage(r.Context(), cluster)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyCluster: cluster.Name,
		"diskUsagePercent": cluster.Status.DiskUsagePercent,
		"diskUsage":        usage,
	})
}

// collectDiskUsage queries per-database disk usage via the DB client when
// available and records each database's size on the disk_usage_bytes gauge.
// It returns the usage slice for the API response, or an empty slice when the
// DB factory is unavailable or the query fails.
func (s *Server) collectDiskUsage(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) []db.DiskUsage {
	if s.dbFactory == nil {
		return []db.DiskUsage{}
	}

	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for disk usage",
			"cluster", cluster.Name, "error", dbErr)
		return []db.DiskUsage{}
	}
	defer dbClient.Close()

	usage, usageErr := dbClient.GetDiskUsage(ctx, "")
	if usageErr != nil {
		s.logger.Error("failed to query disk usage",
			"cluster", cluster.Name, "error", usageErr)
		return []db.DiskUsage{}
	}

	if s.metrics != nil {
		for i := range usage {
			s.metrics.SetDiskUsageBytes(
				cluster.Name, cluster.Namespace,
				usage[i].Database, float64(usage[i].SizeBytes),
			)
		}
	}

	return usage
}

// handleListTables lists tables in a cluster.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace")); err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables":         []interface{}{},
		responseKeyTotal: 0,
	})
}

// handleGetTableDetail returns detailed information about a specific table.
func (s *Server) handleGetTableDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	schema := r.PathValue("schema")
	table := r.PathValue("table")
	if _, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace")); err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"schema": schema,
		"table":  table,
	})
}

// handleListRecommendations lists storage recommendations for a cluster.
func (s *Server) handleListRecommendations(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyCluster:    cluster.Name,
		"recommendations":     []interface{}{},
		"recommendationCount": cluster.Status.RecommendationCount,
		responseKeyTotal:      0,
	})
}

// handleTriggerRecommendationScan triggers a recommendation scan.
func (s *Server) handleTriggerRecommendationScan(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	if cluster.Spec.Storage == nil || cluster.Spec.Storage.RecommendationScan == nil ||
		!cluster.Spec.Storage.RecommendationScan.Enabled {
		writeErrorJSON(w, http.StatusBadRequest, "RECOMMENDATION_SCAN_NOT_ENABLED",
			"recommendation scanning is not enabled for this cluster")
		return
	}

	s.runRecommendationScan(r.Context(), cluster)

	writeJSON(w, http.StatusAccepted, map[string]string{
		responseKeyStatus:  "scan initiated",
		responseKeyCluster: cluster.Name,
	})
}

// runRecommendationScan performs a best-effort recommendation scan via the DB
// client when available, recording the scan duration and the number of
// recommendations per type. It is a no-op when no DB factory is configured.
func (s *Server) runRecommendationScan(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	if s.dbFactory == nil || s.metrics == nil {
		return
	}

	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Error("failed to create database client for recommendation scan",
			"cluster", cluster.Name, "error", dbErr)
		return
	}
	defer dbClient.Close()

	start := time.Now()
	counts := map[string]float64{}
	for _, fetch := range []func(context.Context) ([]db.Recommendation, error){
		dbClient.GetBloatRecommendations,
		dbClient.GetSkewRecommendations,
		dbClient.GetAgeRecommendations,
		dbClient.GetIndexBloatRecommendations,
	} {
		recs, fetchErr := fetch(ctx)
		if fetchErr != nil {
			s.logger.Error("recommendation fetch failed",
				"cluster", cluster.Name, "error", fetchErr)
			continue
		}
		for i := range recs {
			counts[recs[i].Type]++
		}
	}
	s.metrics.ObserveRecommendationScanDuration(cluster.Name, cluster.Namespace, time.Since(start))

	for recType, count := range counts {
		s.metrics.SetRecommendationsTotal(cluster.Name, cluster.Namespace, recType, count)
	}
}

// handleGetUsageReport returns a usage report for a cluster.
func (s *Server) handleGetUsageReport(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace")); err != nil {
		writeClusterNotFound(w, name)
		return
	}

	month := r.URL.Query().Get("month")
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"month":          month,
		"entries":        []interface{}{},
		responseKeyTotal: 0,
	})
}

// setClusterAnnotation sets an action annotation on a cluster.
func (s *Server) setClusterAnnotation(w http.ResponseWriter, r *http.Request, action string) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Annotation-only mutation: use a MergeFrom patch (conflict-safe).
	if patchErr := s.patchClusterAnnotation(r.Context(), cluster,
		util.AnnotationAction, action); patchErr != nil {
		s.logger.Error("failed to set action annotation", "cluster", name, "action", action, "error", patchErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to set action")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: action + " initiated"})
}

// setMaintenanceAnnotation sets a maintenance annotation on a cluster.
func (s *Server) setMaintenanceAnnotation(w http.ResponseWriter, r *http.Request, operation string) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeClusterNotFound(w, name)
		return
	}

	// Annotation-only mutation: use a MergeFrom patch (conflict-safe).
	if patchErr := s.patchClusterAnnotation(r.Context(), cluster,
		util.AnnotationMaintenance, operation); patchErr != nil {
		s.logger.Error("failed to set maintenance annotation",
			"cluster", name, "operation", operation, "error", patchErr)
		writeErrorJSON(w, http.StatusInternalServerError,
			errCodeInternal, "failed to set maintenance")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: operation + " initiated"})
}

// errWorkloadRuleNotFound is the sentinel returned by conflict-retry mutate
// closures when the target workload rule disappeared between the handler's
// pre-validation and the retried update. Handlers map it to 404.
var errWorkloadRuleNotFound = fmt.Errorf("workload rule not found")

// errDuplicateWorkloadRule is the sentinel returned by conflict-retry mutate
// closures when a rule with the same name appeared concurrently. Handlers map
// it to 400 DUPLICATE_RULE.
var errDuplicateWorkloadRule = fmt.Errorf("workload rule already exists")

// errBackupNotConfigured is the sentinel returned by conflict-retry mutate
// closures when spec.backup disappeared between pre-validation and the
// retried update. Handlers map it to 400 BACKUP_NOT_ENABLED.
var errBackupNotConfigured = fmt.Errorf("backup is not configured")

// updateClusterWithConflictRetry re-reads the cluster and applies mutate
// inside retry.RetryOnConflict so concurrent controller updates (which patch
// annotations/status constantly) do not surface as 409→500 to API clients.
// The mutate closure runs against a freshly fetched object on every attempt
// and may return a sentinel error to abort the retry loop. On success the
// latest cluster state is copied back into cluster.
func (s *Server) updateClusterWithConflictRetry(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	mutate func(*cbv1alpha1.CloudberryCluster) error,
) error {
	key := client.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &cbv1alpha1.CloudberryCluster{}
		if err := s.k8sClient.Get(ctx, key, latest); err != nil {
			return err
		}
		if err := mutate(latest); err != nil {
			return err
		}
		if err := s.k8sClient.Update(ctx, latest); err != nil {
			return err
		}
		latest.DeepCopyInto(cluster)
		return nil
	})
}

// patchClusterAnnotation sets a single annotation on the cluster using a
// MergeFrom patch (the same conflict-safe pattern the controllers use), so
// annotation-only API mutations never fail with 409 under concurrent
// controller updates.
func (s *Server) patchClusterAnnotation(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	key, value string,
) error {
	base := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[key] = value
	return s.k8sClient.Patch(ctx, cluster, client.MergeFrom(base))
}

// getCluster retrieves a CloudberryCluster by name.
// It validates the name and namespace parameters for DNS-1123 compliance.
func (s *Server) getCluster(
	ctx context.Context,
	name, namespace string,
) (*cbv1alpha1.CloudberryCluster, error) {
	if !isValidDNS1123Name(name) {
		return nil, fmt.Errorf("invalid cluster name %q", name)
	}
	if namespace != "" && !isValidDNS1123Name(namespace) {
		return nil, fmt.Errorf("invalid namespace %q", namespace)
	}

	if namespace == "" {
		// List all namespaces and find the cluster.
		clusterList := &cbv1alpha1.CloudberryClusterList{}
		if err := s.k8sClient.List(ctx, clusterList); err != nil {
			return nil, err
		}
		for i := range clusterList.Items {
			if clusterList.Items[i].Name == name {
				return &clusterList.Items[i], nil
			}
		}
		return nil, fmt.Errorf("cluster %q not found", name)
	}

	cluster := &cbv1alpha1.CloudberryCluster{}
	key := client.ObjectKey{Name: name, Namespace: namespace}
	if err := s.k8sClient.Get(ctx, key, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

// writeJSON writes a JSON response via the shared encoder (internal/httpjson).
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	httpjson.Write(w, status, data, apiResponseLogger())
}

// writeErrorJSON writes the unified JSON error envelope via internal/httpjson
// so API-layer and auth-layer errors share one contract (L-9).
func writeErrorJSON(w http.ResponseWriter, status int, code, message string) {
	httpjson.WriteError(w, status, code, message, apiResponseLogger())
}

// apiResponseLogger returns the component logger used for response-encoding
// failures. These helpers are free functions (no *Server receiver), so the
// component attribution comes from the default logger.
func apiResponseLogger() *slog.Logger {
	return slog.Default().With("component", "api-server")
}

// StartServer starts the API server.
func StartServer(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	//nolint:contextcheck,gosec // fresh ctx needed for graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), httpShutdownTimeout,
		)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("API server shutdown error", "error", err)
		}
	}()

	logger.Info("starting API server", "address", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("API server error: %w", err)
	}
	return nil
}
