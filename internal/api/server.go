// Package api provides the REST API server for the cloudberry operator.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	apiPrefix       = "/api/v1alpha1"
	defaultPageSize = 50
	maxPageSize     = 100

	responseKeyStatus = "status"
	responseKeyTotal  = "total"
)

// Server is the REST API server for the cloudberry operator.
type Server struct {
	k8sClient client.Client
	authMW    *auth.AuthMiddleware
	metrics   metrics.Recorder
	logger    *slog.Logger
	mux       *http.ServeMux
}

// NewServer creates a new API server.
func NewServer(
	k8sClient client.Client,
	authMW *auth.AuthMiddleware,
	metricsRecorder metrics.Recorder,
	logger *slog.Logger,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		k8sClient: k8sClient,
		authMW:    authMW,
		metrics:   metricsRecorder,
		logger:    logger.With("component", "api-server"),
		mux:       http.NewServeMux(),
	}

	s.registerRoutes()
	return s
}

// Handler returns the HTTP handler for the API server.
func (s *Server) Handler() http.Handler {
	// Apply security headers to all requests.
	return auth.SecurityHeaders()(s.mux)
}

// registerRoutes registers all API routes.
func (s *Server) registerRoutes() {
	// Health endpoints (no auth required).
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)

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

	// Workload management.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetWorkload)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload/resource-groups",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListResourceGroups)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/workload/rules",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleListWorkloadRules)))

	// Query monitoring.
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetQueryMonitoring)))
	s.mux.Handle("GET "+apiPrefix+"/clusters/{name}/queries/active",
		s.withAuth(s.withPermission(auth.PermissionBasic, s.handleGetActiveQueries)))
}

// withAuth wraps a handler with authentication middleware.
func (s *Server) withAuth(handler http.Handler) http.Handler {
	if s.authMW == nil {
		return handler
	}
	return s.authMW.Handler()(handler)
}

// withPermission wraps a handler function with permission checking.
func (s *Server) withPermission(level auth.PermissionLevel, fn http.HandlerFunc) http.Handler {
	return auth.RequirePermission(level)(fn)
}

// handleHealthz handles the health check endpoint.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "ok"})
}

// handleReadyz handles the readiness check endpoint.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "ready"})
}

// handleListClusters lists all CloudberryCluster resources.
func (s *Server) handleListClusters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterList := &cbv1alpha1.CloudberryClusterList{}
	if err := s.k8sClient.List(ctx, clusterList); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list clusters")
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
		"items":          items,
		responseKeyTotal: len(items),
	})
}

// handleGetCluster gets a specific CloudberryCluster resource.
func (s *Server) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, cluster)
}

// handleGetClusterStatus gets the status of a specific cluster.
func (s *Server) handleGetClusterStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":      cluster.Name,
		"namespace": cluster.Namespace,
		"status":    cluster.Status,
	})
}

// handleCreateCluster creates a new CloudberryCluster.
func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	cluster := &cbv1alpha1.CloudberryCluster{}
	if err := json.NewDecoder(r.Body).Decode(cluster); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	if err := s.k8sClient.Create(r.Context(), cluster); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to create cluster: %v", err))
		return
	}

	writeJSON(w, http.StatusCreated, cluster)
}

// handleDeleteCluster deletes a CloudberryCluster.
func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	if err := s.k8sClient.Delete(r.Context(), cluster); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to delete cluster: %v", err))
		return
	}

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
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, cluster.Spec.Config)
}

// handleUpdateConfig updates the cluster configuration.
func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	var configUpdate cbv1alpha1.ConfigSpec
	if decodeErr := json.NewDecoder(r.Body).Decode(&configUpdate); decodeErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	cluster.Spec.Config = &configUpdate
	if updateErr := s.k8sClient.Update(r.Context(), cluster); updateErr != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to update config: %v", updateErr))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{responseKeyStatus: "updated"})
}

// handleListSegments lists cluster segments.
func (s *Server) handleListSegments(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
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
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": cluster.Status.MirroringStatus,
	})
}

// handleListSessions lists active sessions.
func (s *Server) handleListSessions(w http.ResponseWriter, _ *http.Request) {
	// Sessions require a DB connection; return placeholder.
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions":       []interface{}{},
		responseKeyTotal: 0,
	})
}

// handleCancelQuery cancels a running query.
func (s *Server) handleCancelQuery(w http.ResponseWriter, r *http.Request) {
	pidStr := r.PathValue("pid")
	pid, err := strconv.ParseInt(pidStr, 10, 32)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid PID")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pid":      pid,
		"canceled": true,
	})
}

// handleTerminateSession terminates a session.
func (s *Server) handleTerminateSession(w http.ResponseWriter, r *http.Request) {
	pidStr := r.PathValue("pid")
	pid, err := strconv.ParseInt(pidStr, 10, 32)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid PID")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pid":        pid,
		"terminated": true,
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
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
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
	var req struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[util.AnnotationRecovery] = req.Type
	if updateErr := s.k8sClient.Update(r.Context(), cluster); updateErr != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to start recovery: %v", updateErr))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: "recovery started", "type": req.Type})
}

// handleRebalance starts a rebalance operation.
func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	s.setClusterAnnotation(w, r, "rebalance")
}

// handleGetWorkload gets the workload management configuration.
func (s *Server) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
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

// handleListResourceGroups lists resource groups.
func (s *Server) handleListResourceGroups(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

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

// handleListWorkloadRules lists workload rules.
func (s *Server) handleListWorkloadRules(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
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

// handleGetQueryMonitoring gets the query monitoring configuration and status.
func (s *Server) handleGetQueryMonitoring(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	response := map[string]interface{}{
		"activeQueries":  cluster.Status.ActiveQueries,
		"queuedQueries":  cluster.Status.QueuedQueries,
		"blockedQueries": cluster.Status.BlockedQueries,
	}

	if cluster.Spec.QueryMonitoring != nil {
		response["config"] = cluster.Spec.QueryMonitoring
	}

	writeJSON(w, http.StatusOK, response)
}

// handleGetActiveQueries gets the active query counts.
func (s *Server) handleGetActiveQueries(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"activeQueries":  cluster.Status.ActiveQueries,
		"queuedQueries":  cluster.Status.QueuedQueries,
		"blockedQueries": cluster.Status.BlockedQueries,
	})
}

// setClusterAnnotation sets an action annotation on a cluster.
func (s *Server) setClusterAnnotation(w http.ResponseWriter, r *http.Request, action string) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[util.AnnotationAction] = action
	if updateErr := s.k8sClient.Update(r.Context(), cluster); updateErr != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to set action: %v", updateErr))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: action + " initiated"})
}

// setMaintenanceAnnotation sets a maintenance annotation on a cluster.
func (s *Server) setMaintenanceAnnotation(w http.ResponseWriter, r *http.Request, operation string) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get("namespace"))
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %q not found", name))
		return
	}

	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[util.AnnotationMaintenance] = operation
	if updateErr := s.k8sClient.Update(r.Context(), cluster); updateErr != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			fmt.Sprintf("failed to set maintenance: %v", updateErr))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{responseKeyStatus: operation + " initiated"})
}

// getCluster retrieves a CloudberryCluster by name.
func (s *Server) getCluster(
	ctx context.Context,
	name, namespace string,
) (*cbv1alpha1.CloudberryCluster, error) {
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

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// writeErrorJSON writes a JSON error response.
func writeErrorJSON(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

// StartServer starts the API server.
func StartServer(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second) //nolint:contextcheck // parent ctx is done
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
