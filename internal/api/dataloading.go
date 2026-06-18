// Package api: dataloading.go holds the data-loading REST endpoints — PXF
// servers CRUD (P.2–P.5), data-loading job CRUD + lifecycle (P.8/P.10–P.13),
// job log streaming (P.14) and the external-tables observed/expected view
// (P.15). All mutations go through updateClusterWithConflictRetry so concurrent
// controller updates never surface as spurious 409→500 to API clients, and the
// honesty invariant is upheld throughout: real side effects only, never
// synthesized data, ABSENT/501 when a fact is not observable.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// errCodeServerNotFound is returned when a named PXF server is absent.
	errCodeServerNotFound = "SERVER_NOT_FOUND"
	// errCodeServerExists is returned when creating a PXF server whose name
	// already exists in the spec.
	errCodeServerExists = "SERVER_EXISTS"
	// errCodeServerInUse is returned when deleting a PXF server still
	// referenced by one or more data-loading jobs (mirrors webhook W.9).
	errCodeServerInUse = "SERVER_IN_USE"
	// errCodeJobNotFound is returned when a named data-loading job is absent.
	errCodeJobNotFound = "JOB_NOT_FOUND"
	// errCodeJobExists is returned when creating a job whose name already
	// exists in the spec.
	errCodeJobExists = "JOB_EXISTS"
	// errCodeJobAlreadyRunning is returned when starting a job whose one-off
	// Job already exists (so the handler does not clobber an in-flight run).
	errCodeJobAlreadyRunning = "JOB_ALREADY_RUNNING"
	// errCodeValidationFailed is returned when a CR mutation is rejected by the
	// validating admission webhook (client input validation error, not a 500).
	errCodeValidationFailed = "VALIDATION_FAILED"
	// errCodeConflict is returned when a CR mutation cannot be persisted because
	// the optimistic-concurrency retry budget was exhausted by concurrent writes.
	errCodeConflict = "CONFLICT"
	// errCodeLogsNotReady is returned when a job pod exists but its main
	// container has not started yet (init/pending) so logs are not available.
	errCodeLogsNotReady = "LOGS_NOT_READY"

	// errCodeDataLoadingNotEnabled is returned when a data-loading endpoint is
	// invoked for a cluster whose data-loading subsystem is disabled/absent
	// (dataLoading.enabled=false). It is the SUBSYSTEM-level gate; it takes
	// precedence over the PXF-specific PXF_NOT_ENABLED gate. Mutating endpoints
	// map it to HTTP 400; the list/get endpoints return a 200 disabled envelope
	// (mirroring the monitoringDisabledMessage precedent).
	errCodeDataLoadingNotEnabled = "DATA_LOADING_NOT_ENABLED"
	// msgDataLoadingNotEnabled is the message returned when data loading is not
	// enabled for a cluster.
	msgDataLoadingNotEnabled = "data loading is not enabled for this cluster"

	// responseKeyServer is the JSON response key carrying a PXF server name.
	responseKeyServer = "server"

	// pxfServerKeySeparator is the "<server>__<file>.xml" key separator used by
	// builder.BuildPXFServersConfigMap; the handler filters rendered keys by the
	// "<server>__" prefix to isolate a single server's rendered files.
	pxfServerKeySeparator = "__"

	// dataLoadTypePXF / dataLoadTypeGpload are the accepted DataLoadingJob.Type
	// values; they mirror the +kubebuilder Enum on the CR field.
	dataLoadTypePXF    = "pxf"
	dataLoadTypeGpload = "gpload"

	// externalTablesKindFDW is the spec-derived "expected" kind label for an
	// FDW-method PXF job (it materializes a persistent foreign table).
	externalTablesKindFDW = "foreign"
	// externalTablesKindExternal is the spec-derived "expected" kind label for
	// an external-table-method PXF job (it materializes a transient external
	// table named after the target table).
	externalTablesKindExternal = "external"
)

// CreatePXFServerRequest is the POST .../data-loading/pxf/servers request body.
// It mirrors cbv1alpha1.PxfServerSpec so the handler maps the payload 1:1 into
// the spec; the admission webhook remains authoritative for deep validation.
type CreatePXFServerRequest struct {
	Name              string                       `json:"name"`
	Type              string                       `json:"type"`
	Config            map[string]string            `json:"config,omitempty"`
	Hive              map[string]string            `json:"hive,omitempty"`
	Hbase             map[string]string            `json:"hbase,omitempty"`
	Jdbc              map[string]string            `json:"jdbc,omitempty"`
	CredentialSecrets []cbv1alpha1.SecretReference `json:"credentialSecrets,omitempty"`
}

// UpdatePXFServerRequest is the PUT .../data-loading/pxf/servers/{server}
// request body. The {server} path value identifies the target. The update is a
// PARTIAL merge onto the existing server (see mergePXFServerUpdate): only the
// fields actually supplied in the request are applied; unspecified fields are
// preserved. This lets the CLI change a single setting (e.g. an endpoint via
// `--endpoint`) without re-sending `--type` or the full config.
type UpdatePXFServerRequest struct {
	Type              string                       `json:"type"`
	Config            map[string]string            `json:"config,omitempty"`
	Hive              map[string]string            `json:"hive,omitempty"`
	Hbase             map[string]string            `json:"hbase,omitempty"`
	Jdbc              map[string]string            `json:"jdbc,omitempty"`
	CredentialSecrets []cbv1alpha1.SecretReference `json:"credentialSecrets,omitempty"`
}

// pxfServerView is the API representation of a PXF server. It deliberately
// OMITS literal secret values: only the credential-secret REFERENCES (name/key)
// are echoed back, never the resolved credentials (which live in Secrets and
// are folded in by an init container, never in ConfigMaps).
type pxfServerView struct {
	Name              string                       `json:"name"`
	Type              string                       `json:"type"`
	Config            map[string]string            `json:"config,omitempty"`
	Hive              map[string]string            `json:"hive,omitempty"`
	Hbase             map[string]string            `json:"hbase,omitempty"`
	Jdbc              map[string]string            `json:"jdbc,omitempty"`
	CredentialSecrets []cbv1alpha1.SecretReference `json:"credentialSecrets,omitempty"`
}

// CreateDataLoadingJobRequest is the POST .../data-loading/jobs request body. It
// mirrors cbv1alpha1.DataLoadingJob so the handler maps it 1:1 into the spec;
// the admission webhook stays authoritative for deep rules.
type CreateDataLoadingJobRequest struct {
	Name      string                    `json:"name"`
	Type      string                    `json:"type"`
	Enabled   bool                      `json:"enabled,omitempty"`
	Schedule  string                    `json:"schedule,omitempty"`
	PxfJob    *cbv1alpha1.PxfJobSpec    `json:"pxfJob,omitempty"`
	GploadJob *cbv1alpha1.GploadJobSpec `json:"gploadJob,omitempty"`
}

// UpdateDataLoadingJobRequest is the PUT .../data-loading/jobs/{job} request
// body. The {job} path value identifies the target; the named job is replaced
// in place.
type UpdateDataLoadingJobRequest struct {
	Type      string                    `json:"type"`
	Enabled   bool                      `json:"enabled,omitempty"`
	Schedule  string                    `json:"schedule,omitempty"`
	PxfJob    *cbv1alpha1.PxfJobSpec    `json:"pxfJob,omitempty"`
	GploadJob *cbv1alpha1.GploadJobSpec `json:"gploadJob,omitempty"`
}

// ExternalTableInfo is the API representation of one observed external/foreign
// table OR one spec-derived expected table. For observed rows it carries the
// live catalog schema/name/kind/server; for expected rows it carries the
// spec-derived job correlation (Job/Profile/Server) and the would-be table
// name. The two are never merged — see ExternalTablesResponse.
type ExternalTableInfo struct {
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Server  string `json:"server,omitempty"`
	Job     string `json:"job,omitempty"`
	Profile string `json:"profile,omitempty"`
}

// ExternalTablesResponse is the GET .../data-loading/external-tables body. It
// splits the live DB-observed catalog tables (Observed, ABSENT/null when the DB
// is unreachable — NEVER fabricated) from the spec-derived would-be tables the
// operator WOULD create (Expected, clearly labeled, never claimed to "exist").
// ObservedAvailable reports whether the live probe succeeded so callers can
// distinguish "observed: none" from "observed: unobservable".
type ExternalTablesResponse struct {
	Cluster           string              `json:"cluster"`
	Observed          []ExternalTableInfo `json:"observed"`
	ObservedAvailable bool                `json:"observedAvailable"`
	Expected          []ExternalTableInfo `json:"expected"`
}

// errServerNotFound / errServerExists are sentinels returned by the conflict-
// retry mutate closures so handlers can map them to 404 / 409 after a race-safe
// re-check on the freshly fetched cluster.
var (
	errServerNotFound = fmt.Errorf("pxf server not found")
	errServerExists   = fmt.Errorf("pxf server already exists")
	errJobNotFound    = fmt.Errorf("data loading job not found")
	errJobExists      = fmt.Errorf("data loading job already exists")
)

// handleListPXFServers lists the spec-defined PXF servers (P.2). It echoes each
// server's name/type/config plus credential-secret REFERENCES, never literal
// secret values.
func (s *Server) handleListPXFServers(w http.ResponseWriter, r *http.Request) {
	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}

	servers := pxfServerViews(cluster.Spec.DataLoading.Pxf.Servers)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyCluster: cluster.Name,
		"servers":          servers,
		responseKeyTotal:   len(servers),
	})
}

// handleGetPXFServer returns a single spec-defined PXF server (P.2). It writes a
// 404 SERVER_NOT_FOUND when the named server is absent.
func (s *Server) handleGetPXFServer(w http.ResponseWriter, r *http.Request) {
	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}
	serverName := r.PathValue("server")
	srv := findPXFServer(cluster.Spec.DataLoading.Pxf.Servers, serverName)
	if srv == nil {
		writeErrorJSON(w, http.StatusNotFound, errCodeServerNotFound,
			fmt.Sprintf("PXF server %q not found", serverName))
		return
	}
	writeJSON(w, http.StatusOK, newPXFServerView(srv))
}

// handleCreatePXFServer appends a new PXF server to the spec (P.3). It maps the
// request 1:1 into a PxfServerSpec, rejects a duplicate name (race-safely inside
// the conflict-retry closure on the freshly fetched cluster), persists via
// updateClusterWithConflictRetry, fires the honest servers-changed signal in
// parity with the controller, and responds 201 with the new server's RENDERED
// "<server>__*.xml" ConfigMap keys.
func (s *Server) handleCreatePXFServer(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}

	var req CreatePXFServerRequest
	if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "server name is required")
		return
	}
	if req.Type == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "server type is required")
		return
	}

	newServer := pxfServerSpecFromCreate(&req)
	prevData := s.renderedServersData(cluster)
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			pxf := pxfSpecOrErr(latest)
			if pxf == nil {
				return errServerNotFound
			}
			if findPXFServer(pxf.Servers, req.Name) != nil {
				return errServerExists
			}
			pxf.Servers = append(pxf.Servers, newServer)
			return nil
		})
	if s.handlePXFServerMutationError(w, cluster, req.Name, updateErr) {
		return
	}

	rendered := s.renderedServerKeys(cluster, req.Name)
	s.recordPXFServersChangedIfDiff(cluster, prevData)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		responseKeyStatus:  statusCreated,
		responseKeyCluster: cluster.Name,
		responseKeyServer:  req.Name,
		"renderedKeys":     rendered,
	})
}

// handleUpdatePXFServer applies a PARTIAL update to a named PXF server in place
// (P.4). It rejects an absent server with 404 SERVER_NOT_FOUND (race-safely
// inside the closure), preserves the server name, and MERGES the request onto
// the existing server (see mergePXFServerUpdate): only fields actually supplied
// are changed; unspecified fields (notably an empty type) are preserved so the
// merged server still passes the validating webhook. It fires the honest
// servers-changed signal and responds 200 with the rendered "<server>__*.xml"
// keys built from the merged cluster.
func (s *Server) handleUpdatePXFServer(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}
	serverName := r.PathValue("server")

	var req UpdatePXFServerRequest
	if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	prevData := s.renderedServersData(cluster)
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			pxf := pxfSpecOrErr(latest)
			if pxf == nil {
				return errServerNotFound
			}
			idx := pxfServerIndex(pxf.Servers, serverName)
			if idx < 0 {
				return errServerNotFound
			}
			// PARTIAL merge onto the freshly fetched server (re-found by name on
			// every retry, never a stale copy) so unspecified fields — most
			// importantly an empty type — are preserved and the webhook accepts it.
			mergePXFServerUpdate(&pxf.Servers[idx], &req)
			return nil
		})
	if s.handlePXFServerMutationError(w, cluster, serverName, updateErr) {
		return
	}

	rendered := s.renderedServerKeys(cluster, serverName)
	s.recordPXFServersChangedIfDiff(cluster, prevData)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusUpdated,
		responseKeyCluster: cluster.Name,
		responseKeyServer:  serverName,
		"renderedKeys":     rendered,
	})
}

// handleDeletePXFServer removes a named PXF server from the spec (P.5). It first
// runs a referential-integrity pre-check mirroring webhook W.9: if any
// dataLoading.jobs[].pxfJob.server references the target it returns 409
// SERVER_IN_USE listing the referencing jobs and performs NO mutation. Otherwise
// it removes the server via conflict-retry (404 if absent) and responds 200.
func (s *Server) handleDeletePXFServer(w http.ResponseWriter, r *http.Request) {
	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}
	serverName := r.PathValue("server")

	if refs := jobsReferencingServer(cluster, serverName); len(refs) > 0 {
		writeErrorJSON(w, http.StatusConflict, errCodeServerInUse,
			fmt.Sprintf("PXF server %q is still referenced by data-loading job(s): %s",
				serverName, strings.Join(refs, ", ")))
		return
	}

	prevData := s.renderedServersData(cluster)
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			pxf := pxfSpecOrErr(latest)
			if pxf == nil {
				return errServerNotFound
			}
			idx := pxfServerIndex(pxf.Servers, serverName)
			if idx < 0 {
				return errServerNotFound
			}
			// Re-check referential integrity on the freshly fetched object so a
			// concurrently added referencing job cannot be orphaned (TOCTOU-safe).
			if refs := jobsReferencingServer(latest, serverName); len(refs) > 0 {
				return errServerInUse{jobs: refs}
			}
			pxf.Servers = append(pxf.Servers[:idx], pxf.Servers[idx+1:]...)
			return nil
		})
	var inUse errServerInUse
	if errors.As(updateErr, &inUse) {
		writeErrorJSON(w, http.StatusConflict, errCodeServerInUse,
			fmt.Sprintf("PXF server %q is still referenced by data-loading job(s): %s",
				serverName, strings.Join(inUse.jobs, ", ")))
		return
	}
	if s.handlePXFServerMutationError(w, cluster, serverName, updateErr) {
		return
	}

	s.recordPXFServersChangedIfDiff(cluster, prevData)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusDeleted,
		responseKeyCluster: cluster.Name,
		responseKeyServer:  serverName,
	})
}

// errServerInUse is the sentinel carrying the referencing job names when a
// server delete loses the TOCTOU race (a referencing job appears concurrently).
type errServerInUse struct{ jobs []string }

func (e errServerInUse) Error() string {
	return fmt.Sprintf("pxf server in use by jobs: %s", strings.Join(e.jobs, ", "))
}

// handlePXFServerMutationError maps the shared server-mutation sentinels to
// their HTTP responses and returns true when it wrote a response (the caller
// must then stop). A nil error returns false (the caller proceeds).
func (s *Server) handlePXFServerMutationError(
	w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster, server string, err error,
) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, errServerNotFound):
		writeErrorJSON(w, http.StatusNotFound, errCodeServerNotFound,
			fmt.Sprintf("PXF server %q not found", server))
		return true
	case errors.Is(err, errServerExists):
		writeErrorJSON(w, http.StatusConflict, errCodeServerExists,
			fmt.Sprintf("PXF server %q already exists", server))
		return true
	default:
		s.logger.Error("failed to mutate PXF server",
			"cluster", cluster.Name, "server", server, "error", err)
		writeClusterMutationError(w, err, "failed to update PXF server")
		return true
	}
}

// writeClusterMutationError classifies a post-mutation error returned by
// updateClusterWithConflictRetry and writes the appropriate HTTP response so
// the mapping is consistent across every data-loading CRUD handler.
//
// Classification (apierrors from k8s.io/apimachinery/pkg/api/errors):
//   - IsInvalid / IsForbidden / IsBadRequest → 400 VALIDATION_FAILED. A
//     validating admission webhook that "denied the request" surfaces through
//     apimachinery as Invalid or Forbidden (and occasionally BadRequest); these
//     are CLIENT input-validation errors, not operator faults. The webhook's
//     reason is carried in err.Error() ("admission webhook ... denied the
//     request: ...") so it is included verbatim in the message.
//   - IsConflict → 409 CONFLICT. The optimistic-concurrency retry budget was
//     exhausted by concurrent writers; the client may simply retry.
//   - otherwise → 500 INTERNAL_ERROR with the caller's fallback message.
func writeClusterMutationError(w http.ResponseWriter, err error, fallbackMsg string) {
	switch {
	case apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsBadRequest(err):
		writeErrorJSON(w, http.StatusBadRequest, errCodeValidationFailed,
			fmt.Sprintf("%s: %s", fallbackMsg, err.Error()))
	case apierrors.IsConflict(err):
		writeErrorJSON(w, http.StatusConflict, errCodeConflict,
			fmt.Sprintf("%s: conflicting concurrent update, please retry", fallbackMsg))
	default:
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, fallbackMsg)
	}
}

// renderedServerKeys re-renders the PXF servers ConfigMap from the updated
// cluster and returns ONLY the named server's "<server>__*.xml" keys, proving
// the rendering happened without dumping every server's XML.
func (s *Server) renderedServerKeys(
	cluster *cbv1alpha1.CloudberryCluster, server string,
) map[string]string {
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	out := make(map[string]string)
	if cm == nil {
		return out
	}
	prefix := server + pxfServerKeySeparator
	for k, v := range cm.Data {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
		}
	}
	return out
}

// recordPXFServersChangedIfDiff renders the post-mutation ConfigMap and, when
// its Data really differs from the pre-mutation Data, reuses the Scenario 106
// honest servers-changed signal (recordPXFServersChanged) so the metric/log
// stays consistent with the controller and explicit-sync paths. The operator
// reconcile remains responsible for actually materializing the ConfigMap.
func (s *Server) recordPXFServersChangedIfDiff(
	cluster *cbv1alpha1.CloudberryCluster, prevData map[string]string,
) {
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	if cm == nil {
		return
	}
	if !pxfServersDataEqual(prevData, cm.Data) {
		s.recordPXFServersChanged(cluster, prevData, cm.Data)
	}
}

// handleCreateDataLoadingJob appends a new data-loading job to the spec (P.8).
// It validates minimally (name required + DNS-1123, type pxf|gpload, referenced
// pxf server must exist mirroring W.9) and leaves deeper rules to the admission
// webhook. A duplicate job name yields 409 JOB_EXISTS; success yields 201 with
// the created job.
func (s *Server) handleCreateDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	var req CreateDataLoadingJobRequest
	if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}
	if !s.validateCreateJobShape(w, cluster, &req) {
		return
	}

	newJob := dataLoadingJobFromCreate(&req)
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.DataLoading == nil {
				latest.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{}
			}
			if findDataLoadingJob(latest.Spec.DataLoading.Jobs, req.Name) != nil {
				return errJobExists
			}
			latest.Spec.DataLoading.Jobs = append(latest.Spec.DataLoading.Jobs, newJob)
			return nil
		})
	if s.handleJobMutationError(w, cluster, req.Name, updateErr) {
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		responseKeyStatus:  statusCreated,
		responseKeyCluster: cluster.Name,
		responseKeyJob:     newJob,
	})
}

// validateCreateJobShape runs the shallow create-job validation (name/type/
// referenced-server). It writes the error response and returns false on
// rejection; the admission webhook remains authoritative for deeper rules.
func (s *Server) validateCreateJobShape(
	w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster, req *CreateDataLoadingJobRequest,
) bool {
	if req.Name == "" || !isValidDNS1123Name(req.Name) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid job name %q (must be a non-empty DNS-1123 name)", req.Name))
		return false
	}
	if req.Type != dataLoadTypePXF && req.Type != dataLoadTypeGpload {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid job type %q; valid types: pxf, gpload", req.Type))
		return false
	}
	// Mirror W.9: a referenced pxf server must already exist in the spec.
	if req.Type == dataLoadTypePXF && req.PxfJob != nil && req.PxfJob.Server != "" {
		if !pxfServerDefined(cluster, req.PxfJob.Server) {
			writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("pxfJob.server %q does not reference a defined pxf.servers[].name",
					req.PxfJob.Server))
			return false
		}
	}
	return true
}

// handleUpdateDataLoadingJob replaces a named data-loading job in place (P.10).
// It returns 404 JOB_NOT_FOUND when the job is absent (race-safely inside the
// closure) and 200 on success.
func (s *Server) handleUpdateDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	jobName := r.PathValue("job")
	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	var req UpdateDataLoadingJobRequest
	if decErr := json.NewDecoder(r.Body).Decode(&req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	updated := dataLoadingJobFromUpdate(jobName, &req)
	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.DataLoading == nil {
				return errJobNotFound
			}
			idx := dataLoadingJobIndex(latest.Spec.DataLoading.Jobs, jobName)
			if idx < 0 {
				return errJobNotFound
			}
			latest.Spec.DataLoading.Jobs[idx] = updated
			return nil
		})
	if s.handleJobMutationError(w, cluster, jobName, updateErr) {
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusUpdated,
		responseKeyCluster: cluster.Name,
		responseKeyJob:     updated,
	})
}

// handleDeleteDataLoadingJob removes a named data-loading job from the spec
// (P.11). It returns 404 JOB_NOT_FOUND when absent, removes via conflict-retry,
// best-effort deletes any spawned Job/CronJob, and responds 200.
func (s *Server) handleDeleteDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	jobName := r.PathValue("job")
	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	updateErr := s.updateClusterWithConflictRetry(r.Context(), cluster,
		func(latest *cbv1alpha1.CloudberryCluster) error {
			if latest.Spec.DataLoading == nil {
				return errJobNotFound
			}
			idx := dataLoadingJobIndex(latest.Spec.DataLoading.Jobs, jobName)
			if idx < 0 {
				return errJobNotFound
			}
			latest.Spec.DataLoading.Jobs = append(
				latest.Spec.DataLoading.Jobs[:idx], latest.Spec.DataLoading.Jobs[idx+1:]...)
			return nil
		})
	if s.handleJobMutationError(w, cluster, jobName, updateErr) {
		return
	}

	// Best-effort: delete any spawned Job/CronJob so the workload stops promptly
	// (reconcile garbage-collects via ownerRef too; this is belt-and-suspenders).
	s.bestEffortDeleteDataLoadWorkloads(r.Context(), cluster, jobName)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		responseKeyStatus:  statusDeleted,
		responseKeyCluster: cluster.Name,
		responseKeyJob:     jobName,
	})
}

// handleJobMutationError maps the shared job-mutation sentinels to their HTTP
// responses and returns true when it wrote one (the caller must then stop).
func (s *Server) handleJobMutationError(
	w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster, job string, err error,
) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, errJobNotFound):
		writeErrorJSON(w, http.StatusNotFound, errCodeJobNotFound,
			fmt.Sprintf("data loading job %q not found", job))
		return true
	case errors.Is(err, errJobExists):
		writeErrorJSON(w, http.StatusConflict, errCodeJobExists,
			fmt.Sprintf("data loading job %q already exists", job))
		return true
	default:
		s.logger.Error("failed to mutate data loading job",
			"cluster", cluster.Name, "job", job, "error", err)
		writeClusterMutationError(w, err, "failed to update data loading job")
		return true
	}
}

// handleStartDataLoadingJob triggers a REAL one-off run of a named job (P.12).
// The job must exist in the spec (404 else). It creates the data-loading Job via
// the builder under the deterministic util.DataLoadJobName; if such a Job already
// exists it returns 409 JOB_ALREADY_RUNNING rather than clobbering the in-flight
// run. Success yields 202 Accepted with the created Job name.
//
// HONESTY: the cloudberry_data_loading_rows_total metric is intentionally NOT
// recorded here — there is no rows-loaded signal at start time; that count is
// harvested at Job COMPLETION by the controller. Recording here would fabricate
// data.
func (s *Server) handleStartDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	jobName := r.PathValue("job")
	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	jobSpec := findDataLoadingJob(dataLoadingJobs(cluster), jobName)
	if jobSpec == nil {
		writeErrorJSON(w, http.StatusNotFound, errCodeJobNotFound,
			fmt.Sprintf("data loading job %q not found", jobName))
		return
	}

	k8sJobName := util.DataLoadJobName(cluster.Name, jobName)
	existing := &batchv1.Job{}
	getErr := s.k8sClient.Get(r.Context(),
		client.ObjectKey{Name: k8sJobName, Namespace: cluster.Namespace}, existing)
	switch {
	case getErr == nil:
		writeErrorJSON(w, http.StatusConflict, errCodeJobAlreadyRunning,
			fmt.Sprintf("data loading Job %q already exists; stop it before starting a new run",
				k8sJobName))
		return
	case !apierrors.IsNotFound(getErr):
		s.logger.Error("failed to check existing data loading Job",
			"cluster", cluster.Name, "job", jobName, "error", getErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to check existing data loading job")
		return
	default:
		// Not found — proceed to create the one-off run below.
	}

	job := s.builder.BuildDataLoadJob(cluster, *jobSpec)
	if job == nil {
		s.logger.Error("builder returned no data loading job",
			"cluster", cluster.Name, "job", jobName)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to build data loading job")
		return
	}
	if createErr := s.k8sClient.Create(r.Context(), job); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			writeErrorJSON(w, http.StatusConflict, errCodeJobAlreadyRunning,
				fmt.Sprintf("data loading Job %q already exists", k8sJobName))
			return
		}
		s.logger.Error("failed to create data loading Job",
			"cluster", cluster.Name, "job", jobName, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to create data loading job")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:  statusCreated,
		responseKeyCluster: cluster.Name,
		responseKeyJob:     jobName,
		"k8sJob":           job.Name,
	})
}

// handleStopDataLoadingJob halts a running/continuous data-loading job (P.13).
// It deletes the running one-off Job (Background propagation so its pods are
// reaped) and suspends the scheduled CronJob when present, both named
// util.DataLoadJobName. It is idempotent and HONEST: when no Job exists it
// responds 200 with a "nothing to stop" message rather than fabricating a stop.
func (s *Server) handleStopDataLoadingJob(w http.ResponseWriter, r *http.Request) {
	jobName := r.PathValue("job")
	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	k8sName := util.DataLoadJobName(cluster.Name, jobName)
	stopped, suspended, opErr := s.stopDataLoadWorkloads(r.Context(), cluster, k8sName)
	if opErr != nil {
		s.logger.Error("failed to stop data loading job",
			"cluster", cluster.Name, "job", jobName, "error", opErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to stop data loading job")
		return
	}

	if !stopped && !suspended {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			responseKeyStatus:  "noop",
			responseKeyCluster: cluster.Name,
			responseKeyJob:     jobName,
			"stopped":          false,
			responseKeyMessage: "nothing to stop: no running data loading Job or CronJob",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:  statusDeleted,
		responseKeyCluster: cluster.Name,
		responseKeyJob:     jobName,
		"stopped":          stopped,
		"suspended":        suspended,
	})
}

// handleDataLoadingJobLogs streams the container logs of the pod backing a
// data-loading Job (P.14). It mirrors handleBackupJobLogs: 501 LOGS_NOT_AVAILABLE
// when no clientset is configured, resolves the cluster, locates the pod via the
// k8s Job name (util.DataLoadJobName, NOT the bare spec job name), and streams.
// Supports ?follow and ?tailLines.
func (s *Server) handleDataLoadingJobLogs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	jobName := r.PathValue("job")

	if !isValidDNS1123Name(jobName) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid job name %q", jobName))
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
	if !dataLoadingEnabled(cluster) {
		writeErrorJSON(w, http.StatusBadRequest,
			errCodeDataLoadingNotEnabled, msgDataLoadingNotEnabled)
		return
	}

	k8sJobName := util.DataLoadJobName(cluster.Name, jobName)
	podName, found, listErr := s.findJobPod(r.Context(), cluster.Namespace, k8sJobName)
	if listErr != nil {
		s.logger.Error("failed to list data loading job pods",
			"cluster", name, "job", jobName, "error", listErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal, "failed to list job pods")
		return
	}
	if !found {
		writeErrorJSON(w, http.StatusNotFound, errCodeJobNotFound,
			fmt.Sprintf("no pods found for data loading job %q", jobName))
		return
	}

	// Readiness pre-check (P.14): when the data-loading Job pod is still in Init
	// (the health-check init container is running, main container not started),
	// the kubelet log request fails with a transient "container ... is waiting to
	// start" / ContainerCreating / PodInitializing error. Surface that EXPECTED
	// state as 409 LOGS_NOT_READY rather than a misleading 500. This pre-check is
	// data-loading-only so the shared streamPodLogs (used by backups) is
	// untouched and backup behavior is preserved.
	if notReady := s.podLogsNotReadyReason(r.Context(), cluster.Namespace, podName); notReady != "" {
		writeErrorJSON(w, http.StatusConflict, errCodeLogsNotReady,
			fmt.Sprintf("job pod is not running yet (%s); logs not available yet", notReady))
		return
	}

	s.streamPodLogs(w, r, cluster.Namespace, podName, k8sJobName)
}

// podLogsNotReadyReason fetches the located pod and reports a short human
// reason when its MAIN container has not started yet — i.e. the pod is Pending,
// an init container is still running/waiting, or a regular container is waiting
// with a not-yet-started reason (ContainerCreating / PodInitializing). It
// returns "" when logs should be streamable (the container is or has been
// running, or the pod status is not observable — in which case streamPodLogs
// still does the authoritative attempt and maps any genuine error to 500).
func (s *Server) podLogsNotReadyReason(ctx context.Context, namespace, podName string) string {
	pod := &corev1.Pod{}
	if getErr := s.k8sClient.Get(ctx,
		types.NamespacedName{Name: podName, Namespace: namespace}, pod); getErr != nil {
		// Not observable here — let streamPodLogs make the authoritative attempt.
		return ""
	}
	if pod.Status.Phase == corev1.PodPending {
		return "init/pending"
	}
	if reason := initContainersNotReady(pod.Status.InitContainerStatuses); reason != "" {
		return reason
	}
	return mainContainersNotReady(pod.Status.ContainerStatuses)
}

// initContainersNotReady returns a short reason when any init container has not
// finished (still running or waiting) — the main container cannot have started.
func initContainersNotReady(statuses []corev1.ContainerStatus) string {
	for i := range statuses {
		if statuses[i].State.Terminated == nil {
			return "init containers still running"
		}
	}
	return ""
}

// mainContainersNotReady returns a short reason when a main container is waiting
// with a not-yet-started reason (ContainerCreating / PodInitializing). A waiting
// container with any OTHER reason (e.g. CrashLoopBackOff) HAS started before, so
// logs are available and we return "" to let the stream proceed.
func mainContainersNotReady(statuses []corev1.ContainerStatus) string {
	for i := range statuses {
		w := statuses[i].State.Waiting
		if w == nil {
			continue
		}
		if isNotStartedWaitReason(w.Reason) {
			return "container waiting to start"
		}
	}
	return ""
}

// isNotStartedWaitReason reports whether a container Waiting.Reason indicates the
// container has never started yet (so no logs exist), matching the kubelet error
// surface "waiting to start" / ContainerCreating / PodInitializing.
func isNotStartedWaitReason(reason string) bool {
	switch reason {
	case "ContainerCreating", "PodInitializing":
		return true
	default:
		return false
	}
}

// handleListExternalTables returns the observed-vs-expected external/foreign
// table view (P.15). Observed is the LIVE DB catalog result (null + available
// false when the DB is unreachable — NEVER fabricated); Expected is the
// spec-derived set of tables the operator WOULD create for each pxf job, clearly
// labeled as expected and never claimed to exist.
func (s *Server) handleListExternalTables(w http.ResponseWriter, r *http.Request) {
	cluster, ok := s.getDataLoadingCluster(w, r)
	if !ok {
		return
	}

	observed, available := s.observeExternalTables(r.Context(), cluster)
	resp := ExternalTablesResponse{
		Cluster:           cluster.Name,
		Observed:          observed,
		ObservedAvailable: available,
		Expected:          expectedExternalTables(cluster),
	}
	writeJSON(w, http.StatusOK, resp)
}

// TestReadSource is the resolved PXF source (server/profile/resource) echoed in
// the test-read response so the caller sees exactly which source was sampled
// (handy when ?job= resolved it indirectly).
type TestReadSource struct {
	Server   string `json:"server,omitempty"`
	Profile  string `json:"profile"`
	Resource string `json:"resource"`
}

// TestReadResponse is the GET .../data-loading/test-read body (L.15). It is
// HONEST: Available is true ONLY when the live read succeeded, in which case
// Columns/Rows carry the REAL sampled rows. When the DB/source is unreachable
// Available is false and Rows is null (never fabricated, never a 500). Limit
// echoes the effective (clamped) row cap.
type TestReadResponse struct {
	Cluster   string         `json:"cluster"`
	Source    TestReadSource `json:"source"`
	Limit     int            `json:"limit"`
	Available bool           `json:"available"`
	RowCount  int            `json:"rowCount"`
	Columns   []string       `json:"columns,omitempty"`
	Rows      [][]string     `json:"rows"`
}

const (
	// testReadDefaultLimit is the default preview row count when ?limit= is
	// omitted (LOCKED decision: default 10).
	testReadDefaultLimit = 10
	// testReadMaxLimit is the hard cap on ?limit=; larger values are clamped down
	// (LOCKED decision: hard cap 1000, round-down above).
	testReadMaxLimit = 1000
)

// handleTestReadPXFSource reads up to ?limit rows from a PXF source and returns
// them HONESTLY (L.15). The source is selected by ?job=<job> (resolved to that
// job's pxfJob.{server,profile,resource}) — the primary path — or by explicit
// ?server=&profile=&resource=. PermissionBasic, read-only (NO metric recorded:
// a sample read loads nothing). When the DB/source is unreachable it responds
// 200 {available:false, rows:null} — NEVER 500 for mere unreachability, NEVER
// fabricated rows. 400 on missing/invalid params; 404 on an unknown ?job=.
func (s *Server) handleTestReadPXFSource(w http.ResponseWriter, r *http.Request) {
	cluster, ok := s.getPXFCluster(w, r)
	if !ok {
		return
	}

	source, ok := s.resolveTestReadSource(w, cluster, r)
	if !ok {
		return
	}

	limit, ok := parseTestReadLimit(w, r.URL.Query().Get("limit"))
	if !ok {
		return
	}

	sample, available := s.samplePXFSource(r.Context(), cluster, source, limit)
	resp := TestReadResponse{
		Cluster:   cluster.Name,
		Source:    source,
		Limit:     limit,
		Available: available,
	}
	if available && sample != nil {
		resp.Columns = sample.Columns
		resp.Rows = sample.Rows
		resp.RowCount = len(sample.Rows)
	}
	writeJSON(w, http.StatusOK, resp)
}

// resolveTestReadSource resolves the PXF source from the request: ?job=<job>
// (primary — resolved to the job's pxfJob source) takes precedence; otherwise
// the explicit ?server=&profile=&resource= triple is used. It writes the error
// response and returns ok=false on an unknown job (404) or a missing
// profile/resource (400).
func (s *Server) resolveTestReadSource(
	w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster, r *http.Request,
) (TestReadSource, bool) {
	q := r.URL.Query()
	if jobName := q.Get("job"); jobName != "" {
		job := findDataLoadingJob(dataLoadingJobs(cluster), jobName)
		if job == nil {
			writeErrorJSON(w, http.StatusNotFound, errCodeJobNotFound,
				fmt.Sprintf("data loading job %q not found", jobName))
			return TestReadSource{}, false
		}
		if job.Type != dataLoadTypePXF || job.PxfJob == nil {
			writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("data loading job %q is not a PXF job; test-read requires a pxf source", jobName))
			return TestReadSource{}, false
		}
		return TestReadSource{
			Server:   job.PxfJob.Server,
			Profile:  job.PxfJob.Profile,
			Resource: job.PxfJob.Resource,
		}, true
	}

	source := TestReadSource{
		Server:   q.Get(responseKeyServer),
		Profile:  q.Get("profile"),
		Resource: q.Get("resource"),
	}
	if source.Profile == "" || source.Resource == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"either ?job=<job> or both ?profile= and ?resource= are required")
		return TestReadSource{}, false
	}
	return source, true
}

// parseTestReadLimit parses the ?limit param into [1, testReadMaxLimit],
// defaulting an empty value to testReadDefaultLimit and clamping (rounding down)
// anything above the cap. A non-numeric or non-positive value is a 400.
func parseTestReadLimit(w http.ResponseWriter, raw string) (int, bool) {
	if raw == "" {
		return testReadDefaultLimit, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("invalid limit %q: must be a positive integer", raw))
		return 0, false
	}
	if limit > testReadMaxLimit {
		limit = testReadMaxLimit
	}
	return limit, true
}

// samplePXFSource runs the live, transient PXF preview read via the db factory.
// It returns (sample, true) on a successful read and (nil, false) when there is
// no factory, a connection failure, or a read error — the HONEST ABSENT signal
// (never a synthesized result). It mirrors observeExternalTables's honesty
// contract so an unreachable source is reported as available:false, not as an
// error/500.
func (s *Server) samplePXFSource(
	ctx context.Context, cluster *cbv1alpha1.CloudberryCluster, source TestReadSource, limit int,
) (*db.PXFSourceSample, bool) {
	if s.dbFactory == nil {
		return nil, false
	}
	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Warn("test-read: failed to connect to database (source ABSENT)",
			"cluster", cluster.Name, "error", dbErr)
		return nil, false
	}
	defer dbClient.Close()

	sample, readErr := dbClient.ReadPXFSourceSample(
		ctx, source.Server, source.Profile, source.Resource, limit)
	if readErr != nil {
		s.logger.Warn("test-read: PXF source read failed (source ABSENT)",
			"cluster", cluster.Name, "server", source.Server,
			"profile", source.Profile, "resource", source.Resource, "error", readErr)
		return nil, false
	}
	return sample, true
}

// observeExternalTables runs the live, observed-only external/foreign table
// probe via the db factory. It returns (rows, true) on a successful probe and
// (nil, false) when there is no factory, a connection failure, or a query error
// — the HONEST ABSENT signal (never a synthesized "none"). The bool lets the
// handler distinguish "observed: none" from "observed: unobservable".
func (s *Server) observeExternalTables(
	ctx context.Context, cluster *cbv1alpha1.CloudberryCluster,
) ([]ExternalTableInfo, bool) {
	if s.dbFactory == nil {
		return nil, false
	}
	dbClient, dbErr := s.dbFactory.NewClient(ctx, cluster)
	if dbErr != nil {
		s.logger.Warn("external-tables: failed to connect to database (observed ABSENT)",
			"cluster", cluster.Name, "error", dbErr)
		return nil, false
	}
	defer dbClient.Close()

	rows, listErr := dbClient.ListExternalTables(ctx)
	if listErr != nil {
		s.logger.Warn("external-tables: catalog probe failed (observed ABSENT)",
			"cluster", cluster.Name, "error", listErr)
		return nil, false
	}
	return externalTableViews(rows), true
}

// bestEffortDeleteDataLoadWorkloads deletes the one-off Job and scheduled
// CronJob spawned for a data-loading job, ignoring NotFound. It is best-effort:
// the reconcile garbage-collects via ownerRefs anyway, so a delete failure is
// logged at warn and does NOT fail the spec-deletion that already succeeded.
func (s *Server) bestEffortDeleteDataLoadWorkloads(
	ctx context.Context, cluster *cbv1alpha1.CloudberryCluster, jobName string,
) {
	k8sName := util.DataLoadJobName(cluster.Name, jobName)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: cluster.Namespace}}
	if delErr := s.k8sClient.Delete(ctx, job, backgroundDeletion()); delErr != nil &&
		!apierrors.IsNotFound(delErr) {
		s.logger.Warn("best-effort delete of data loading Job failed",
			"cluster", cluster.Name, "job", jobName, "error", delErr)
	}
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: cluster.Namespace}}
	if delErr := s.k8sClient.Delete(ctx, cronJob, backgroundDeletion()); delErr != nil &&
		!apierrors.IsNotFound(delErr) {
		s.logger.Warn("best-effort delete of data loading CronJob failed",
			"cluster", cluster.Name, "job", jobName, "error", delErr)
	}
}

// stopDataLoadWorkloads deletes the running one-off Job (Background propagation
// so its pods are reaped) and suspends the scheduled CronJob, both named k8sName.
// It returns whether a Job was deleted, whether a CronJob was suspended, and the
// first hard error (NotFound is treated as "absent", not an error — honest
// idempotency).
func (s *Server) stopDataLoadWorkloads(
	ctx context.Context, cluster *cbv1alpha1.CloudberryCluster, k8sName string,
) (stopped, suspended bool, err error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: cluster.Namespace}}
	delErr := s.k8sClient.Delete(ctx, job, backgroundDeletion())
	switch {
	case delErr == nil:
		stopped = true
	case !apierrors.IsNotFound(delErr):
		return false, false, fmt.Errorf("deleting data loading Job %s: %w", k8sName, delErr)
	}

	suspended, err = s.suspendDataLoadCronJob(ctx, cluster, k8sName)
	if err != nil {
		return stopped, false, err
	}
	return stopped, suspended, nil
}

// suspendDataLoadCronJob suspends the named data-loading CronJob in place. It
// returns false/nil when the CronJob is absent or already suspended, and true
// when it flipped a running CronJob to suspended.
func (s *Server) suspendDataLoadCronJob(
	ctx context.Context, cluster *cbv1alpha1.CloudberryCluster, k8sName string,
) (bool, error) {
	cronJob := &batchv1.CronJob{}
	getErr := s.k8sClient.Get(ctx,
		types.NamespacedName{Name: k8sName, Namespace: cluster.Namespace}, cronJob)
	if apierrors.IsNotFound(getErr) {
		return false, nil
	}
	if getErr != nil {
		return false, fmt.Errorf("getting data loading CronJob %s: %w", k8sName, getErr)
	}
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		return false, nil
	}
	suspend := true
	cronJob.Spec.Suspend = &suspend
	if updateErr := s.k8sClient.Update(ctx, cronJob); updateErr != nil {
		return false, fmt.Errorf("suspending data loading CronJob %s: %w", k8sName, updateErr)
	}
	return true, nil
}

// backgroundDeletion returns the delete option requesting Background cascade
// propagation so a deleted Job's pods are reaped by the garbage collector.
func backgroundDeletion() client.DeleteOption {
	policy := metav1.DeletePropagationBackground
	return &client.DeleteOptions{PropagationPolicy: &policy}
}

// --- helpers -------------------------------------------------------------

// pxfSpecOrErr returns the cluster's PxfSpec when PXF is configured, or nil.
func pxfSpecOrErr(cluster *cbv1alpha1.CloudberryCluster) *cbv1alpha1.PxfSpec {
	if cluster.Spec.DataLoading == nil || cluster.Spec.DataLoading.Pxf == nil {
		return nil
	}
	return cluster.Spec.DataLoading.Pxf
}

// pxfServers returns the spec-defined PXF servers (nil when PXF is unconfigured).
func pxfServers(cluster *cbv1alpha1.CloudberryCluster) []cbv1alpha1.PxfServerSpec {
	if pxf := pxfSpecOrErr(cluster); pxf != nil {
		return pxf.Servers
	}
	return nil
}

// dataLoadingJobs returns the spec-defined jobs (nil when unconfigured).
func dataLoadingJobs(cluster *cbv1alpha1.CloudberryCluster) []cbv1alpha1.DataLoadingJob {
	if cluster.Spec.DataLoading == nil {
		return nil
	}
	return cluster.Spec.DataLoading.Jobs
}

// dataLoadingEnabled reports whether the data-loading subsystem is enabled for a
// cluster (dataLoading present AND enabled). It is the SUBSYSTEM-level gate that
// fronts every data-loading endpoint; it is broader than (and takes precedence
// over) the PXF-specific pxfEnabled gate.
func dataLoadingEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	dl := cluster.Spec.DataLoading
	return dl != nil && dl.Enabled
}

// getDataLoadingCluster resolves the cluster and enforces the data-loading
// subsystem-enabled gate shared by the MUTATING data-loading endpoints. On the
// not-found path it writes a 404; on the disabled path a 400
// DATA_LOADING_NOT_ENABLED envelope, returning ok=false so the caller stops.
// It mirrors getPXFCluster, one subsystem up.
func (s *Server) getDataLoadingCluster(
	w http.ResponseWriter, r *http.Request,
) (*cbv1alpha1.CloudberryCluster, bool) {
	name := r.PathValue("name")
	cluster, err := s.getCluster(r.Context(), name, r.URL.Query().Get(responseKeyNamespace))
	if err != nil {
		writeClusterNotFound(w, name)
		return nil, false
	}
	if !dataLoadingEnabled(cluster) {
		writeErrorJSON(w, http.StatusBadRequest,
			errCodeDataLoadingNotEnabled, msgDataLoadingNotEnabled)
		return nil, false
	}
	return cluster, true
}

// writeDataLoadingDisabled writes a 200 disabled envelope for the data-loading
// LIST/GET endpoints (mirrors writeMonitoringDisabled), so a read against a
// disabled subsystem is honest without being an error.
func writeDataLoadingDisabled(w http.ResponseWriter, cluster *cbv1alpha1.CloudberryCluster) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"dataLoadingEnabled": false,
		responseKeyCluster:   cluster.Name,
		responseKeyMessage:   msgDataLoadingNotEnabled,
	})
}

// findPXFServer returns a pointer to the named server, or nil when absent.
func findPXFServer(servers []cbv1alpha1.PxfServerSpec, name string) *cbv1alpha1.PxfServerSpec {
	if idx := pxfServerIndex(servers, name); idx >= 0 {
		return &servers[idx]
	}
	return nil
}

// pxfServerIndex returns the index of the named server, or -1 when absent.
func pxfServerIndex(servers []cbv1alpha1.PxfServerSpec, name string) int {
	for i := range servers {
		if servers[i].Name == name {
			return i
		}
	}
	return -1
}

// pxfServerDefined reports whether the named PXF server exists in the spec.
func pxfServerDefined(cluster *cbv1alpha1.CloudberryCluster, name string) bool {
	return findPXFServer(pxfServers(cluster), name) != nil
}

// findDataLoadingJob returns a pointer to the named job, or nil when absent.
func findDataLoadingJob(jobs []cbv1alpha1.DataLoadingJob, name string) *cbv1alpha1.DataLoadingJob {
	if idx := dataLoadingJobIndex(jobs, name); idx >= 0 {
		return &jobs[idx]
	}
	return nil
}

// dataLoadingJobIndex returns the index of the named job, or -1 when absent.
func dataLoadingJobIndex(jobs []cbv1alpha1.DataLoadingJob, name string) int {
	for i := range jobs {
		if jobs[i].Name == name {
			return i
		}
	}
	return -1
}

// jobsReferencingServer returns the names of data-loading jobs whose
// pxfJob.server references the given server (mirrors webhook W.9's inverse).
func jobsReferencingServer(cluster *cbv1alpha1.CloudberryCluster, server string) []string {
	var refs []string
	for i := range dataLoadingJobs(cluster) {
		job := &cluster.Spec.DataLoading.Jobs[i]
		if job.PxfJob != nil && job.PxfJob.Server == server {
			refs = append(refs, job.Name)
		}
	}
	return refs
}

// pxfServerSpecFromCreate maps a create request to a PxfServerSpec.
func pxfServerSpecFromCreate(req *CreatePXFServerRequest) cbv1alpha1.PxfServerSpec {
	return cbv1alpha1.PxfServerSpec{
		Name:              req.Name,
		Type:              req.Type,
		Config:            req.Config,
		Hive:              req.Hive,
		Hbase:             req.Hbase,
		Jdbc:              req.Jdbc,
		CredentialSecrets: req.CredentialSecrets,
	}
}

// mergePXFServerUpdate applies a PARTIAL update request onto the existing PXF
// server in place, preserving every field the request does not supply:
//   - Type: kept when the request type is empty; replaced when provided (so the
//     type CAN be changed, but omitting it — e.g. when changing only an endpoint
//     — preserves it, keeping the merged server valid for the webhook).
//   - Config/Hive/Hbase/Jdbc: each request map is MERGED key-by-key over the
//     existing map (request keys add/override; keys absent from the request are
//     preserved). An empty/nil request map leaves the existing map untouched, so
//     `--endpoint` alone changes just that key and keeps the rest.
//   - CredentialSecrets: replaced only when the request supplies a non-empty
//     list; otherwise the existing references are kept.
//
// The server name is immutable and never altered here.
func mergePXFServerUpdate(srv *cbv1alpha1.PxfServerSpec, req *UpdatePXFServerRequest) {
	if req.Type != "" {
		srv.Type = req.Type
	}
	srv.Config = mergeStringMap(srv.Config, req.Config)
	srv.Hive = mergeStringMap(srv.Hive, req.Hive)
	srv.Hbase = mergeStringMap(srv.Hbase, req.Hbase)
	srv.Jdbc = mergeStringMap(srv.Jdbc, req.Jdbc)
	if len(req.CredentialSecrets) > 0 {
		srv.CredentialSecrets = req.CredentialSecrets
	}
}

// mergeStringMap returns base with every key from overlay applied on top
// (overlay keys add or override; keys only in base are preserved). When overlay
// is empty base is returned unchanged. When base is nil but overlay is non-empty
// a fresh map is allocated so the caller never aliases the request's map.
func mergeStringMap(base, overlay map[string]string) map[string]string {
	if len(overlay) == 0 {
		return base
	}
	if base == nil {
		base = make(map[string]string, len(overlay))
	}
	for k, v := range overlay {
		base[k] = v
	}
	return base
}

// dataLoadingJobFromCreate maps a create request to a DataLoadingJob.
func dataLoadingJobFromCreate(req *CreateDataLoadingJobRequest) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:      req.Name,
		Type:      req.Type,
		Enabled:   req.Enabled,
		Schedule:  req.Schedule,
		PxfJob:    req.PxfJob,
		GploadJob: req.GploadJob,
	}
}

// dataLoadingJobFromUpdate maps an update request to a DataLoadingJob,
// preserving the immutable job name from the path.
func dataLoadingJobFromUpdate(name string, req *UpdateDataLoadingJobRequest) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:      name,
		Type:      req.Type,
		Enabled:   req.Enabled,
		Schedule:  req.Schedule,
		PxfJob:    req.PxfJob,
		GploadJob: req.GploadJob,
	}
}

// pxfServerViews maps spec servers to API views (secret values omitted).
func pxfServerViews(servers []cbv1alpha1.PxfServerSpec) []pxfServerView {
	out := make([]pxfServerView, 0, len(servers))
	for i := range servers {
		out = append(out, newPXFServerView(&servers[i]))
	}
	return out
}

// newPXFServerView builds one API view from a spec server. Credential secrets
// are echoed as REFERENCES only — literal secret values are never resolved here.
func newPXFServerView(srv *cbv1alpha1.PxfServerSpec) pxfServerView {
	return pxfServerView{
		Name:              srv.Name,
		Type:              srv.Type,
		Config:            srv.Config,
		Hive:              srv.Hive,
		Hbase:             srv.Hbase,
		Jdbc:              srv.Jdbc,
		CredentialSecrets: srv.CredentialSecrets,
	}
}

// externalTableViews maps DB-observed rows to the API representation.
func externalTableViews(rows []db.ExternalTableInfo) []ExternalTableInfo {
	out := make([]ExternalTableInfo, 0, len(rows))
	for i := range rows {
		out = append(out, ExternalTableInfo{
			Schema: rows[i].Schema,
			Name:   rows[i].Name,
			Kind:   rows[i].Kind,
			Server: rows[i].Server,
		})
	}
	return out
}

// expectedExternalTables derives the spec-labeled "expected" tables the operator
// WOULD create for each pxf job: an FDW job materializes a persistent foreign
// table named builder.ForeignTableName(job); an external-table job materializes
// an (transient) external table named after its target table. These are NOT
// observed facts — they are the operator's intent, labeled as such. The result
// is sorted for determinism.
func expectedExternalTables(cluster *cbv1alpha1.CloudberryCluster) []ExternalTableInfo {
	jobs := dataLoadingJobs(cluster)
	out := make([]ExternalTableInfo, 0, len(jobs))
	for i := range jobs {
		job := &jobs[i]
		if job.Type != dataLoadTypePXF || job.PxfJob == nil {
			continue
		}
		out = append(out, expectedTableForPXFJob(job))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Job != out[j].Job {
			return out[i].Job < out[j].Job
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// expectedTableForPXFJob derives the single expected table descriptor for a pxf
// job, branching on its load method (fdw → persistent foreign table; otherwise →
// external table named after the target table).
func expectedTableForPXFJob(job *cbv1alpha1.DataLoadingJob) ExternalTableInfo {
	info := ExternalTableInfo{
		Job:     job.Name,
		Server:  job.PxfJob.Server,
		Profile: job.PxfJob.Profile,
	}
	if strings.EqualFold(job.PxfJob.LoadMethod, "fdw") {
		info.Kind = externalTablesKindFDW
		info.Name = builder.ForeignTableName(job.Name)
		return info
	}
	info.Kind = externalTablesKindExternal
	info.Name = job.PxfJob.TargetTable
	return info
}

// renderedServersData returns the rendered PXF servers ConfigMap Data for the
// cluster, or nil when the builder produces no ConfigMap (PXF off / no image).
// It is the pre-mutation snapshot source for the honest servers-changed diff.
func (s *Server) renderedServersData(cluster *cbv1alpha1.CloudberryCluster) map[string]string {
	if cm := s.builder.BuildPXFServersConfigMap(cluster); cm != nil {
		return cm.Data
	}
	return nil
}

// pxfServersDataEqual reports whether two rendered ConfigMap Data maps are
// byte-identical (used to gate the honest servers-changed signal on a real
// diff). Two nil/empty maps are equal.
func pxfServersDataEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
