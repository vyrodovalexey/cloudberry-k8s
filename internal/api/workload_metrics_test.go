package api

// TASK 6 (W2-B4): verify the workload-operations counter
// (cloudberry_api_workload_operations_total) is recorded exactly once per
// request with a BOUNDED (kind,operation,result) tuple — never a DDL identifier
// as a label — across all nine workload handlers, and that the RetryOnConflict
// loop does NOT over-count (the counter is recorded once after the loop, not
// per retry).

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ruleCluster returns a cluster carrying a single pre-existing workload rule so
// the update/delete rule handlers find their target.
func ruleCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster("test-cluster", "default")
	c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Rules: []cbv1alpha1.WorkloadRule{{Name: "rule1", Action: "cancel"}},
	}
	return c
}

// TestWorkloadOpCounter_AllHandlers drives each workload handler on its success
// and error paths and asserts EXACTLY one counter increment carrying the bounded
// "kind/operation/result" tuple.
func TestWorkloadOpCounter_AllHandlers(t *testing.T) {
	type invokeFn func(s *Server, w http.ResponseWriter, r *http.Request)

	cases := []struct {
		name        string
		want        string // "kind/operation/result"
		method      string
		path        string
		body        string
		pathValues  map[string]string
		dbErrField  func(*mockDBClient) // sets the DB error on the mock for the error case (nil for CR-based handlers)
		cluster     func() *cbv1alpha1.CloudberryCluster
		invoke      invokeFn
		dbBacked    bool
		failPatchCR bool // CR-based handlers fail via Update interceptor for the error case
	}{
		{
			name: "resource_group/create", want: "resource_group/create",
			method: http.MethodPost, path: "/workload/resource-groups",
			body:       `{"name":"rg1"}`,
			pathValues: map[string]string{"name": "test-cluster"},
			dbErrField: func(m *mockDBClient) { m.createResGroupErr = errBoom },
			invoke:     (*Server).handleCreateResourceGroup, dbBacked: true,
		},
		{
			name: "resource_group/delete", want: "resource_group/delete",
			method: http.MethodDelete, path: "/workload/resource-groups/rg1",
			pathValues: map[string]string{"name": "test-cluster", "groupName": "rg1"},
			dbErrField: func(m *mockDBClient) { m.dropResGroupErr = errBoom },
			invoke:     (*Server).handleDeleteResourceGroup, dbBacked: true,
		},
		{
			name: "resource_group/update", want: "resource_group/update",
			method: http.MethodPut, path: "/workload/resource-groups/rg1",
			body:       `{"concurrency":10}`,
			pathValues: map[string]string{"name": "test-cluster", "groupName": "rg1"},
			dbErrField: func(m *mockDBClient) { m.alterResGroupErr = errBoom },
			invoke:     (*Server).handleUpdateResourceGroup, dbBacked: true,
		},
		{
			name: "resource_group/assign", want: "resource_group/assign",
			method: http.MethodPost, path: "/workload/resource-groups/rg1/assign",
			body:       `{"role":"analyst"}`,
			pathValues: map[string]string{"name": "test-cluster", "groupName": "rg1"},
			dbErrField: func(m *mockDBClient) { m.assignRoleErr = errBoom },
			invoke:     (*Server).handleAssignResourceGroup, dbBacked: true,
		},
		{
			name: "resource_queue/create", want: "resource_queue/create",
			method: http.MethodPost, path: "/workload/resource-queues",
			body:       `{"name":"rq1"}`,
			pathValues: map[string]string{"name": "test-cluster"},
			dbErrField: func(m *mockDBClient) { m.createResQueueErr = errBoom },
			invoke:     (*Server).handleCreateResourceQueue, dbBacked: true,
		},
		{
			name: "resource_queue/delete", want: "resource_queue/delete",
			method: http.MethodDelete, path: "/workload/resource-queues/rq1",
			pathValues: map[string]string{"name": "test-cluster", "queueName": "rq1"},
			dbErrField: func(m *mockDBClient) { m.dropResQueueErr = errBoom },
			invoke:     (*Server).handleDeleteResourceQueue, dbBacked: true,
		},
		{
			name: "rule/create", want: "rule/create",
			method: http.MethodPost, path: "/workload/rules",
			body:        `{"name":"newrule","action":"cancel"}`,
			pathValues:  map[string]string{"name": "test-cluster"},
			invoke:      (*Server).handleCreateWorkloadRule,
			failPatchCR: true,
		},
		{
			name: "rule/update", want: "rule/update",
			method: http.MethodPut, path: "/workload/rules/rule1",
			body:        `{"action":"move","moveTarget":"slow"}`,
			pathValues:  map[string]string{"name": "test-cluster", "ruleName": "rule1"},
			cluster:     ruleCluster,
			invoke:      (*Server).handleUpdateWorkloadRule,
			failPatchCR: true,
		},
		{
			name: "rule/delete", want: "rule/delete",
			method: http.MethodDelete, path: "/workload/rules/rule1",
			pathValues:  map[string]string{"name": "test-cluster", "ruleName": "rule1"},
			cluster:     ruleCluster,
			invoke:      (*Server).handleDeleteWorkloadRule,
			failPatchCR: true,
		},
	}

	clusterFor := func(tc int) *cbv1alpha1.CloudberryCluster {
		if cases[tc].cluster != nil {
			return cases[tc].cluster()
		}
		return newTestCluster("test-cluster", "default")
	}

	buildReq := func(method, path, body string, pv map[string]string) *http.Request {
		var rdr *bytes.Reader
		if body != "" {
			rdr = bytes.NewReader([]byte(body))
		}
		var req *http.Request
		full := apiPrefix + "/clusters/test-cluster" + path + "?namespace=default"
		if rdr != nil {
			req = obsAuthedRequest(method, full, rdr)
		} else {
			req = obsAuthedRequest(method, full, nil)
		}
		for k, v := range pv {
			req.SetPathValue(k, v)
		}
		return req
	}

	for i := range cases {
		tc := cases[i]
		idx := i

		t.Run(tc.name+"/success", func(t *testing.T) {
			rec := &obsRecorder{}
			s := newObsServer(rec, clusterFor(idx))
			if tc.dbBacked {
				s.dbFactory = &mockDBFactory{client: &mockDBClient{}}
			}
			req := buildReq(tc.method, tc.path, tc.body, tc.pathValues)
			w := httptest.NewRecorder()
			tc.invoke(s, w, req)

			require.Less(t, w.Code, 400, "happy path body=%s", w.Body.String())
			require.Equal(t, []string{tc.want + "/success"}, rec.workloadOps,
				"exactly one success increment with the bounded tuple")
		})

		t.Run(tc.name+"/error", func(t *testing.T) {
			rec := &obsRecorder{}
			var s *Server
			if tc.dbBacked {
				mock := &mockDBClient{}
				tc.dbErrField(mock)
				s = newObsServer(rec, clusterFor(idx))
				s.dbFactory = &mockDBFactory{client: mock}
			} else {
				// CR-backed handler: fail the Update so the closure result
				// propagates as a 500 error.
				s = newObsServerWithInterceptor(rec, interceptor.Funcs{
					Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
						_ ...client.UpdateOption) error {
						return errBoom
					},
				}, clusterFor(idx))
			}
			req := buildReq(tc.method, tc.path, tc.body, tc.pathValues)
			w := httptest.NewRecorder()
			tc.invoke(s, w, req)

			require.Equal(t, http.StatusInternalServerError, w.Code, "body=%s", w.Body.String())
			require.Equal(t, []string{tc.want + "/error"}, rec.workloadOps,
				"exactly one error increment with the bounded tuple")
		})
	}
}

// TestWorkloadOpCounter_ConflictRetryDoesNotOverCount drives a CR-based rule
// handler through a conflict-then-success RetryOnConflict loop and asserts the
// workload counter rose by exactly ONE, not once per retry (W2-B4 over-counting
// guard).
func TestWorkloadOpCounter_ConflictRetryDoesNotOverCount(t *testing.T) {
	rec := &obsRecorder{}
	conflicts := 2 // two conflicts then success → three Update attempts total
	cluster := newTestCluster("test-cluster", "default")

	k8sClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.UpdateOption) error {
				if conflicts > 0 {
					conflicts--
					return apierrors.NewConflict(
						schema.GroupResource{Resource: "cloudberryclusters"},
						obj.GetName(), errBoom)
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, rec, nil, 0))

	body := bytes.NewBufferString(`{"name":"newrule","action":"cancel"}`)
	req := obsAuthedRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default", body)
	req.SetPathValue("name", "test-cluster")

	w := httptest.NewRecorder()
	s.handleCreateWorkloadRule(w, req)

	require.Equal(t, http.StatusCreated, w.Code, "conflicts must be retried, not surfaced")
	assert.Equal(t, 0, conflicts, "all injected conflicts must have been consumed")
	assert.Equal(t, []string{"rule/create/success"}, rec.workloadOps,
		"the counter must rise by exactly one despite multiple RetryOnConflict iterations")
}
