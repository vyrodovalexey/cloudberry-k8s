package controller

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// gpexpandFinalizeSpyClient wraps the controller-package mockDBClient and records
// the D9 best-effort finalize-guard interactions: whether the operator checked
// for a lingering gpexpand schema (GpexpandSchemaPresent) and, when present,
// whether it dispatched the pure-SQL drop (FinalizeGpexpand). The presence result
// is configurable so both the "schema left behind" and "already clean" paths are
// exercised.
type gpexpandFinalizeSpyClient struct {
	*mockDBClient
	present       bool
	presentCalls  atomic.Int32
	finalizeCalls atomic.Int32
}

func (s *gpexpandFinalizeSpyClient) GpexpandSchemaPresent(_ context.Context) (bool, error) {
	s.presentCalls.Add(1)
	return s.present, nil
}

func (s *gpexpandFinalizeSpyClient) FinalizeGpexpand(_ context.Context) error {
	s.finalizeCalls.Add(1)
	return nil
}

// runScaleOutExpandingSuccess drives the expanding phase over a Succeeded gpexpand
// Job with the given finalize spy and returns the resulting phase.
func runScaleOutExpandingSuccess(t *testing.T, spy *gpexpandFinalizeSpyClient) string {
	t.Helper()
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	cluster.Status.SegmentsTotal = 2
	cluster.Spec.Segments.Count = 3

	jobName := util.GpexpandJobName(cluster.Name, 2, 3)
	state := scaleStateData{
		Phase: scalePhaseExpanding, OldCount: 2, NewCount: 3, ExpandJobName: jobName,
	}
	cluster.Annotations = map[string]string{
		annotationScaleState: scaleStateAnnotation(t, state),
	}

	job := gpexpandJob(cluster.Name, cluster.Namespace, 2, 3,
		batchv1.JobStatus{Succeeded: 1})

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, job).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	factory := &mockDBClientFactory{client: spy}
	r := NewClusterReconciler(k8sClient, scheme, rec, builder.NewBuilder(),
		&wave345Recorder{}, nil, factory)

	_, err := r.checkScaleOutPhases(context.Background(), cluster,
		scaleStateAnnotation(t, state))
	require.NoError(t, err)

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKeyFromObject(cluster), updated))
	var after scaleStateData
	require.NoError(t, json.Unmarshal(
		[]byte(updated.Annotations[annotationScaleState]), &after))
	return after.Phase
}

// TestScaleOutExpanding_FinalizeGuard_SchemaPresent proves the D9 best-effort
// finalize guard: when a gpexpand Job Succeeds but a lingering gpexpand schema is
// reported present, the success path checks presence AND dispatches
// FinalizeGpexpand (pure-SQL DROP SCHEMA) before advancing to completed.
func TestScaleOutExpanding_FinalizeGuard_SchemaPresent(t *testing.T) {
	spy := &gpexpandFinalizeSpyClient{mockDBClient: &mockDBClient{}, present: true}

	phase := runScaleOutExpandingSuccess(t, spy)

	assert.Equal(t, scalePhaseCompleted, phase,
		"a Succeeded gpexpand Job advances expanding -> completed")
	assert.Equal(t, int32(1), spy.presentCalls.Load(),
		"the success path must check for a lingering gpexpand schema")
	assert.Equal(t, int32(1), spy.finalizeCalls.Load(),
		"a present schema must be dropped via FinalizeGpexpand")
}

// TestScaleOutExpanding_FinalizeGuard_SchemaAbsent proves the guard is a no-op
// drop when the schema is already gone: presence is checked but FinalizeGpexpand
// is NOT called, and scale-out still advances to completed.
func TestScaleOutExpanding_FinalizeGuard_SchemaAbsent(t *testing.T) {
	spy := &gpexpandFinalizeSpyClient{mockDBClient: &mockDBClient{}, present: false}

	phase := runScaleOutExpandingSuccess(t, spy)

	assert.Equal(t, scalePhaseCompleted, phase,
		"scale-out completes even when no finalize is needed")
	assert.Equal(t, int32(1), spy.presentCalls.Load(),
		"the success path always checks schema presence")
	assert.Equal(t, int32(0), spy.finalizeCalls.Load(),
		"an absent schema must NOT trigger a redundant DROP")
}

// gpexpandFinalizeErrClient is a configurable spy for the D9 finalize guard's
// error/edge branches. It records presence/finalize invocations and can inject
// a presence-check error, a drop error, or a specific presence result so the
// best-effort guard's non-happy paths are exercised directly.
type gpexpandFinalizeErrClient struct {
	*mockDBClient
	present       bool
	presentErr    error
	finalizeErr   error
	presentCalls  atomic.Int32
	finalizeCalls atomic.Int32
}

func (s *gpexpandFinalizeErrClient) GpexpandSchemaPresent(_ context.Context) (bool, error) {
	s.presentCalls.Add(1)
	return s.present, s.presentErr
}

func (s *gpexpandFinalizeErrClient) FinalizeGpexpand(_ context.Context) error {
	s.finalizeCalls.Add(1)
	return s.finalizeErr
}

// TestFinalizeGpexpandSchema_GuardBranches is a table-driven exercise of the D9
// best-effort finalize guard (finalizeGpexpandSchema) covering every non-happy
// branch: nil db factory (skip), NewClient failure, presence-check error, an
// already-absent schema (no drop), a drop failure, and the fully-successful
// present->drop path. Every branch is best-effort and must never panic.
func TestFinalizeGpexpandSchema_GuardBranches(t *testing.T) {
	tests := []struct {
		name         string
		nilFactory   bool
		newClientErr error
		client       *gpexpandFinalizeErrClient
		wantPresent  int32
		wantFinalize int32
	}{
		{
			name:       "nil factory skips guard entirely",
			nilFactory: true,
		},
		{
			name:         "NewClient error is logged and skipped",
			newClientErr: errors.New("connect refused"),
		},
		{
			name: "presence check error aborts before drop",
			client: &gpexpandFinalizeErrClient{
				mockDBClient: &mockDBClient{},
				presentErr:   errors.New("ping failed"),
			},
			wantPresent:  1,
			wantFinalize: 0,
		},
		{
			name: "schema absent is a no-op drop",
			client: &gpexpandFinalizeErrClient{
				mockDBClient: &mockDBClient{},
				present:      false,
			},
			wantPresent:  1,
			wantFinalize: 0,
		},
		{
			name: "drop error is logged best-effort",
			client: &gpexpandFinalizeErrClient{
				mockDBClient: &mockDBClient{},
				present:      true,
				finalizeErr:  errors.New("drop failed"),
			},
			wantPresent:  1,
			wantFinalize: 1,
		},
		{
			name: "present schema is dropped and event emitted",
			client: &gpexpandFinalizeErrClient{
				mockDBClient: &mockDBClient{},
				present:      true,
			},
			wantPresent:  1,
			wantFinalize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newTestCluster()
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(cluster).WithStatusSubresource(cluster).Build()
			rec := record.NewFakeRecorder(10)

			var factory *mockDBClientFactory
			if !tt.nilFactory {
				factory = &mockDBClientFactory{err: tt.newClientErr}
				if tt.client != nil {
					factory.client = tt.client
				}
			}

			var r *ClusterReconciler
			if tt.nilFactory {
				r = NewClusterReconciler(k8sClient, scheme, rec,
					builder.NewBuilder(), &wave345Recorder{}, nil)
			} else {
				r = NewClusterReconciler(k8sClient, scheme, rec,
					builder.NewBuilder(), &wave345Recorder{}, nil, factory)
			}

			// Must be best-effort: never panics, never returns.
			require.NotPanics(t, func() {
				r.finalizeGpexpandSchema(context.Background(), cluster)
			})

			if tt.client != nil {
				assert.Equal(t, tt.wantPresent, tt.client.presentCalls.Load(),
					"presence-check invocation count")
				assert.Equal(t, tt.wantFinalize, tt.client.finalizeCalls.Load(),
					"finalize (drop) invocation count")
			}
		})
	}
}
