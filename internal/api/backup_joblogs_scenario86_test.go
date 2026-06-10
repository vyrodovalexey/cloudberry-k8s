package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// ---------------------------------------------------------------------------
// buildPodLogOptions — query parameter parsing (follow / tailLines).
// ---------------------------------------------------------------------------

func TestBuildPodLogOptions(t *testing.T) {
	tail100 := int64(100)
	tail0 := int64(0)
	tests := []struct {
		name      string
		query     string
		wantOpts  corev1.PodLogOptions
		wantTail  *int64
		wantTailP bool
	}{
		{
			name:     "empty query",
			query:    "",
			wantOpts: corev1.PodLogOptions{},
		},
		{
			name:     "follow true",
			query:    "follow=true",
			wantOpts: corev1.PodLogOptions{Follow: true},
		},
		{
			name:     "follow false",
			query:    "follow=false",
			wantOpts: corev1.PodLogOptions{Follow: false},
		},
		{
			name:     "follow invalid is ignored",
			query:    "follow=notabool",
			wantOpts: corev1.PodLogOptions{},
		},
		{
			name:      "tailLines positive",
			query:     "tailLines=100",
			wantTail:  &tail100,
			wantTailP: true,
		},
		{
			name:      "tailLines zero",
			query:     "tailLines=0",
			wantTail:  &tail0,
			wantTailP: true,
		},
		{
			name:  "tailLines negative is ignored",
			query: "tailLines=-5",
		},
		{
			name:  "tailLines non-numeric is ignored",
			query: "tailLines=abc",
		},
		{
			name:      "follow and tailLines together",
			query:     "follow=true&tailLines=100",
			wantOpts:  corev1.PodLogOptions{Follow: true},
			wantTail:  &tail100,
			wantTailP: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			values, err := url.ParseQuery(tc.query)
			require.NoError(t, err)

			opts := buildPodLogOptions(values)
			require.NotNil(t, opts)
			assert.Equal(t, tc.wantOpts.Follow, opts.Follow)
			if tc.wantTailP {
				require.NotNil(t, opts.TailLines)
				assert.Equal(t, *tc.wantTail, *opts.TailLines)
			} else {
				assert.Nil(t, opts.TailLines)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// mostRecentPodName — newest-by-CreationTimestamp selection.
// ---------------------------------------------------------------------------

func TestMostRecentPodName(t *testing.T) {
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	mkPod := func(name string, ageOffset time.Duration) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.NewTime(base.Add(ageOffset)),
			},
		}
	}

	t.Run("empty slice returns empty", func(t *testing.T) {
		assert.Equal(t, "", mostRecentPodName(nil))
		assert.Equal(t, "", mostRecentPodName([]corev1.Pod{}))
	})

	t.Run("single pod", func(t *testing.T) {
		assert.Equal(t, "only", mostRecentPodName([]corev1.Pod{mkPod("only", 0)}))
	})

	t.Run("picks newest pod among many", func(t *testing.T) {
		pods := []corev1.Pod{
			mkPod("old", -2*time.Hour),
			mkPod("newest", 1*time.Hour),
			mkPod("middle", -1*time.Hour),
		}
		assert.Equal(t, "newest", mostRecentPodName(pods))
	})

	t.Run("first pod is the newest", func(t *testing.T) {
		pods := []corev1.Pod{
			mkPod("newest", 5*time.Minute),
			mkPod("older", -5*time.Minute),
		}
		assert.Equal(t, "newest", mostRecentPodName(pods))
	})
}

// ---------------------------------------------------------------------------
// copyLogStream — non-follow copy and follow (flushing) copy.
// ---------------------------------------------------------------------------

// flushRecorder is an http.ResponseWriter + http.Flusher that records flush
// calls, used to exercise the follow path of copyLogStream.
type flushRecorder struct {
	buf     bytes.Buffer
	flushes int
}

func (f *flushRecorder) Header() http.Header         { return http.Header{} }
func (f *flushRecorder) Write(p []byte) (int, error) { return f.buf.Write(p) }
func (f *flushRecorder) WriteHeader(int)             {}
func (f *flushRecorder) Flush()                      { f.flushes++ }

// plainWriter is an http.ResponseWriter WITHOUT http.Flusher support, used to
// verify graceful (non-flushing, no-panic) degradation of copyLogStream.
type plainWriter struct{ buf bytes.Buffer }

func (p *plainWriter) Header() http.Header         { return http.Header{} }
func (p *plainWriter) Write(b []byte) (int, error) { return p.buf.Write(b) }
func (p *plainWriter) WriteHeader(int)             {}

func TestCopyLogStream_NonFollow(t *testing.T) {
	w := &plainWriter{}
	src := bytes.NewBufferString("hello world\nsecond line\n")
	copyLogStream(context.Background(), w, src, false)
	assert.Equal(t, "hello world\nsecond line\n", w.buf.String())
}

func TestCopyLogStream_NonFlusherWriter(t *testing.T) {
	// follow=true but the writer is not an http.Flusher → the
	// ResponseController flush returns ErrNotSupported, which is ignored:
	// the copy completes without panicking.
	w := &plainWriter{}
	src := bytes.NewBufferString("streamed data\n")
	copyLogStream(context.Background(), w, src, true)
	assert.Equal(t, "streamed data\n", w.buf.String())
}

func TestCopyLogStream_FollowFlushes(t *testing.T) {
	rec := &flushRecorder{}
	src := bytes.NewBufferString("line a\nline b\nline c\n")
	copyLogStream(context.Background(), rec, src, true)
	assert.Equal(t, "line a\nline b\nline c\n", rec.buf.String())
	// At least the final flush (on EOF) must have happened.
	assert.GreaterOrEqual(t, rec.flushes, 1)
}

// errWriter is a flusher that fails on Write to exercise the write-error branch
// of copyLogStream's follow loop.
type errFlushWriter struct{ flushes int }

func (e *errFlushWriter) Header() http.Header       { return http.Header{} }
func (e *errFlushWriter) Write([]byte) (int, error) { return 0, errBoom }
func (e *errFlushWriter) WriteHeader(int)           {}
func (e *errFlushWriter) Flush()                    { e.flushes++ }

func TestCopyLogStream_FollowWriteError(t *testing.T) {
	w := &errFlushWriter{}
	src := bytes.NewBufferString("some data that will fail to write\n")
	// Must return cleanly even when the writer errors mid-stream.
	copyLogStream(context.Background(), w, src, true)
}

// slowReader emits one chunk, then waits longer than the flush interval before
// emitting the next, so copyLogStream's periodic-flush branch is exercised.
type slowReader struct {
	chunks [][]byte
	idx    int
	delay  time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if s.idx >= len(s.chunks) {
		return 0, io.EOF
	}
	if s.idx > 0 {
		time.Sleep(s.delay)
	}
	n := copy(p, s.chunks[s.idx])
	s.idx++
	return n, nil
}

func TestCopyLogStream_FollowPeriodicFlush(t *testing.T) {
	rec := &flushRecorder{}
	src := &slowReader{
		chunks: [][]byte{[]byte("chunk-1\n"), []byte("chunk-2\n")},
		delay:  jobLogsFlushInterval + 100*time.Millisecond,
	}
	copyLogStream(context.Background(), rec, src, true)
	assert.Equal(t, "chunk-1\nchunk-2\n", rec.buf.String())
	// One periodic flush (after the delay) plus the final EOF flush.
	assert.GreaterOrEqual(t, rec.flushes, 2)
}

func TestCopyLogStream_FollowCanceledContext(t *testing.T) {
	rec := &flushRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled → loop returns immediately
	src := bytes.NewBufferString("never copied\n")
	copyLogStream(ctx, rec, src, true)
	assert.Empty(t, rec.buf.String())
}

// ---------------------------------------------------------------------------
// findJobPod / handleBackupJobLogs — list-error and stream-error paths.
// ---------------------------------------------------------------------------

// errBoom is a sentinel error used for injected client/clientset failures.
var errBoom = errors.New("boom")

func TestFindJobPod_ListError(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption,
			) error {
				return errBoom
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0))

	_, found, err := s.findJobPod(context.Background(), "default", "some-job")
	require.Error(t, err)
	assert.False(t, found)
}

func TestHandleBackupJobLogs_ListError(t *testing.T) {
	cluster := newBackupEnabledCluster()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption,
			) error {
				return errBoom
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "test-cluster-backup-1"))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

func TestHandleBackupJobLogs_StreamError(t *testing.T) {
	cluster := newBackupEnabledCluster()
	pod := jobPod("test-cluster-backup-1-abcde", "test-cluster-backup-1")

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects([]runtime.Object{cluster, pod}...).
		Build()

	// A fake clientset whose pod-log "get" reactor fails, so Stream() errors.
	cs := k8sfake.NewSimpleClientset()
	cs.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "log" {
			return true, nil, errBoom
		}
		return false, nil, nil
	})

	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).WithClientset(cs)

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "test-cluster-backup-1"))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

// TestHandleBackupJobLogs_WithTailAndFollow exercises the query-parameter path
// of streamPodLogs/buildPodLogOptions through the full handler.
func TestHandleBackupJobLogs_WithTailAndFollow(t *testing.T) {
	cluster := newBackupEnabledCluster()
	pod := jobPod("test-cluster-backup-1-abcde", "test-cluster-backup-1")
	s := newJobLogsTestServer(cluster, pod)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs"+
			"?namespace=default&follow=false&tailLines=50", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "test-cluster-backup-1")

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "fake logs", rec.Body.String())
}
