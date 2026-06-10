package api

// E-3 (H-2/H-3): the follow-mode log-streaming regression exercised through
// the REAL middleware chain — s.Handler() (tracing → metrics → security
// headers → mux) with the per-route rate-limit + auth wrappers — on an
// httptest.Server with an aggressive WriteTimeout. This is the only
// configuration in which the original bug manifested: the deadline clearing
// must traverse the tracing middleware's statusRecorder via Unwrap to reach
// the real connection. A synthetic handler cannot detect a regression in
// handleBackupJobLogs/clearWriteDeadline; this test fails if either drops the
// deadline-clearing behavior.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// timedChunkBody is an io.ReadCloser that emits chunks separated by a fixed
// delay, simulating a pod log stream in follow mode.
type timedChunkBody struct {
	chunks [][]byte
	delay  time.Duration
	idx    int
}

func (b *timedChunkBody) Read(p []byte) (int, error) {
	if b.idx >= len(b.chunks) {
		return 0, io.EOF
	}
	if b.idx > 0 {
		time.Sleep(b.delay)
	}
	n := copy(p, b.chunks[b.idx])
	b.idx++
	return n, nil
}

func (b *timedChunkBody) Close() error { return nil }

// slowLogsClientset wraps the fake clientset so Pods(...).GetLogs returns a
// stream backed by a timedChunkBody instead of the fixed "fake logs" payload.
type slowLogsClientset struct {
	*k8sfake.Clientset
	mkBody func() io.ReadCloser
}

func (c *slowLogsClientset) CoreV1() corev1client.CoreV1Interface {
	return &slowLogsCoreV1{CoreV1Interface: c.Clientset.CoreV1(), mkBody: c.mkBody}
}

type slowLogsCoreV1 struct {
	corev1client.CoreV1Interface
	mkBody func() io.ReadCloser
}

func (c *slowLogsCoreV1) Pods(ns string) corev1client.PodInterface {
	return &slowLogsPods{PodInterface: c.CoreV1Interface.Pods(ns), mkBody: c.mkBody}
}

type slowLogsPods struct {
	corev1client.PodInterface
	mkBody func() io.ReadCloser
}

func (p *slowLogsPods) GetLogs(name string, _ *corev1.PodLogOptions) *rest.Request {
	mkBody := p.mkBody
	fakeREST := &restfake.RESTClient{
		Client: restfake.CreateHTTPClient(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/plain"}},
				Body:       mkBody(),
			}, nil
		}),
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		GroupVersion:         corev1.SchemeGroupVersion,
		VersionedAPIPath:     "/api/v1",
	}
	return fakeREST.Request().Name(name).Resource("pods").SubResource("log")
}

// newFullChainStreamingServer builds a Server with REAL auth middleware (basic
// provider, MinCost hashing — behavior under test is routing/streaming, not
// hash strength) and the slow-log clientset, returning the operator handler
// chain and the credentials to use.
func newFullChainStreamingServer(t *testing.T, mkBody func() io.ReadCloser) http.Handler {
	t.Helper()

	cluster := newBackupEnabledCluster()
	pod := &corev1.Pod{}
	pod.Name = "test-cluster-backup-1-abcde"
	pod.Namespace = "default"
	pod.Labels = map[string]string{labelJobNameBatch: "test-cluster-backup-1"}

	k8sClient := ctrlfake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(cluster, pod).
		Build()

	credStore := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	credStore.SetCredentials("operator", "stream-pass", auth.PermissionOperator)
	authMW := auth.NewAuthMiddleware(auth.NewBasicAuthProvider(credStore, nil), nil, nil, nil)

	s := trackServer(NewServer(k8sClient, authMW, nil, &metrics.NoopRecorder{}, nil, 0))
	t.Cleanup(s.Close)
	var clientset kubernetes.Interface = &slowLogsClientset{
		Clientset: k8sfake.NewSimpleClientset(),
		mkBody:    mkBody,
	}
	s.WithClientset(clientset)
	return s.Handler()
}

func TestFollowMode_FullMiddlewareChain_SurvivesWriteTimeout(t *testing.T) {
	// Margins (H-3): WriteTimeout 500ms, inter-chunk delay 300ms, 4 chunks →
	// the stream stays open ~900ms past the first byte, comfortably beyond
	// the WriteTimeout, while individual waits stay far from the limit.
	const (
		writeTimeout = 500 * time.Millisecond
		chunkDelay   = 300 * time.Millisecond
		chunkCount   = 4
	)

	mkBody := func() io.ReadCloser {
		chunks := make([][]byte, 0, chunkCount)
		for i := 0; i < chunkCount; i++ {
			chunks = append(chunks, []byte("chunk\n"))
		}
		return &timedChunkBody{chunks: chunks, delay: chunkDelay}
	}

	handler := newFullChainStreamingServer(t, mkBody)
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.WriteTimeout = writeTimeout
	srv.Start()
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+apiPrefix+
		"/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs?follow=true&namespace=default", nil)
	require.NoError(t, err)
	req.SetBasicAuth("operator", "stream-pass")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"the request must clear rate-limit + auth + permission middlewares")

	var (
		body         []byte
		firstChunkAt time.Time
		buf          = make([]byte, 64)
	)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 && firstChunkAt.IsZero() {
			firstChunkAt = time.Now()
		}
		body = append(body, buf[:n]...)
		if readErr != nil {
			assert.ErrorIs(t, readErr, io.EOF,
				"the stream must end with a clean EOF, not a timeout-cut connection")
			break
		}
	}
	elapsed := time.Since(start)

	// The first chunk must be flushed promptly — before the source stream
	// completes (follow contract), i.e. well before all chunks were emitted.
	require.False(t, firstChunkAt.IsZero(), "no data received")
	assert.Less(t, firstChunkAt.Sub(start), chunkDelay*2,
		"first chunk must be flushed before the stream completes")

	// All chunks must arrive even though the session outlived WriteTimeout.
	assert.Equal(t, chunkCount*len("chunk\n"), len(body),
		"every chunk must be delivered: the follow stream must survive the WriteTimeout")
	assert.Greater(t, elapsed, writeTimeout,
		"sanity: the session must actually outlive the server WriteTimeout")
}

// TestFollowMode_FullMiddlewareChain_NonStreamingStillTimed ensures the
// exemption is scoped: a non-follow request through the same chain completes
// normally (no deadline clearing needed) — guarding against an accidental
// global WriteTimeout opt-out.
func TestFollowMode_FullMiddlewareChain_NonFollowCompletes(t *testing.T) {
	mkBody := func() io.ReadCloser {
		return &timedChunkBody{chunks: [][]byte{[]byte("all logs\n")}}
	}
	handler := newFullChainStreamingServer(t, mkBody)
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.WriteTimeout = 500 * time.Millisecond
	srv.Start()
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+apiPrefix+
		"/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs?namespace=default", nil)
	require.NoError(t, err)
	req.SetBasicAuth("operator", "stream-pass")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "all logs\n", string(body))
}

// TestFollowMode_FullMiddlewareChain_AuthRequired proves the streaming route
// is NOT exempt from authentication in the real chain.
func TestFollowMode_FullMiddlewareChain_AuthRequired(t *testing.T) {
	handler := newFullChainStreamingServer(t, func() io.ReadCloser {
		return &timedChunkBody{chunks: [][]byte{[]byte("secret\n")}}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + apiPrefix + //nolint:noctx // test request
		"/clusters/test-cluster/backups/jobs/test-cluster-backup-1/logs?follow=true&namespace=default")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
