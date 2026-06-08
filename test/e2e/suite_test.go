//go:build e2e

// Package e2e contains end-to-end tests for the cloudberry operator.
package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// E2ESuite is the base suite for all E2E tests.
type E2ESuite struct {
	suite.Suite
	client    client.Client
	scheme    *runtime.Scheme
	env       *testutil.TestEnv
	logger    *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	namespace string
}

// SetupSuite initializes the E2E test environment.
func (s *E2ESuite) SetupSuite() {
	s.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s.env = testutil.NewTestEnv()

	// Create scheme
	s.scheme = runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(s.scheme)
	_ = corev1.AddToScheme(s.scheme)
	_ = appsv1.AddToScheme(s.scheme)

	// Use fake client for E2E tests (in a real setup, this would use envtest or kind)
	s.client = fake.NewClientBuilder().
		WithScheme(s.scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		Build()

	s.logger.Info("E2E test suite initialized")
}

// SetupTest creates a fresh context and namespace for each test.
func (s *E2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	s.namespace = testutil.UniqueNamespace("e2e")
	s.logger.Info("starting E2E test", "namespace", s.namespace)
}

// TearDownTest cleans up after each test.
func (s *E2ESuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("E2E test completed", "namespace", s.namespace)
}

// TearDownSuite cleans up the E2E test environment.
func (s *E2ESuite) TearDownSuite() {
	s.logger.Info("E2E test suite completed")
}

func TestE2E_Suite(t *testing.T) {
	// E2E tests are not parallelized at the suite level
	suite.Run(t, new(ClusterE2ESuite))
	suite.Run(t, new(AuthE2ESuite))
	suite.Run(t, new(HAE2ESuite))
}
