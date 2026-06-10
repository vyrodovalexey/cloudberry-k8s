package main

// Test support for E-1: a stub controller-runtime manager implementing the
// subset of ctrl.Manager that run(), registerControllers, registerWebhooks,
// setupWebhookCerts, and startAPIServer use. The embedded interfaces make
// any UNstubbed method panic loudly, so a new production dependency on the
// manager surfaces immediately as a test failure rather than silent green.

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
)

// fakeCache implements cache.Cache for the WaitForCacheSync seam used by
// startAPIServer. Every other cache method panics via the embedded interface.
type fakeCache struct {
	cache.Cache
	synced bool
	// syncCalled is closed on the first WaitForCacheSync invocation so tests
	// can sequence assertions deterministically. Optional.
	syncCalled chan struct{}
	notified   bool
}

func (c *fakeCache) WaitForCacheSync(ctx context.Context) bool {
	if c.syncCalled != nil && !c.notified {
		c.notified = true
		close(c.syncCalled)
	}
	if c.synced {
		return true
	}
	// Mirror the real cache contract: an unsynced cache blocks until the
	// context is canceled and only then reports failure. This guarantees
	// startAPIServer (and therefore run()'s blocking join of the API server
	// goroutine) terminates promptly on cancellation instead of depending on
	// scheduler timing.
	<-ctx.Done()
	return false
}

// fakeManager implements the ctrl.Manager subset the operator binary uses.
type fakeManager struct {
	ctrl.Manager // embedded nil interface: unstubbed methods panic

	client     client.Client
	scheme     *runtime.Scheme
	cache      *fakeCache
	restCfg    *rest.Config
	webhookSrv ctrlwebhook.Server

	addErr     error
	healthzErr error
	readyzErr  error
	startErr   error

	healthzNames []string
	readyzNames  []string
	runnables    int
}

// newFakeManager builds a stub manager backed by the given fake client.
func newFakeManager(c client.Client) *fakeManager {
	return &fakeManager{
		client:  c,
		scheme:  newTestScheme(),
		cache:   &fakeCache{synced: true},
		restCfg: &rest.Config{Host: "http://127.0.0.1:1"},
		// A real (unstarted) webhook server: Register works without Start.
		webhookSrv: ctrlwebhook.NewServer(ctrlwebhook.Options{Port: 9443}),
	}
}

func (m *fakeManager) GetClient() client.Client    { return m.client }
func (m *fakeManager) GetScheme() *runtime.Scheme  { return m.scheme }
func (m *fakeManager) GetCache() cache.Cache       { return m.cache }
func (m *fakeManager) GetConfig() *rest.Config     { return m.restCfg }
func (m *fakeManager) GetHTTPClient() *http.Client { return http.DefaultClient }
func (m *fakeManager) GetRESTMapper() meta.RESTMapper {
	return m.client.RESTMapper()
}

func (m *fakeManager) GetEventRecorderFor(_ string) record.EventRecorder {
	return record.NewFakeRecorder(100)
}

func (m *fakeManager) GetControllerOptions() crconfig.Controller {
	skip := true
	// Skip the process-global controller-name uniqueness validation so
	// multiple tests can register the same controllers repeatedly.
	return crconfig.Controller{SkipNameValidation: &skip}
}

func (m *fakeManager) GetLogger() logr.Logger { return logr.Discard() }

func (m *fakeManager) Add(_ manager.Runnable) error {
	if m.addErr != nil {
		return m.addErr
	}
	m.runnables++
	return nil
}

func (m *fakeManager) AddHealthzCheck(name string, _ healthz.Checker) error {
	if m.healthzErr != nil {
		return m.healthzErr
	}
	m.healthzNames = append(m.healthzNames, name)
	return nil
}

func (m *fakeManager) AddReadyzCheck(name string, _ healthz.Checker) error {
	if m.readyzErr != nil {
		return m.readyzErr
	}
	m.readyzNames = append(m.readyzNames, name)
	return nil
}

func (m *fakeManager) GetWebhookServer() ctrlwebhook.Server { return m.webhookSrv }

// Start blocks until the context is canceled (mirroring the real manager's
// contract) unless a start error is injected.
func (m *fakeManager) Start(ctx context.Context) error {
	if m.startErr != nil {
		return m.startErr
	}
	<-ctx.Done()
	return nil
}

// quietLogger returns an error-only logger for noisy run() tests.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
