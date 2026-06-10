package main

// E-1 tests: run(), registerControllers, registerWebhooks, startAPIServer,
// newAdminVaultClient, buildOIDCProvider, setupWebhookCerts and the cert
// rotation loop — exercised through the manager-constructor seams without a
// live cluster.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// testOperatorConfig returns a minimal operator config for direct seam tests.
func testOperatorConfig() *config.OperatorConfig {
	return &config.OperatorConfig{
		ReconcileInterval: time.Minute,
		OperationTimeout:  config.DefaultOperationTimeout,
	}
}

// installRunSeams swaps the manager-construction and metrics-registry seams
// for one run() invocation and restores them afterwards.
func installRunSeams(t *testing.T, mgr ctrl.Manager, mgrErr error) {
	t.Helper()
	prevReg := metricsRegistry
	prevNew := newManager
	prevGet := getRestConfig
	metricsRegistry = prometheus.NewRegistry()
	getRestConfig = func() *rest.Config { return &rest.Config{Host: "http://127.0.0.1:1"} }
	newManager = func(_ *rest.Config, _ ctrl.Options) (ctrl.Manager, error) {
		if mgrErr != nil {
			return nil, mgrErr
		}
		return mgr, nil
	}
	t.Cleanup(func() {
		metricsRegistry = prevReg
		newManager = prevNew
		getRestConfig = prevGet
	})
}

// cleanRunEnv pins the environment variables run() consumes so test results
// do not depend on the developer's shell.
func cleanRunEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CLOUDBERRY_CONFIG_FILE", "")
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "test-admin-pw")
	t.Setenv("POD_NAMESPACE", "test-ns")
}

// reserveAddr reserves and releases a free TCP address.
func reserveAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// startRun launches run() in a goroutine and guarantees the goroutine has
// fully terminated before any earlier-registered cleanup (in particular the
// seam restore from installRunSeams) executes. t.Cleanup runs LIFO, so the
// join registered here always fires BEFORE the seams are restored — on every
// exit path, including require/t.Fatal failures and timeouts. Without this
// join, a failing test could restore the seams while run() was still
// executing, letting it reach the real ctrl.GetConfigOrDie, which os.Exit(1)s
// the whole test binary on kubeconfig-less CI runners.
func startRun(t *testing.T, args ...string) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runErrCh <- run(ctx, args)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("run() goroutine did not terminate during cleanup; " +
				"seams would be restored while run() is still executing")
		}
	})
	return runErrCh, cancel
}

// ----------------------------------------------------------------------------
// run()
// ----------------------------------------------------------------------------

func TestRun_FlagParseError(t *testing.T) {
	cleanRunEnv(t)
	err := run(context.Background(), []string{"--definitely-not-a-flag"})
	require.Error(t, err)
}

func TestRun_ConfigLoadError(t *testing.T) {
	cleanRunEnv(t)
	cfgFile := filepath.Join(t.TempDir(), "broken.yaml")
	// NOTE: the content must be YAML that genuinely fails to parse. A scalar
	// like "::: not yaml {{{" is accepted by the YAML parser (it decodes to a
	// plain string), in which case run() proceeds past config loading toward
	// getRestConfig() — historically reaching the real ctrl.GetConfigOrDie and
	// os.Exit(1)ing the whole test binary on kubeconfig-less CI runners.
	require.NoError(t, os.WriteFile(cfgFile, []byte("a: [unclosed"), 0o600))
	t.Setenv("CLOUDBERRY_CONFIG_FILE", cfgFile)
	// Defense in depth: even if config loading unexpectedly succeeds, run()
	// must hit these seams instead of any real cluster machinery.
	installRunSeams(t, nil, fmt.Errorf("must not reach manager construction"))

	err := run(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading config file",
		"run() must fail at config loading, not at a later stage")
}

func TestRun_ManagerConstructionError(t *testing.T) {
	cleanRunEnv(t)
	installRunSeams(t, nil, fmt.Errorf("manager boom"))

	err := run(context.Background(), []string{"--log-level=error"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager boom")
}

func TestRun_TelemetryEnabled_ShutdownDeferred(t *testing.T) {
	// With telemetry enabled, run() must initialize the tracer, register the
	// deferred shutdown, and continue — proven by reaching the (failing)
	// manager constructor and returning without hanging on tracer shutdown.
	cleanRunEnv(t)
	cfgFile := filepath.Join(t.TempDir(), "telemetry.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(
		"telemetry:\n  enabled: true\n  otlp-protocol: http\n  otlp-endpoint: 127.0.0.1:1\n",
	), 0o600))
	t.Setenv("CLOUDBERRY_CONFIG_FILE", cfgFile)
	installRunSeams(t, nil, fmt.Errorf("reached manager"))

	err := run(context.Background(), []string{"--log-level=error"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reached manager",
		"telemetry initialization must not abort startup")
}

func TestRun_VaultClientError(t *testing.T) {
	cleanRunEnv(t)
	t.Setenv("CLOUDBERRY_VAULT_ENABLED", "true")
	// A syntactically invalid address passes config validation (non-empty)
	// but fails vault client construction immediately (no retry budget).
	t.Setenv("CLOUDBERRY_VAULT_ADDRESS", "://bad-url")
	installRunSeams(t, newFakeManager(newFakeClient()), nil)

	err := run(context.Background(), []string{"--log-level=error"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating vault client for admin controller")
}

func TestRun_RegisterControllersError(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	mgr.addErr = fmt.Errorf("add rejected")
	installRunSeams(t, mgr, nil)

	err := run(context.Background(), []string{"--log-level=error"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "setting up cluster controller")
}

func TestRun_WebhookCertSetupError(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	installRunSeams(t, mgr, nil)

	prevDirect := newDirectClient
	newDirectClient = func(_ *rest.Config, _ client.Options) (client.Client, error) {
		return nil, fmt.Errorf("direct client boom")
	}
	t.Cleanup(func() { newDirectClient = prevDirect })

	err := run(context.Background(), []string{"--log-level=error", "--webhook-enabled=true"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "setting up webhook certificates")
}

func TestRun_ManagerStartError(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	mgr.startErr = fmt.Errorf("start boom")
	installRunSeams(t, mgr, nil)

	err := run(context.Background(), []string{
		"--log-level=error",
		"--webhook-enabled=false",
		"--api-address=" + reserveAddr(t),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start boom")
}

func TestRun_HappyPath_WebhooksDisabled(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	installRunSeams(t, mgr, nil)

	apiAddr := reserveAddr(t)
	runErrCh, cancel := startRun(t,
		"--log-level=error",
		"--webhook-enabled=false",
		"--api-address="+apiAddr,
	)

	// Wait until the REST API server is actually listening, then shut down.
	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", apiAddr, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond, "API server never started listening")

	cancel()
	select {
	case err := <-runErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancellation")
	}

	assert.Equal(t, 4, mgr.runnables, "all four controllers must be registered")
	assert.Equal(t, []string{"healthz"}, mgr.healthzNames)
	assert.Equal(t, []string{"readyz"}, mgr.readyzNames)
}

func TestRun_HappyPath_WebhooksEnabled(t *testing.T) {
	cleanRunEnv(t)
	directClient := newFakeClient()
	mgr := newFakeManager(newFakeClient())
	installRunSeams(t, mgr, nil)

	prevDirect := newDirectClient
	newDirectClient = func(_ *rest.Config, _ client.Options) (client.Client, error) {
		return directClient, nil
	}
	t.Cleanup(func() { newDirectClient = prevDirect })

	apiAddr := reserveAddr(t)
	runErrCh, cancel := startRun(t,
		"--log-level=error",
		"--webhook-enabled=true",
		"--api-address="+apiAddr,
	)

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", apiAddr, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond, "API server never started listening")

	cancel()
	select {
	case err := <-runErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancellation (cert-rotation goroutine leak?)")
	}

	// The self-signed webhook certificates were provisioned via the direct client.
	secret := &corev1.Secret{}
	getErr := directClient.Get(context.Background(), types.NamespacedName{
		Name:      "cloudberry-operator-webhook-certs", // config default
		Namespace: "test-ns",
	}, secret)
	require.NoError(t, getErr, "webhook cert secret must be created during startup")
	assert.NotEmpty(t, secret.Data["tls.crt"])
}

func TestRun_APIServerErrorSurfaced(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	syncCalled := make(chan struct{})
	mgr.cache = &fakeCache{synced: false, syncCalled: syncCalled}
	installRunSeams(t, mgr, nil)

	runErrCh, cancel := startRun(t,
		"--log-level=error",
		"--webhook-enabled=false",
		"--api-address="+reserveAddr(t),
	)

	// Wait until the API server goroutine has hit the failed cache sync, give
	// it a moment to publish the error, then stop the manager.
	select {
	case <-syncCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("startAPIServer never consulted the cache")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErrCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "API server error")
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return")
	}
}

// ----------------------------------------------------------------------------
// registerControllers
// ----------------------------------------------------------------------------

func TestRegisterControllers_HappyPath(t *testing.T) {
	mgr := newFakeManager(newFakeClient())

	err := registerControllers(mgr, testOperatorConfig(), &metrics.NoopRecorder{}, testLogger(), nil)

	require.NoError(t, err)
	assert.Equal(t, 4, mgr.runnables,
		"cluster, HA, auth, and admin controllers must all be added")
	assert.Equal(t, []string{"healthz"}, mgr.healthzNames)
	assert.Equal(t, []string{"readyz"}, mgr.readyzNames)
}

func TestRegisterControllers_SetupFailure(t *testing.T) {
	mgr := newFakeManager(newFakeClient())
	mgr.addErr = fmt.Errorf("manager rejects runnables")

	err := registerControllers(mgr, testOperatorConfig(), &metrics.NoopRecorder{}, testLogger(), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "setting up cluster controller")
}

func TestRegisterControllers_HealthzError(t *testing.T) {
	mgr := newFakeManager(newFakeClient())
	mgr.healthzErr = fmt.Errorf("healthz rejected")

	err := registerControllers(mgr, testOperatorConfig(), &metrics.NoopRecorder{}, testLogger(), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "adding healthz check")
}

func TestRegisterControllers_ReadyzError(t *testing.T) {
	mgr := newFakeManager(newFakeClient())
	mgr.readyzErr = fmt.Errorf("readyz rejected")

	err := registerControllers(mgr, testOperatorConfig(), &metrics.NoopRecorder{}, testLogger(), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "adding readyz check")
}

// ----------------------------------------------------------------------------
// registerWebhooks
// ----------------------------------------------------------------------------

func TestRegisterWebhooks_HappyPath(t *testing.T) {
	mgr := newFakeManager(newFakeClient())

	err := registerWebhooks(mgr, testLogger(), &metrics.NoopRecorder{})

	require.NoError(t, err)
}

func TestRegisterWebhooks_SchemeMissingType_Error(t *testing.T) {
	// A manager whose scheme does not know CloudberryCluster makes the
	// webhook builder fail; the error must be wrapped and propagated.
	mgr := newFakeManager(newFakeClient())
	mgr.scheme = runtime.NewScheme() // empty scheme

	err := registerWebhooks(mgr, testLogger(), &metrics.NoopRecorder{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating validating webhook")
}

// ----------------------------------------------------------------------------
// buildVaultClientConfig + newAdminVaultClient
// ----------------------------------------------------------------------------

func TestBuildVaultClientConfig_MapsAllFields(t *testing.T) {
	// All vault settings — including the AppRole credentials emitted by the
	// Helm chart as CLOUDBERRY_VAULT_ROLE_ID / CLOUDBERRY_VAULT_SECRET_ID —
	// must be wired into the vault client config.
	cfg := testOperatorConfig()
	cfg.Vault = config.VaultConfig{
		Enabled:    true,
		Address:    "https://vault.example.com:8200",
		AuthMethod: "approle",
		AuthPath:   "auth/approle",
		Role:       "legacy-role",
		Token:      config.RedactedString("legacy-token"),
		RoleID:     "test-role-id",
		SecretID:   config.RedactedString("test-secret-id"),
		SecretPath: "secret/data/cloudberry",
	}

	vaultCfg := buildVaultClientConfig(cfg)

	assert.True(t, vaultCfg.Enabled)
	assert.Equal(t, "https://vault.example.com:8200", vaultCfg.Address)
	assert.Equal(t, "approle", vaultCfg.AuthMethod)
	assert.Equal(t, "auth/approle", vaultCfg.AuthPath)
	assert.Equal(t, "legacy-role", vaultCfg.Role)
	assert.Equal(t, "legacy-token", vaultCfg.Token)
	assert.Equal(t, "test-role-id", vaultCfg.RoleID)
	assert.Equal(t, "test-secret-id", vaultCfg.SecretID)
	assert.Equal(t, "secret/data/cloudberry", vaultCfg.SecretPath)
}

func TestBuildVaultClientConfig_EmptyAppRoleFieldsStayEmpty(t *testing.T) {
	// Empty RoleID/SecretID must be passed through unchanged: the
	// backward-compatible fallback to Role/Token lives in internal/vault.
	cfg := testOperatorConfig()
	cfg.Vault = config.VaultConfig{
		Enabled:    true,
		Address:    "https://vault.example.com:8200",
		AuthMethod: "approle",
		Role:       "legacy-role",
		Token:      config.RedactedString("legacy-token"),
	}

	vaultCfg := buildVaultClientConfig(cfg)

	assert.Empty(t, vaultCfg.RoleID)
	assert.Empty(t, vaultCfg.SecretID)
}

func TestNewAdminVaultClient_AppRoleAuthSuccess(t *testing.T) {
	// An httptest Vault that accepts the AppRole login proves the RoleID and
	// SecretID from the operator config reach the vault client login call.
	var gotRoleID, gotSecretID atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/approle/login", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RoleID   string `json:"role_id"`
			SecretID string `json:"secret_id"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		gotRoleID.Store(body.RoleID)
		gotSecretID.Store(body.SecretID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"auth":{"client_token":"s.approle-token","lease_duration":3600,"renewable":false}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := testOperatorConfig()
	cfg.Vault = config.VaultConfig{
		Enabled:    true,
		Address:    server.URL,
		AuthMethod: "approle",
		AuthPath:   "auth/approle",
		RoleID:     "test-role-id",
		SecretID:   config.RedactedString("test-secret-id"),
	}

	vc, err := newAdminVaultClient(context.Background(), cfg, &metrics.NoopRecorder{}, testLogger())

	require.NoError(t, err)
	require.NotNil(t, vc)
	assert.Equal(t, "test-role-id", gotRoleID.Load())
	assert.Equal(t, "test-secret-id", gotSecretID.Load())
	type closer interface{ Close() }
	if c, ok := vc.(closer); ok {
		c.Close()
	}
}

func TestNewAdminVaultClient_Disabled(t *testing.T) {
	cfg := testOperatorConfig()
	cfg.Vault.Enabled = false

	vc, err := newAdminVaultClient(context.Background(), cfg, &metrics.NoopRecorder{}, testLogger())

	require.NoError(t, err)
	assert.Nil(t, vc, "vault disabled must yield a nil client without error")
}

func TestNewAdminVaultClient_TokenAuthSuccess(t *testing.T) {
	server := httptest.NewServer(http.NewServeMux())
	defer server.Close()

	cfg := testOperatorConfig()
	cfg.Vault.Enabled = true
	cfg.Vault.Address = server.URL
	cfg.Vault.AuthMethod = "token"
	cfg.Vault.Token = config.RedactedString("s.test-token")

	vc, err := newAdminVaultClient(context.Background(), cfg, &metrics.NoopRecorder{}, testLogger())

	require.NoError(t, err)
	require.NotNil(t, vc)
	// Stop the background token watcher (run() does the same via Closer).
	type closer interface{ Close() }
	if c, ok := vc.(closer); ok {
		c.Close()
	}
}

func TestNewAdminVaultClient_AuthError(t *testing.T) {
	cfg := testOperatorConfig()
	cfg.Vault.Enabled = true
	cfg.Vault.Address = "" // enabled without address → immediate error

	vc, err := newAdminVaultClient(context.Background(), cfg, &metrics.NoopRecorder{}, testLogger())

	require.Error(t, err)
	assert.Nil(t, vc)
}

// ----------------------------------------------------------------------------
// buildOIDCProvider + oidcStartupRetryOpts
// ----------------------------------------------------------------------------

// fakeOIDCDiscovery is an httptest OIDC issuer whose discovery document fails
// with HTTP 500 for the first failCount requests.
type fakeOIDCDiscovery struct {
	server   *httptest.Server
	attempts atomic.Int64
	failures int64
}

func newFakeOIDCDiscovery(t *testing.T, failures int64) *fakeOIDCDiscovery {
	t.Helper()
	f := &fakeOIDCDiscovery{failures: failures}
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		if f.attempts.Add(1) <= f.failures {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"issuer":%q,"authorization_endpoint":"%s/auth",`+
			`"token_endpoint":"%s/token","jwks_uri":"%s/keys"}`,
			issuer, issuer, issuer, issuer)
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	f.server = httptest.NewServer(mux)
	issuer = f.server.URL
	t.Cleanup(f.server.Close)
	return f
}

// shrinkOIDCRetry makes the startup retry budget test-fast.
func shrinkOIDCRetry(t *testing.T) {
	t.Helper()
	prev := oidcStartupRetry
	oidcStartupRetry = func() util.RetryOptions {
		return util.RetryOptions{
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     5 * time.Millisecond,
			Multiplier:     2.0,
			JitterFraction: 0,
		}
	}
	t.Cleanup(func() { oidcStartupRetry = prev })
}

func TestBuildOIDCProvider_Disabled(t *testing.T) {
	cfg := testOperatorConfig()
	cfg.OIDC.Enabled = false

	provider := buildOIDCProvider(context.Background(), cfg, testLogger(), &metrics.NoopRecorder{})

	assert.Nil(t, provider)
}

func TestBuildOIDCProvider_DiscoverySucceeds(t *testing.T) {
	disco := newFakeOIDCDiscovery(t, 0)
	cfg := testOperatorConfig()
	cfg.OIDC.Enabled = true
	cfg.OIDC.IssuerURL = disco.server.URL
	cfg.OIDC.ClientID = "cloudberry"

	provider := buildOIDCProvider(context.Background(), cfg, testLogger(), &metrics.NoopRecorder{})

	require.NotNil(t, provider)
	assert.Equal(t, int64(1), disco.attempts.Load())
}

func TestBuildOIDCProvider_TransientFailureRetried(t *testing.T) {
	shrinkOIDCRetry(t)
	disco := newFakeOIDCDiscovery(t, 1) // 500 once, then 200
	cfg := testOperatorConfig()
	cfg.OIDC.Enabled = true
	cfg.OIDC.IssuerURL = disco.server.URL
	cfg.OIDC.ClientID = "cloudberry"
	cfg.OIDC.RoleMapping = map[string]string{"admin": "Admin"}

	provider := buildOIDCProvider(context.Background(), cfg, testLogger(), &metrics.NoopRecorder{})

	require.NotNil(t, provider)
	assert.GreaterOrEqual(t, disco.attempts.Load(), int64(2),
		"discovery must have been retried after the transient 500")
}

func TestBuildOIDCProvider_BudgetExhausted_LazyHandleReturned(t *testing.T) {
	shrinkOIDCRetry(t)
	disco := newFakeOIDCDiscovery(t, 1<<30) // never recovers within the budget
	cfg := testOperatorConfig()
	cfg.OIDC.Enabled = true
	cfg.OIDC.IssuerURL = disco.server.URL
	cfg.OIDC.ClientID = "cloudberry"

	provider := buildOIDCProvider(context.Background(), cfg, testLogger(), &metrics.NoopRecorder{})

	require.NotNil(t, provider,
		"B-7 contract: an exhausted startup budget must still return the lazy provider")
	assert.GreaterOrEqual(t, disco.attempts.Load(), int64(2))
}

func TestOIDCStartupRetryOpts_Budget(t *testing.T) {
	opts := oidcStartupRetryOpts()
	assert.Equal(t, 3, opts.MaxRetries)
	assert.Equal(t, time.Second, opts.InitialBackoff)
	assert.Equal(t, 5*time.Second, opts.MaxBackoff)
}

// ----------------------------------------------------------------------------
// startAPIServer
// ----------------------------------------------------------------------------

func TestStartAPIServer_CacheSyncTimeout(t *testing.T) {
	cleanRunEnv(t)
	mgr := newFakeManager(newFakeClient())
	mgr.cache = &fakeCache{synced: false}

	err := startAPIServer(context.Background(), testOperatorConfig(), mgr,
		&metrics.NoopRecorder{}, testLogger())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for manager cache to sync")
}

func TestStartAPIServer_AdminPasswordError(t *testing.T) {
	cleanRunEnv(t)
	t.Setenv("CLOUDBERRY_API_ADMIN_PASSWORD", "") // force the Secret path
	failingClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("api server down")
			},
		}).
		Build()
	mgr := newFakeManager(failingClient)

	err := startAPIServer(context.Background(), testOperatorConfig(), mgr,
		&metrics.NoopRecorder{}, testLogger())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolving admin password")
}

func TestStartAPIServer_PortBusy(t *testing.T) {
	cleanRunEnv(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	cfg := testOperatorConfig()
	cfg.APIAddress = ln.Addr().String()
	mgr := newFakeManager(newFakeClient())

	err = startAPIServer(context.Background(), cfg, mgr, &metrics.NoopRecorder{}, testLogger())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "address already in use")
}

func TestStartAPIServer_HappyPathShutdown(t *testing.T) {
	cleanRunEnv(t)
	cfg := testOperatorConfig()
	cfg.APIAddress = reserveAddr(t)
	mgr := newFakeManager(newFakeClient())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- startAPIServer(ctx, cfg, mgr, &metrics.NoopRecorder{}, testLogger())
	}()

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", cfg.APIAddress, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "graceful shutdown must not error")
	case <-time.After(10 * time.Second):
		t.Fatal("startAPIServer did not return after cancel")
	}
}

func TestStartAPIServer_ClientsetBuildFailure_WarnsAndServes(t *testing.T) {
	cleanRunEnv(t)
	cfg := testOperatorConfig()
	cfg.APIAddress = reserveAddr(t)
	mgr := newFakeManager(newFakeClient())
	// AuthProvider + ExecProvider together make kubernetes.NewForConfig fail;
	// the API server must still start (log streaming degraded).
	mgr.restCfg = &rest.Config{
		Host:         "http://127.0.0.1:1",
		AuthProvider: &clientcmdapi.AuthProviderConfig{Name: "x"},
		ExecProvider: &clientcmdapi.ExecConfig{Command: "x"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- startAPIServer(ctx, cfg, mgr, &metrics.NoopRecorder{}, testLogger())
	}()

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", cfg.APIAddress, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond,
		"server must come up even when the typed clientset cannot be built")

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("startAPIServer did not return after cancel")
	}
}

func TestStartAPIServer_OIDCEnabled(t *testing.T) {
	cleanRunEnv(t)
	disco := newFakeOIDCDiscovery(t, 0)
	cfg := testOperatorConfig()
	cfg.APIAddress = reserveAddr(t)
	cfg.OIDC.Enabled = true
	cfg.OIDC.IssuerURL = disco.server.URL
	cfg.OIDC.ClientID = "cloudberry"
	mgr := newFakeManager(newFakeClient())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- startAPIServer(ctx, cfg, mgr, &metrics.NoopRecorder{}, testLogger())
	}()

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", cfg.APIAddress, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), disco.attempts.Load(), "OIDC discovery must run at startup")

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("startAPIServer did not return after cancel")
	}
}

// ----------------------------------------------------------------------------
// setupWebhookCerts
// ----------------------------------------------------------------------------

// installDirectClientSeam routes setupWebhookCerts' direct client to a fake.
func installDirectClientSeam(t *testing.T, c client.Client, err error) {
	t.Helper()
	prev := newDirectClient
	newDirectClient = func(_ *rest.Config, _ client.Options) (client.Client, error) {
		return c, err
	}
	t.Cleanup(func() { newDirectClient = prev })
}

// webhookCertsConfig returns a config exercising the self-signed source.
func webhookCertsConfig() *config.OperatorConfig {
	cfg := testOperatorConfig()
	cfg.WebhookCertSource = "self-signed"
	cfg.WebhookCertSecretName = "cloudberry-webhook-certs"
	cfg.WebhookServiceName = "cloudberry-webhook"
	return cfg
}

func TestSetupWebhookCerts_SecretAbsent_GeneratesAndInjects(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")
	directClient := newFakeClient()
	installDirectClientSeam(t, directClient, nil)
	mgr := newFakeManager(newFakeClient())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	err := setupWebhookCerts(ctx, mgr, webhookCertsConfig(), testLogger(), &wg, &metrics.NoopRecorder{})
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, directClient.Get(context.Background(), types.NamespacedName{
		Name: "cloudberry-webhook-certs", Namespace: "test-ns",
	}, secret))
	assert.NotEmpty(t, secret.Data["tls.crt"], "certificate must be generated")
	assert.NotEmpty(t, secret.Data["tls.key"], "key must be generated")

	// The background rotation goroutine must terminate with the context.
	cancel()
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("cert rotation goroutine leaked past context cancellation")
	}
}

func TestSetupWebhookCerts_InvalidSecretRegenerated(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")
	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-webhook-certs",
			Namespace: "test-ns",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("not-a-pem"),
			"tls.key": []byte("not-a-pem"),
		},
	}
	directClient := newFakeClient(bad)
	installDirectClientSeam(t, directClient, nil)
	mgr := newFakeManager(newFakeClient())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	err := setupWebhookCerts(ctx, mgr, webhookCertsConfig(), testLogger(), &wg, &metrics.NoopRecorder{})
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, directClient.Get(context.Background(), types.NamespacedName{
		Name: "cloudberry-webhook-certs", Namespace: "test-ns",
	}, secret))
	assert.NotEqual(t, []byte("not-a-pem"), secret.Data["tls.crt"],
		"an invalid PEM secret must be regenerated")
}

func TestSetupWebhookCerts_DirectClientError(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")
	installDirectClientSeam(t, nil, fmt.Errorf("no kubeconfig"))
	mgr := newFakeManager(newFakeClient())

	var wg sync.WaitGroup
	err := setupWebhookCerts(context.Background(), mgr, webhookCertsConfig(),
		testLogger(), &wg, &metrics.NoopRecorder{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating direct API client")
}

func TestSetupWebhookCerts_EnsureCertificatesError(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")
	failing := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("secrets unavailable")
			},
		}).
		Build()
	installDirectClientSeam(t, failing, nil)
	mgr := newFakeManager(newFakeClient())

	var wg sync.WaitGroup
	err := setupWebhookCerts(context.Background(), mgr, webhookCertsConfig(),
		testLogger(), &wg, &metrics.NoopRecorder{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensuring webhook certificates")
}

func TestSetupWebhookCerts_VaultPKIClientError(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")
	installDirectClientSeam(t, newFakeClient(), nil)
	mgr := newFakeManager(newFakeClient())

	cfg := webhookCertsConfig()
	cfg.WebhookCertSource = "vault-pki"
	cfg.Vault.Address = "" // enabled-by-source without address → fast failure

	var wg sync.WaitGroup
	err := setupWebhookCerts(context.Background(), mgr, cfg, testLogger(), &wg, &metrics.NoopRecorder{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating vault client for webhook cert management")
}

func TestSetupWebhookCerts_NamespaceFallsBackToConfigThenDefault(t *testing.T) {
	// POD_NAMESPACE unset → cfg.Namespace → util.OperatorNamespace.
	t.Setenv("POD_NAMESPACE", "")
	directClient := newFakeClient()
	installDirectClientSeam(t, directClient, nil)
	mgr := newFakeManager(newFakeClient())

	cfg := webhookCertsConfig()
	cfg.Namespace = "" // both empty → compile-time default

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	err := setupWebhookCerts(ctx, mgr, cfg, testLogger(), &wg, &metrics.NoopRecorder{})
	require.NoError(t, err)

	secret := &corev1.Secret{}
	require.NoError(t, directClient.Get(context.Background(), types.NamespacedName{
		Name: "cloudberry-webhook-certs", Namespace: util.OperatorNamespace,
	}, secret))
}

func TestSetupWebhookCerts_InjectCABundleConflictRetried(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "test-ns")

	se := admissionWebhookFixture()
	conflicts := 1
	updateCalls := 0
	directClient := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(se).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.UpdateOption) error {
				updateCalls++
				if conflicts > 0 {
					conflicts--
					return fmt.Errorf("transient API error")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	installDirectClientSeam(t, directClient, nil)
	mgr := newFakeManager(newFakeClient())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	err := setupWebhookCerts(ctx, mgr, webhookCertsConfig(), testLogger(), &wg, &metrics.NoopRecorder{})

	require.NoError(t, err, "a single transient CA-injection failure must be retried away")
	assert.GreaterOrEqual(t, updateCalls, 2, "the failed update must have been retried")
}
