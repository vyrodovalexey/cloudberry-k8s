package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// authObsRecorder captures the auth-related metric calls (C-8).
type authObsRecorder struct {
	metrics.NoopRecorder
	mu             sync.Mutex
	discoveries    []string
	verifyDuration int
	attempts       []string
	userinfos      []string
}

func (r *authObsRecorder) RecordOIDCUserinfo(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.userinfos = append(r.userinfos, result)
}

func (r *authObsRecorder) RecordOIDCDiscovery(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.discoveries = append(r.discoveries, result)
}

func (r *authObsRecorder) ObserveAuthTokenVerifyDuration(_ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verifyDuration++
}

func (r *authObsRecorder) RecordAuthAttempt(method, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, method+"/"+result)
}

// setupUserinfoOIDCServer builds a fake OIDC server whose discovery document
// advertises a userinfo endpoint. userinfoStatus controls the userinfo
// response code; userinfoBody is the JSON claims payload.
func setupUserinfoOIDCServer(
	t *testing.T,
	key *rsa.PrivateKey,
	userinfoStatus *atomic.Int32,
	userinfoBody *atomic.Value,
	userinfoCalls *atomic.Int32,
) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	var serverURL string

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{
			"issuer": %q,
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys",
			"userinfo_endpoint": "%s/userinfo"
		}`, serverURL, serverURL, serverURL, serverURL, serverURL)
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
		_, _ = fmt.Fprintf(w,
			`{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","n":%q,"e":%q,"kid":"test-key"}]}`, n, e)
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, _ *http.Request) {
		userinfoCalls.Add(1)
		status := int(userinfoStatus.Load())
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		body, _ := userinfoBody.Load().(string)
		_, _ = w.Write([]byte(body))
	})

	server := httptest.NewServer(mux)
	serverURL = server.URL
	t.Cleanup(server.Close)
	return server
}

// userinfoTestFixture bundles the provider + fake server controls.
type userinfoTestFixture struct {
	provider      *OIDCProvider
	server        *httptest.Server
	key           *rsa.PrivateKey
	userinfoCalls *atomic.Int32
	userinfoCode  *atomic.Int32
	userinfoBody  *atomic.Value
}

func newUserinfoFixture(t *testing.T) *userinfoTestFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	var calls, code atomic.Int32
	var body atomic.Value
	code.Store(http.StatusOK)
	body.Store(`{"sub":"user-123","realm_access":{"roles":["admin"]}}`)

	server := setupUserinfoOIDCServer(t, key, &code, &body, &calls)

	cfg := OIDCConfig{
		IssuerURL:       server.URL,
		ClientID:        "test-client",
		RoleClaimPath:   "realm_access.roles",
		RoleClaimSource: RoleClaimSourceUserinfo,
		RoleMapping:     map[string]string{"admin": "Admin", "viewer": "Basic"},
		RoleMatchMode:   "exact",
	}
	provider, err := NewOIDCProvider(context.Background(), cfg, nil)
	require.NoError(t, err)

	return &userinfoTestFixture{
		provider:      provider,
		server:        server,
		key:           key,
		userinfoCalls: &calls,
		userinfoCode:  &code,
		userinfoBody:  &body,
	}
}

// bearerRequest builds a request with a signed token carrying base claims.
func (f *userinfoTestFixture) bearerRequest(t *testing.T, extra map[string]interface{}) *http.Request {
	t.Helper()
	claims := map[string]interface{}{
		"iss": f.server.URL,
		"sub": "user-123",
		"aud": "test-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	}
	for k, v := range extra {
		claims[k] = v
	}
	token := signJWT(t, f.key, claims)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

// TestOIDCProvider_UserinfoRoleSource verifies B-8: with
// role-claim-source=userinfo, roles come from the userinfo endpoint.
func TestOIDCProvider_UserinfoRoleSource(t *testing.T) {
	f := newUserinfoFixture(t)

	// ID token carries NO roles; userinfo carries admin.
	identity, err := f.provider.Authenticate(context.Background(), f.bearerRequest(t, nil))
	require.NoError(t, err)
	assert.Equal(t, PermissionAdmin, identity.Permission)
	assert.Equal(t, int32(1), f.userinfoCalls.Load(), "userinfo endpoint must be called")
}

// TestOIDCProvider_UserinfoFallbackOnError verifies the documented fallback:
// a failing userinfo endpoint falls back to ID-token claims.
func TestOIDCProvider_UserinfoFallbackOnError(t *testing.T) {
	f := newUserinfoFixture(t)
	f.userinfoCode.Store(http.StatusInternalServerError)

	identity, err := f.provider.Authenticate(context.Background(), f.bearerRequest(t,
		map[string]interface{}{
			"realm_access": map[string]interface{}{"roles": []interface{}{"viewer"}},
		}))
	require.NoError(t, err)
	assert.Equal(t, PermissionBasic, identity.Permission,
		"must fall back to the verified ID-token roles")
}

// TestOIDCProvider_UserinfoFallbackOnMissingClaim verifies the fallback when
// the userinfo response carries no roles at the configured claim path.
func TestOIDCProvider_UserinfoFallbackOnMissingClaim(t *testing.T) {
	f := newUserinfoFixture(t)
	f.userinfoBody.Store(`{"sub":"user-123"}`)

	identity, err := f.provider.Authenticate(context.Background(), f.bearerRequest(t,
		map[string]interface{}{
			"realm_access": map[string]interface{}{"roles": []interface{}{"viewer"}},
		}))
	require.NoError(t, err)
	assert.Equal(t, PermissionBasic, identity.Permission)
}

// TestOIDCProvider_IDTokenSourceRegression verifies the id_token source path
// never calls userinfo (regression for B-8).
func TestOIDCProvider_IDTokenSourceRegression(t *testing.T) {
	f := newUserinfoFixture(t)
	f.provider.config.RoleClaimSource = RoleClaimSourceIDToken

	identity, err := f.provider.Authenticate(context.Background(), f.bearerRequest(t,
		map[string]interface{}{
			"realm_access": map[string]interface{}{"roles": []interface{}{"admin"}},
		}))
	require.NoError(t, err)
	assert.Equal(t, PermissionAdmin, identity.Permission)
	assert.Equal(t, int32(0), f.userinfoCalls.Load(), "userinfo must not be called for id_token source")
}

// TestOIDCProvider_RecordUserinfo verifies the recordUserinfo helper: it is a
// nil-safe no-op without a recorder and forwards the result to a configured
// recorder (covers the non-nil branch at oidc.go:285).
func TestOIDCProvider_RecordUserinfo(t *testing.T) {
	// Nil recorder: must not panic and must be a no-op.
	t.Run("nil recorder is safe", func(t *testing.T) {
		p := &OIDCProvider{}
		assert.NotPanics(t, func() {
			p.recordUserinfo("success")
			p.recordUserinfo("error")
		})
	})

	// Non-nil recorder: results are forwarded.
	t.Run("forwards results to recorder", func(t *testing.T) {
		rec := &authObsRecorder{}
		p := &OIDCProvider{}
		p.SetRecorder(rec)

		p.recordUserinfo("success")
		p.recordUserinfo("error")

		assert.Equal(t, []string{"success", "error"}, rec.userinfos)
	})
}

// TestOIDCProvider_UserinfoMetricViaAuthenticate drives the real UserInfo
// fetch path end-to-end with a configured recorder, asserting the success and
// error outcomes are recorded through recordUserinfo.
func TestOIDCProvider_UserinfoMetricViaAuthenticate(t *testing.T) {
	rec := &authObsRecorder{}

	// Success: userinfo endpoint returns 200 with admin roles.
	f := newUserinfoFixture(t)
	f.provider.SetRecorder(rec)
	_, err := f.provider.Authenticate(context.Background(), f.bearerRequest(t, nil))
	require.NoError(t, err)

	// Error: userinfo endpoint returns 500 (fetch fails -> "error" recorded).
	fErr := newUserinfoFixture(t)
	fErr.provider.SetRecorder(rec)
	fErr.userinfoCode.Store(http.StatusInternalServerError)
	_, err = fErr.provider.Authenticate(context.Background(), fErr.bearerRequest(t,
		map[string]interface{}{
			"realm_access": map[string]interface{}{"roles": []interface{}{"viewer"}},
		}))
	require.NoError(t, err)

	assert.Contains(t, rec.userinfos, "success")
	assert.Contains(t, rec.userinfos, "error")
}

// TestLazyOIDCProvider_DiscoveryMetric verifies C-8: discovery attempts are
// counted with error and success results.
func TestLazyOIDCProvider_DiscoveryMetric(t *testing.T) {
	rec := &authObsRecorder{}

	// Failure first: unreachable issuer.
	lazyBad := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: "http://127.0.0.1:1", ClientID: "c",
	}, nil, rec)
	require.Error(t, lazyBad.Init(context.Background()))

	// Then success against the fake server.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := setupOIDCTestServer(t, key)
	lazyGood := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL: server.URL, ClientID: "c",
	}, nil, rec)
	require.NoError(t, lazyGood.Init(context.Background()))

	assert.Equal(t, []string{"error", "success"}, rec.discoveries)
}

// TestTokenVerifyDurationMetric verifies C-8: Bearer verification latency is
// observed on both success and failure.
func TestTokenVerifyDurationMetric(t *testing.T) {
	rec := &authObsRecorder{}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := setupOIDCTestServer(t, key)

	lazy := NewLazyOIDCProvider(OIDCConfig{
		IssuerURL:     server.URL,
		ClientID:      "test-client",
		RoleClaimPath: "realm_access.roles",
	}, nil, rec)
	require.NoError(t, lazy.Init(context.Background()))

	token := signJWT(t, key, map[string]interface{}{
		"iss": server.URL, "sub": "u", "aud": "test-client",
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	_, err = lazy.Authenticate(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, rec.verifyDuration)

	// Invalid token also observes the latency.
	req.Header.Set("Authorization", "Bearer not-a-token")
	_, err = lazy.Authenticate(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, 2, rec.verifyDuration)
}

// TestAuthSpans verifies D-5: the authenticate dispatch span carries bounded
// method/result attributes, the OIDC verify child span exists, the userinfo
// span exists, and NO attribute contains credentials or usernames.
func TestAuthSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	f := newUserinfoFixture(t)
	rec := &authObsRecorder{}
	store := NewInMemoryCredentialStoreWithCost(4)
	store.SetCredentials("alice", "wonderland-secret", PermissionAdmin)
	basic := NewBasicAuthProvider(store, nil)
	mw := NewAuthMiddleware(basic, f.provider, nil, rec)

	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Basic auth request.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("alice", "wonderland-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Bearer request (userinfo role source → verify + userinfo child spans).
	req = f.bearerRequest(t, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Failed bearer request.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer garbage")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	spans := sr.Ended()
	var sawBasic, sawBearerOK, sawBearerFail, sawVerify, sawUserinfo bool
	for _, s := range spans {
		attrs := map[string]string{}
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.Emit()
			// PII / credential hygiene: no attribute may contain the
			// password, the token material, or the username.
			assert.NotContains(t, a.Value.Emit(), "wonderland-secret")
			assert.NotContains(t, a.Value.Emit(), "alice")
			assert.NotContains(t, strings.ToLower(string(a.Key)), "password")
			assert.NotContains(t, strings.ToLower(string(a.Key)), "token")
		}
		switch s.Name() {
		case "auth.authenticate":
			switch {
			case attrs["auth.method"] == "basic" && attrs["auth.result"] == "success":
				sawBasic = true
			case attrs["auth.method"] == "oidc" && attrs["auth.result"] == "success":
				sawBearerOK = true
			case attrs["auth.method"] == "oidc" && attrs["auth.result"] == "failure":
				sawBearerFail = true
			}
		case "auth.oidc.verify":
			sawVerify = true
		case "auth.oidc.userinfo":
			sawUserinfo = true
		}
	}
	assert.True(t, sawBasic, "basic auth span with method=basic missing")
	assert.True(t, sawBearerOK, "bearer success span missing")
	assert.True(t, sawBearerFail, "bearer failure span missing")
	assert.True(t, sawVerify, "auth.oidc.verify child span missing")
	assert.True(t, sawUserinfo, "auth.oidc.userinfo span missing")
}
