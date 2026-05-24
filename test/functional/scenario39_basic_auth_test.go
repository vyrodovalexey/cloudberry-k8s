//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 39: Basic Authentication Flow
// ============================================================================
//
// This scenario tests the basic authentication flow including:
// 39a — Admin user validation (correct/wrong password, missing/malformed header,
//
//	timing attack prevention, no password in logs)
//
// 39b — DB role validation (unknown user, multiple users with different permissions)
// ============================================================================

// Scenario39BasicAuthSuite tests the basic authentication flow.
type Scenario39BasicAuthSuite struct {
	suite.Suite
	store         *auth.InMemoryCredentialStore
	basicProvider *auth.BasicAuthProvider
	middleware    *auth.AuthMiddleware
	logBuf        *bytes.Buffer
	logger        *slog.Logger
}

func TestFunctional_Scenario39(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario39BasicAuthSuite))
}

func (s *Scenario39BasicAuthSuite) SetupTest() {
	s.store = auth.NewInMemoryCredentialStore()
	s.store.SetCredentials("admin", "admin-secret", auth.PermissionAdmin)
	s.store.SetCredentials("operator", "operator-pass", auth.PermissionOperator)
	s.store.SetCredentials("opbasic", "opbasic-pass", auth.PermissionOperatorBasic)
	s.store.SetCredentials("viewer", "viewer-pass", auth.PermissionBasic)
	s.store.SetCredentials("reader", "reader-pass", auth.PermissionSelfOnly)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s.basicProvider = auth.NewBasicAuthProvider(s.store, s.logger)
	s.middleware = auth.NewAuthMiddleware(s.basicProvider, nil, s.logger, &metrics.NoopRecorder{})
}

// --- 39a: Admin user validation ---

// TestFunctional_Scenario39a_AdminAuth_CorrectPassword verifies that valid admin
// credentials produce an Identity with Username="admin", Permission=Admin, AuthMethod="basic".
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_CorrectPassword() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "admin-secret")

	identity, err := s.basicProvider.Authenticate(context.Background(), req)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), identity)
	assert.Equal(s.T(), "admin", identity.Username)
	assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission)
	assert.Equal(s.T(), "basic", identity.AuthMethod)
}

// TestFunctional_Scenario39a_AdminAuth_WrongPassword verifies that wrong admin
// password returns 401 via the middleware.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_WrongPassword() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "wrong-password")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"wrong password should return 401")
}

// TestFunctional_Scenario39a_AdminAuth_NoPasswordInLogs verifies that the password
// is never logged when authentication fails.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_NoPasswordInLogs() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "super-secret-password-12345")

	_, _ = s.basicProvider.Authenticate(context.Background(), req)

	logOutput := s.logBuf.String()
	assert.NotContains(s.T(), logOutput, "super-secret-password-12345",
		"password should never appear in log output")
	// Verify that the username IS logged (for audit purposes).
	assert.Contains(s.T(), logOutput, "admin",
		"username should appear in log output for audit")
}

// TestFunctional_Scenario39a_AdminAuth_TimingAttack verifies that when a user is
// not found, the provider still performs a bcrypt comparison (against dummyHash)
// to prevent timing attacks. We verify this by checking that the operation takes
// a non-trivial amount of time (bcrypt is intentionally slow).
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_TimingAttack() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("nonexistent-user", "some-password")

	start := time.Now()
	identity, err := s.basicProvider.Authenticate(context.Background(), req)
	elapsed := time.Since(start)

	require.Error(s.T(), err)
	assert.Nil(s.T(), identity)
	assert.Contains(s.T(), err.Error(), "invalid credentials")

	// bcrypt comparison should take at least a few milliseconds even on fast hardware.
	// This verifies the dummy hash comparison is actually happening.
	assert.Greater(s.T(), elapsed.Milliseconds(), int64(1),
		"user-not-found path should take non-trivial time due to dummy hash comparison")
}

// TestFunctional_Scenario39a_AdminAuth_MissingHeader verifies that a request
// without an Authorization header returns 401.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_MissingHeader() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"missing auth header should return 401")
}

// TestFunctional_Scenario39a_AdminAuth_MalformedHeader verifies that a request
// with a malformed Basic auth header returns 401.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39a_AdminAuth_MalformedHeader() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	malformedHeaders := []string{
		"Basic not-valid-base64!!!",
		"Basic ",
		"BasicInvalid",
		"Digest username=test",
	}

	for _, header := range malformedHeaders {
		s.Run(header, func() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", header)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
				"malformed auth header %q should return 401", header)
		})
	}
}

// --- 39b: DB role validation (current behavior) ---

// TestFunctional_Scenario39b_DBRole_NotInStore verifies that an unknown user
// returns 401 (DB role validation not implemented, only in-memory store).
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39b_DBRole_NotInStore() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("unknown-db-user", "some-password")

	identity, err := s.basicProvider.Authenticate(context.Background(), req)
	require.Error(s.T(), err)
	assert.Nil(s.T(), identity)
	assert.Contains(s.T(), err.Error(), "invalid credentials")
}

// TestFunctional_Scenario39b_DBRole_MultipleUsers verifies that multiple users
// in the credential store with different permissions all authenticate correctly.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39b_DBRole_MultipleUsers() {
	users := []struct {
		username           string
		password           string
		expectedPermission auth.PermissionLevel
		expectedString     string
	}{
		{"admin", "admin-secret", auth.PermissionAdmin, "Admin"},
		{"operator", "operator-pass", auth.PermissionOperator, "Operator"},
		{"opbasic", "opbasic-pass", auth.PermissionOperatorBasic, "Operator Basic"},
		{"viewer", "viewer-pass", auth.PermissionBasic, "Basic"},
		{"reader", "reader-pass", auth.PermissionSelfOnly, "Self Only"},
	}

	for _, u := range users {
		s.Run(u.username, func() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(u.username, u.password)

			identity, err := s.basicProvider.Authenticate(context.Background(), req)
			require.NoError(s.T(), err)
			require.NotNil(s.T(), identity)
			assert.Equal(s.T(), u.username, identity.Username)
			assert.Equal(s.T(), u.expectedPermission, identity.Permission)
			assert.Equal(s.T(), u.expectedString, identity.Permission.String())
			assert.Equal(s.T(), "basic", identity.AuthMethod)
		})
	}
}

// --- Integration tests ---

// TestFunctional_Scenario39_BasicAuthFlowCases runs the BasicAuthFlowCases catalog.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39_BasicAuthFlowCases() {
	testCases := cases.BasicAuthFlowCases()
	require.NotEmpty(s.T(), testCases, "BasicAuthFlowCases should return test cases")

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			s.T().Log(tc.Description)

			// Handle the malformed header case specially.
			if tc.Name == "39a_malformed_auth_header" {
				handler := s.middleware.Handler()(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Set("Authorization", "Basic not-valid-base64!!!")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			// Handle missing header case.
			if tc.Name == "39a_missing_auth_header" {
				handler := s.middleware.Handler()(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			// Standard Basic auth cases via middleware.
			var capturedIdentity *auth.Identity
			handler := s.middleware.Handler()(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					capturedIdentity = auth.IdentityFromContext(r.Context())
					w.WriteHeader(http.StatusOK)
				}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(tc.Username, tc.Password)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if !tc.ExpectSuccess {
				assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
				return
			}

			assert.Equal(s.T(), tc.ExpectStatusCode, rec.Code)
			require.NotNil(s.T(), capturedIdentity)
			assert.Equal(s.T(), tc.ExpectedAuthMethod, capturedIdentity.AuthMethod)
			assert.Equal(s.T(), tc.ExpectedPermission, capturedIdentity.Permission.String())
		})
	}
}

// TestFunctional_Scenario39_ProviderType verifies that Type() returns "basic".
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39_ProviderType() {
	assert.Equal(s.T(), "basic", s.basicProvider.Type(),
		"BasicAuthProvider.Type() should return 'basic'")
}

// TestFunctional_Scenario39_IdentityFields verifies that all Identity fields
// are populated correctly after successful authentication.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39_IdentityFields() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "admin-secret")

	identity, err := s.basicProvider.Authenticate(context.Background(), req)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), identity)

	// Verify all expected fields.
	assert.Equal(s.T(), "admin", identity.Username,
		"Username should be set")
	assert.Equal(s.T(), auth.PermissionAdmin, identity.Permission,
		"Permission should be Admin")
	assert.Equal(s.T(), "basic", identity.AuthMethod,
		"AuthMethod should be 'basic'")

	// Verify fields that are not set by basic auth.
	assert.Empty(s.T(), identity.Email,
		"Email should be empty for basic auth")
	assert.Nil(s.T(), identity.Groups,
		"Groups should be nil for basic auth")
	assert.Nil(s.T(), identity.Roles,
		"Roles should be nil for basic auth")
	assert.True(s.T(), identity.TokenExpiry.IsZero(),
		"TokenExpiry should be zero for basic auth")
}

// TestFunctional_Scenario39_MiddlewareWithAPIServer verifies that the basic auth
// middleware integrates correctly with the API server.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39_MiddlewareWithAPIServer() {
	cluster := testutil.NewClusterBuilder("s39-basic-auth", "cloudberry-test").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)
	server := api.NewServer(k8sEnv.Client, s.middleware, nil, &metrics.NoopRecorder{}, nil, 0)
	defer server.Close()
	handler := server.Handler()

	// Authenticated request should succeed.
	s.Run("authenticated_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", "admin-secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusOK, rec.Code,
			"authenticated request should succeed")
	})

	// Unauthenticated request should fail.
	s.Run("unauthenticated_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated request should return 401")
	})

	// Wrong password should fail.
	s.Run("wrong_password_request", func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/clusters", nil)
		req.SetBasicAuth("admin", "wrong")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"wrong password should return 401")
	})
}

// TestFunctional_Scenario39_ErrorResponseJSON verifies that auth error responses
// are in the expected JSON format.
func (s *Scenario39BasicAuthSuite) TestFunctional_Scenario39_ErrorResponseJSON() {
	handler := s.middleware.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
	assert.Contains(s.T(), rec.Header().Get("Content-Type"), "application/json",
		"error response should be JSON")

	var errResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&errResp))
	errObj, ok := errResp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should have 'error' object")
	assert.Equal(s.T(), "UNAUTHORIZED", errObj["code"])
	assert.NotEmpty(s.T(), errObj["message"])
}
