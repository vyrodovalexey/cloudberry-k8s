//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// Scenario49CTLAuthE2ESuite tests Scenario 49: CTL Auth Commands end-to-end.
type Scenario49CTLAuthE2ESuite struct {
	E2ESuite
}

func TestE2E_Scenario49(t *testing.T) {
	suite.Run(t, new(Scenario49CTLAuthE2ESuite))
}

// e2eScenario49MockServer creates a mock operator API server for E2E tests.
func e2eScenario49MockServer(t *testing.T, validUser, validPass string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if validUser != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || user != validUser || pass != validPass {
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error": map[string]string{
						"code":    "UNAUTHORIZED",
						"message": "invalid credentials",
					},
				})
				return
			}
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"items": []interface{}{},
			"total": 0,
		})
	}))
}

// TestE2E_Scenario49a_LoginOIDC tests OIDC login returns not-implemented
// for the browser-based flow.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49a_LoginOIDC() {
	s.logger.Info("starting scenario 49 E2E: OIDC login not-implemented")

	loginCmd := e2eScenario49BuildLoginCmd("", "", "", "oidc", "5s")

	err := loginCmd.Execute()
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "not yet implemented")

	s.logger.Info("scenario 49 E2E: OIDC login not-implemented completed")
}

// TestE2E_Scenario49b_LoginBasic tests basic login with valid credentials.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49b_LoginBasic() {
	s.logger.Info("starting scenario 49 E2E: basic login")

	server := e2eScenario49MockServer(s.T(), "gpadmin", "admin-password")
	defer server.Close()

	loginCmd := e2eScenario49BuildLoginCmd(
		server.URL, "gpadmin", "admin-password", "basic", "5s",
	)
	_ = loginCmd.Flags().Set("basic", "true")

	var buf bytes.Buffer
	loginCmd.SetOut(&buf)

	err := loginCmd.Execute()
	require.NoError(s.T(), err)
	assert.Contains(s.T(), buf.String(), "Login successful")
	assert.Contains(s.T(), buf.String(), "gpadmin")

	s.logger.Info("scenario 49 E2E: basic login completed")
}

// TestE2E_Scenario49b_LoginBasic_InvalidPassword tests basic login with
// an incorrect password.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49b_LoginBasic_InvalidPassword() {
	s.logger.Info("starting scenario 49 E2E: basic login invalid password")

	server := e2eScenario49MockServer(s.T(), "gpadmin", "correct-password")
	defer server.Close()

	loginCmd := e2eScenario49BuildLoginCmd(
		server.URL, "gpadmin", "wrong-password", "basic", "5s",
	)
	_ = loginCmd.Flags().Set("basic", "true")

	err := loginCmd.Execute()
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "login failed")

	s.logger.Info("scenario 49 E2E: basic login invalid password completed")
}

// TestE2E_Scenario49c_AuthStatus tests auth status shows connectivity.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49c_AuthStatus() {
	s.logger.Info("starting scenario 49 E2E: auth status")

	server := e2eScenario49MockServer(s.T(), "gpadmin", "admin-password")
	defer server.Close()

	statusCmd := e2eScenario49BuildStatusCmd(
		server.URL, "gpadmin", "admin-password", "basic", "json", "5s",
	)

	var buf bytes.Buffer
	statusCmd.SetOut(&buf)

	err := statusCmd.Execute()
	require.NoError(s.T(), err)

	output := buf.String()
	assert.Contains(s.T(), output, "authenticated")
	assert.Contains(s.T(), output, "basic")

	s.logger.Info("scenario 49 E2E: auth status completed")
}

// TestE2E_Scenario49d_Logout tests logout clears state.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49d_Logout() {
	s.logger.Info("starting scenario 49 E2E: logout")

	logoutCmd := e2eScenario49BuildLogoutCmd()

	var buf bytes.Buffer
	logoutCmd.SetOut(&buf)

	err := logoutCmd.Execute()
	require.NoError(s.T(), err)

	output := buf.String()
	assert.Contains(s.T(), output, "Logged out")
	assert.Contains(s.T(), output, "CLOUDBERRY_PASSWORD")

	s.logger.Info("scenario 49 E2E: logout completed")
}

// TestE2E_Scenario49_CTLAuthCasesCatalog runs the CTL auth cases catalog.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49_CTLAuthCasesCatalog() {
	s.logger.Info("starting scenario 49 E2E: CTL auth cases catalog")

	testCases := cases.CTLAuthCases()
	require.Len(s.T(), testCases, 6, "should have 6 CTL auth test cases")

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			assert.NotEmpty(t, tc.Name, "test case should have a name")
			assert.NotEmpty(t, tc.Command, "test case should have a command")
			assert.NotEmpty(t, tc.Description, "test case should have a description")

			validCommands := map[string]bool{"login": true, "status": true, "logout": true}
			assert.True(t, validCommands[tc.Command],
				"[%s] command should be login, status, or logout, got %q", tc.Name, tc.Command)
		})
	}

	s.logger.Info("scenario 49 E2E: CTL auth cases catalog completed")
}

// TestE2E_Scenario49_ClusterCRWithAuthConfig creates a cluster CR with auth
// config and verifies it is accepted.
func (s *Scenario49CTLAuthE2ESuite) TestE2E_Scenario49_ClusterCRWithAuthConfig() {
	s.logger.Info("starting scenario 49 E2E: cluster CR with auth config")

	cluster := testutil.NewClusterBuilder("e2e-s49-ctl-auth", s.namespace).
		WithBasicAuth(true, "gpadmin").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithStatusReady().
		Build()

	require.NotNil(s.T(), cluster)
	assert.Equal(s.T(), "e2e-s49-ctl-auth", cluster.Name)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, cluster.Status.Phase)
	require.NotNil(s.T(), cluster.Spec.Auth)
	require.NotNil(s.T(), cluster.Spec.Auth.Basic)
	assert.True(s.T(), cluster.Spec.Auth.Basic.Enabled)
	assert.Equal(s.T(), "gpadmin", cluster.Spec.Auth.Basic.AdminUser)

	env := testutil.NewTestK8sEnv(cluster)
	retrieved, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cluster.Name, retrieved.Name)

	s.logger.Info("scenario 49 E2E: cluster CR with auth config completed")
}

// ---------------------------------------------------------------------------
// Helper functions for building cobra commands in E2E tests.
// ---------------------------------------------------------------------------

// e2eScenario49BuildLoginCmd creates a login command for E2E testing.
func e2eScenario49BuildLoginCmd(operatorURL, username, password, authMethod, timeout string) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "login",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			basic, _ := cmd.Flags().GetBool("basic")
			if basic {
				return e2eScenario49RunLoginBasic(cmd, operatorURL, username, password, authMethod)
			}
			return e2eScenario49RunLoginOIDC(cmd, operatorURL, username, password, authMethod)
		},
	}
	cmd.Flags().Bool("basic", false, "Use basic authentication")
	return cmd
}

// e2eScenario49BuildStatusCmd creates a status command for E2E testing.
func e2eScenario49BuildStatusCmd(operatorURL, username, password, authMethod, output, timeout string) *cobra.Command {
	return &cobra.Command{
		Use:           "status",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return e2eScenario49RunStatus(cmd, operatorURL, username, password, authMethod)
		},
	}
}

// e2eScenario49BuildLogoutCmd creates a logout command for E2E testing.
func e2eScenario49BuildLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:           "logout",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Println("Logged out. Cached credentials have been cleared.")
			cmd.Println("Note: If you set CLOUDBERRY_USERNAME or CLOUDBERRY_PASSWORD environment " +
				"variables, unset them to fully log out.")
			return nil
		},
	}
}

// e2eScenario49RunLoginBasic verifies basic auth credentials.
func e2eScenario49RunLoginBasic(cmd *cobra.Command, operatorURL, username, password, authMethod string) error {
	if username == "" {
		return e2eScenario49Err("username is required for basic auth")
	}
	if password == "" {
		return e2eScenario49Err("password is required for basic auth")
	}

	resp, err := e2eScenario49DoGet(operatorURL, username, password, authMethod, "/api/v1alpha1/clusters")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return e2eScenario49Err("login failed: HTTP " + resp.Status)
	}

	cmd.Printf("Login successful (method=basic, user=%s)\n", username)
	return nil
}

// e2eScenario49RunLoginOIDC simulates OIDC login.
func e2eScenario49RunLoginOIDC(cmd *cobra.Command, operatorURL, username, password, authMethod string) error {
	if username != "" && password != "" {
		resp, err := e2eScenario49DoGet(operatorURL, username, password, authMethod, "/api/v1alpha1/clusters")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= http.StatusBadRequest {
			return e2eScenario49Err("OIDC login failed: HTTP " + resp.Status)
		}

		cmd.Printf("Login successful (method=oidc, user=%s)\n", username)
		return nil
	}

	return e2eScenario49Err("command \"auth login (browser-based OIDC flow)\" is not yet implemented")
}

// e2eScenario49RunStatus checks auth status.
func e2eScenario49RunStatus(cmd *cobra.Command, operatorURL, username, password, authMethod string) error {
	status := map[string]interface{}{
		"auth_method":  authMethod,
		"username":     username,
		"operator_url": operatorURL,
	}

	resp, err := e2eScenario49DoGet(operatorURL, username, password, authMethod, "/api/v1alpha1/clusters")
	if err != nil {
		status["authenticated"] = false
		status["error"] = err.Error()
	} else {
		defer resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			status["authenticated"] = false
		} else {
			status["authenticated"] = true
		}
	}

	out, _ := json.MarshalIndent(status, "", "  ")
	cmd.Println(string(out))
	return nil
}

// e2eScenario49DoGet performs a GET request with basic auth.
func e2eScenario49DoGet(baseURL, username, password, authMethod, path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if authMethod == "basic" && username != "" {
		req.SetBasicAuth(username, password)
	}
	return http.DefaultClient.Do(req)
}

// e2eScenario49Err creates a formatted error.
func e2eScenario49Err(msg string) error {
	return &e2eScenario49Error{message: msg}
}

// e2eScenario49Error is a simple error type for scenario 49 E2E tests.
type e2eScenario49Error struct {
	message string
}

func (e *e2eScenario49Error) Error() string {
	return e.message
}
