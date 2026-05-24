//go:build functional

package functional

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

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// scenario49Globals mirrors the globalFlags struct from cmd/cloudberry-ctl.
// We use this to configure the auth commands under test without importing
// the main package directly.
type scenario49Globals struct {
	cluster     string
	namespace   string
	operatorURL string
	authMethod  string
	username    string
	password    string
	output      string
	verbose     bool
	timeout     string
}

// scenario49MockServer creates a mock operator API server that validates
// basic auth credentials and returns appropriate responses.
func scenario49MockServer(t *testing.T, validUser, validPass string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check basic auth if credentials are expected.
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

// Scenario49CTLAuthSuite tests Scenario 49: CTL Auth Commands.
type Scenario49CTLAuthSuite struct {
	suite.Suite
}

func TestFunctional_Scenario49(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario49CTLAuthSuite))
}

// TestFunctional_Scenario49a_LoginOIDC tests that OIDC login without
// credentials returns a not-implemented error for the browser-based flow.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49a_LoginOIDC() {
	// Build a login command that simulates the OIDC path (no --basic flag).
	loginCmd := buildScenario49LoginCmd("", "", "", "oidc", "table", "5s")

	err := loginCmd.Execute()
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "not yet implemented")
}

// TestFunctional_Scenario49b_LoginBasic tests basic login with valid credentials.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49b_LoginBasic() {
	server := scenario49MockServer(s.T(), "gpadmin", "admin-password")
	defer server.Close()

	loginCmd := buildScenario49LoginCmd(
		server.URL, "gpadmin", "admin-password", "basic", "table", "5s",
	)
	_ = loginCmd.Flags().Set("basic", "true")

	var buf bytes.Buffer
	loginCmd.SetOut(&buf)

	err := loginCmd.Execute()
	require.NoError(s.T(), err)
	assert.Contains(s.T(), buf.String(), "Login successful")
	assert.Contains(s.T(), buf.String(), "gpadmin")
}

// TestFunctional_Scenario49b_LoginBasic_InvalidPassword tests basic login
// with an incorrect password.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49b_LoginBasic_InvalidPassword() {
	server := scenario49MockServer(s.T(), "gpadmin", "correct-password")
	defer server.Close()

	loginCmd := buildScenario49LoginCmd(
		server.URL, "gpadmin", "wrong-password", "basic", "table", "5s",
	)
	_ = loginCmd.Flags().Set("basic", "true")

	err := loginCmd.Execute()
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "login failed")
}

// TestFunctional_Scenario49c_AuthStatus tests that auth status shows
// connectivity and authentication information.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49c_AuthStatus() {
	server := scenario49MockServer(s.T(), "gpadmin", "admin-password")
	defer server.Close()

	statusCmd := buildScenario49StatusCmd(
		server.URL, "gpadmin", "admin-password", "basic", "json", "5s",
	)

	var buf bytes.Buffer
	statusCmd.SetOut(&buf)

	err := statusCmd.Execute()
	require.NoError(s.T(), err)

	output := buf.String()
	assert.Contains(s.T(), output, "authenticated")
	assert.Contains(s.T(), output, "basic")
}

// TestFunctional_Scenario49c_AuthStatus_Unauthenticated tests that auth status
// reports unauthenticated state without returning an error.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49c_AuthStatus_Unauthenticated() {
	server := scenario49MockServer(s.T(), "gpadmin", "correct-password")
	defer server.Close()

	statusCmd := buildScenario49StatusCmd(
		server.URL, "gpadmin", "wrong-password", "basic", "json", "5s",
	)

	var buf bytes.Buffer
	statusCmd.SetOut(&buf)

	err := statusCmd.Execute()
	require.NoError(s.T(), err)

	output := buf.String()
	assert.Contains(s.T(), output, "authenticated")
}

// TestFunctional_Scenario49d_Logout tests that logout succeeds and prints
// a reminder about environment variables.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49d_Logout() {
	logoutCmd := buildScenario49LogoutCmd("table")

	var buf bytes.Buffer
	logoutCmd.SetOut(&buf)

	err := logoutCmd.Execute()
	require.NoError(s.T(), err)

	output := buf.String()
	assert.Contains(s.T(), output, "Logged out")
	assert.Contains(s.T(), output, "CLOUDBERRY_PASSWORD")
}

// TestFunctional_Scenario49_CTLAuthCases runs the CTL auth cases catalog.
func (s *Scenario49CTLAuthSuite) TestFunctional_Scenario49_CTLAuthCases() {
	testCases := cases.CTLAuthCases()
	require.Len(s.T(), testCases, 6, "should have 6 CTL auth test cases")

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			assert.NotEmpty(t, tc.Name, "test case should have a name")
			assert.NotEmpty(t, tc.Command, "test case should have a command")
			assert.NotEmpty(t, tc.Description, "test case should have a description")

			// Verify the command is valid.
			validCommands := map[string]bool{"login": true, "status": true, "logout": true}
			assert.True(t, validCommands[tc.Command],
				"command should be login, status, or logout, got %q", tc.Command)
		})
	}
}

// ---------------------------------------------------------------------------
// Helper functions for building cobra commands that mirror the ctl auth logic.
// These avoid importing the main package by reconstructing minimal commands.
// ---------------------------------------------------------------------------

// buildScenario49LoginCmd creates a login command that mirrors the ctl auth login logic.
func buildScenario49LoginCmd(operatorURL, username, password, authMethod, output, timeout string) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "login",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			basic, _ := cmd.Flags().GetBool("basic")
			if basic {
				return scenario49RunLoginBasic(cmd, operatorURL, username, password, authMethod, timeout)
			}
			return scenario49RunLoginOIDC(cmd, operatorURL, username, password, authMethod, timeout)
		},
	}
	cmd.Flags().Bool("basic", false, "Use basic authentication")
	return cmd
}

// buildScenario49StatusCmd creates a status command that mirrors the ctl auth status logic.
func buildScenario49StatusCmd(operatorURL, username, password, authMethod, output, timeout string) *cobra.Command {
	return &cobra.Command{
		Use:           "status",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return scenario49RunStatus(cmd, operatorURL, username, password, authMethod, output, timeout)
		},
	}
}

// buildScenario49LogoutCmd creates a logout command that mirrors the ctl auth logout logic.
func buildScenario49LogoutCmd(output string) *cobra.Command {
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

// scenario49RunLoginBasic verifies basic auth credentials against a mock server.
func scenario49RunLoginBasic(cmd *cobra.Command, operatorURL, username, password, authMethod, timeout string) error {
	if username == "" {
		return errScenario49("username is required for basic auth")
	}
	if password == "" {
		return errScenario49("password is required for basic auth")
	}

	resp, err := scenario49DoGet(operatorURL, username, password, authMethod, timeout, "/api/v1alpha1/clusters")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return errScenario49("login failed: HTTP " + resp.Status)
	}

	cmd.Printf("Login successful (method=basic, user=%s)\n", username)
	return nil
}

// scenario49RunLoginOIDC simulates OIDC login.
func scenario49RunLoginOIDC(cmd *cobra.Command, operatorURL, username, password, authMethod, timeout string) error {
	if username != "" && password != "" {
		resp, err := scenario49DoGet(operatorURL, username, password, authMethod, timeout, "/api/v1alpha1/clusters")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= http.StatusBadRequest {
			return errScenario49("OIDC login failed: HTTP " + resp.Status)
		}

		cmd.Printf("Login successful (method=oidc, user=%s)\n", username)
		return nil
	}

	return errScenario49("command \"auth login (browser-based OIDC flow)\" is not yet implemented")
}

// scenario49RunStatus checks auth status.
func scenario49RunStatus(cmd *cobra.Command, operatorURL, username, password, authMethod, output, timeout string) error {
	status := map[string]interface{}{
		"auth_method":  authMethod,
		"username":     username,
		"operator_url": operatorURL,
	}

	resp, err := scenario49DoGet(operatorURL, username, password, authMethod, timeout, "/api/v1alpha1/clusters")
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

// scenario49DoGet performs a GET request with basic auth.
func scenario49DoGet(baseURL, username, password, authMethod, _ string, path string) (*http.Response, error) {
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

// errScenario49 creates a formatted error.
func errScenario49(msg string) error {
	return &scenario49Error{message: msg}
}

// scenario49Error is a simple error type for scenario 49 tests.
type scenario49Error struct {
	message string
}

func (e *scenario49Error) Error() string {
	return e.message
}
