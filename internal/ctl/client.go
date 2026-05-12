// Package ctl provides the operator API client for the cloudberry-ctl CLI.
package ctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// apiPrefix is the base path for all operator API endpoints.
	apiPrefix = "/api/v1alpha1"
	// defaultTimeout is the default HTTP client timeout.
	defaultTimeout = 30 * time.Second
)

// OperatorClient makes HTTP calls to the operator API.
type OperatorClient struct {
	baseURL    string
	httpClient *http.Client
	username   string
	password   string
	authMethod string
}

// ClientConfig holds configuration for creating an OperatorClient.
type ClientConfig struct {
	// BaseURL is the operator API base URL (e.g., "http://localhost:8443").
	BaseURL string
	// Username is the basic auth username.
	Username string
	// Password is the basic auth password.
	Password string
	// AuthMethod is the authentication method ("basic" or "oidc").
	AuthMethod string
	// Timeout is the HTTP client timeout.
	Timeout time.Duration
}

// NewOperatorClient creates a new OperatorClient.
func NewOperatorClient(cfg ClientConfig) *OperatorClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &OperatorClient{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: timeout,
			// Prevent open redirect attacks by disabling automatic redirects.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		username:   cfg.Username,
		password:   cfg.Password,
		authMethod: cfg.AuthMethod,
	}
}

// APIResponse represents a generic API response.
type APIResponse struct {
	StatusCode int
	Body       map[string]interface{}
	RawBody    []byte
}

// APIError represents an error returned by the operator API.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d (%s): %s", e.StatusCode, e.Code, e.Message)
}

// Get performs a GET request to the operator API.
func (c *OperatorClient) Get(ctx context.Context, path string) (*APIResponse, error) {
	return c.do(ctx, http.MethodGet, path, nil)
}

// Post performs a POST request to the operator API.
func (c *OperatorClient) Post(ctx context.Context, path string, body interface{}) (*APIResponse, error) {
	return c.do(ctx, http.MethodPost, path, body)
}

// Put performs a PUT request to the operator API.
func (c *OperatorClient) Put(ctx context.Context, path string, body interface{}) (*APIResponse, error) {
	return c.do(ctx, http.MethodPut, path, body)
}

// Delete performs a DELETE request to the operator API.
func (c *OperatorClient) Delete(ctx context.Context, path string) (*APIResponse, error) {
	return c.do(ctx, http.MethodDelete, path, nil)
}

// buildBodyReader marshals body to JSON and returns an io.Reader.
// Returns nil reader when body is nil.
func buildBodyReader(body interface{}) (io.Reader, error) {
	if body == nil {
		return nil, nil
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}
	return strings.NewReader(string(jsonBytes)), nil
}

// parseAPIError extracts an APIError from the response.
func parseAPIError(resp *APIResponse) *APIError {
	apiErr := &APIError{StatusCode: resp.StatusCode}
	if errObj, ok := resp.Body["error"].(map[string]interface{}); ok {
		if code, ok := errObj["code"].(string); ok {
			apiErr.Code = code
		}
		if msg, ok := errObj["message"].(string); ok {
			apiErr.Message = msg
		}
	}
	if apiErr.Message == "" {
		apiErr.Message = string(resp.RawBody)
	}
	return apiErr
}

// do performs an HTTP request to the operator API.
func (c *OperatorClient) do(ctx context.Context, method, path string, body interface{}) (*APIResponse, error) {
	url := c.baseURL + apiPrefix + path

	bodyReader, err := buildBodyReader(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	c.applyAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	apiResp := &APIResponse{
		StatusCode: resp.StatusCode,
		RawBody:    rawBody,
	}

	// Parse JSON body.
	if len(rawBody) > 0 {
		var parsed map[string]interface{}
		if jsonErr := json.Unmarshal(rawBody, &parsed); jsonErr == nil {
			apiResp.Body = parsed
		}
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return apiResp, parseAPIError(apiResp)
	}

	return apiResp, nil
}

// applyAuth applies authentication headers to the request.
func (c *OperatorClient) applyAuth(req *http.Request) {
	switch c.authMethod {
	case "basic":
		if c.username != "" {
			req.SetBasicAuth(c.username, c.password)
		}
	case "oidc":
		// OIDC token would be set as Bearer token.
		if c.password != "" {
			req.Header.Set("Authorization", "Bearer "+c.password)
		}
	}
}

// ClusterPath returns the API path for a cluster resource.
func ClusterPath(name, namespace string) string {
	path := fmt.Sprintf("/clusters/%s", name)
	if namespace != "" {
		path += "?namespace=" + namespace
	}
	return path
}

// ClustersPath returns the API path for listing clusters.
func ClustersPath() string {
	return "/clusters"
}

// ClusterStatusPath returns the API path for cluster status.
func ClusterStatusPath(name, namespace string) string {
	path := fmt.Sprintf("/clusters/%s/status", name)
	if namespace != "" {
		path += "?namespace=" + namespace
	}
	return path
}

// ClusterActionPath returns the API path for a cluster action.
func ClusterActionPath(name, action, namespace string) string {
	path := fmt.Sprintf("/clusters/%s/%s", name, action)
	if namespace != "" {
		path += "?namespace=" + namespace
	}
	return path
}

// ClusterSubresourcePath returns the API path for a cluster subresource.
func ClusterSubresourcePath(name, subresource, namespace string) string {
	path := fmt.Sprintf("/clusters/%s/%s", name, subresource)
	if namespace != "" {
		path += "?namespace=" + namespace
	}
	return path
}
