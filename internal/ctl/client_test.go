package ctl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOperatorClient(t *testing.T) {
	tests := []struct {
		name       string
		cfg        ClientConfig
		wantBase   string
		wantUser   string
		wantMethod string
	}{
		{
			name: "basic config",
			cfg: ClientConfig{
				BaseURL:    "http://localhost:8443",
				Username:   "admin",
				Password:   "secret",
				AuthMethod: "basic",
			},
			wantBase:   "http://localhost:8443",
			wantUser:   "admin",
			wantMethod: "basic",
		},
		{
			name: "trailing slash stripped",
			cfg: ClientConfig{
				BaseURL: "http://localhost:8443/",
			},
			wantBase: "http://localhost:8443",
		},
		{
			name: "multiple trailing slashes stripped",
			cfg: ClientConfig{
				BaseURL: "http://localhost:8443///",
			},
			wantBase: "http://localhost:8443",
		},
		{
			name: "custom timeout",
			cfg: ClientConfig{
				BaseURL: "http://localhost:8443",
				Timeout: 10 * time.Second,
			},
			wantBase: "http://localhost:8443",
		},
		{
			name: "zero timeout uses default",
			cfg: ClientConfig{
				BaseURL: "http://localhost:8443",
				Timeout: 0,
			},
			wantBase: "http://localhost:8443",
		},
		{
			name: "negative timeout uses default",
			cfg: ClientConfig{
				BaseURL: "http://localhost:8443",
				Timeout: -1 * time.Second,
			},
			wantBase: "http://localhost:8443",
		},
		{
			name: "oidc auth method",
			cfg: ClientConfig{
				BaseURL:    "http://localhost:8443",
				Password:   "oidc-token",
				AuthMethod: "oidc",
			},
			wantBase:   "http://localhost:8443",
			wantMethod: "oidc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewOperatorClient(tt.cfg)
			require.NotNil(t, client)
			assert.Equal(t, tt.wantBase, client.baseURL)
			if tt.wantUser != "" {
				assert.Equal(t, tt.wantUser, client.username)
			}
			if tt.wantMethod != "" {
				assert.Equal(t, tt.wantMethod, client.authMethod)
			}
			assert.NotNil(t, client.httpClient)
		})
	}
}

func TestOperatorClient_Get(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, apiPrefix+"/clusters", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []interface{}{}, "total": 0})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotNil(t, resp.Body)
	assert.Equal(t, float64(0), resp.Body["total"])
}

func TestOperatorClient_Post(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		assert.Equal(t, "test-cluster", body["name"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Post(context.Background(), "/clusters", map[string]string{"name": "test-cluster"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestOperatorClient_Put(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Put(context.Background(), "/clusters/test/config", map[string]string{"key": "value"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestOperatorClient_Patch(t *testing.T) {
	var body map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Patch(context.Background(),
		"/clusters/test/backups/schedule", map[string]string{"schedule": "0 3 * * *"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "0 3 * * *", body["schedule"])
}

func TestOperatorClient_Delete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Delete(context.Background(), "/clusters/test")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestOperatorClient_BasicAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "admin", user)
		assert.Equal(t, "secret", pass)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{
		BaseURL:    server.URL,
		Username:   "admin",
		Password:   "secret",
		AuthMethod: "basic",
	})
	resp, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestOperatorClient_BasicAuth_EmptyUsername(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No basic auth should be set when username is empty
		_, _, ok := r.BasicAuth()
		assert.False(t, ok)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{
		BaseURL:    server.URL,
		Username:   "",
		Password:   "secret",
		AuthMethod: "basic",
	})
	_, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
}

func TestOperatorClient_OIDCAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		assert.Equal(t, "Bearer my-oidc-token", authHeader)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{
		BaseURL:    server.URL,
		Password:   "my-oidc-token",
		AuthMethod: "oidc",
	})
	resp, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestOperatorClient_OIDCAuth_EmptyToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		assert.Empty(t, authHeader)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{
		BaseURL:    server.URL,
		Password:   "",
		AuthMethod: "oidc",
	})
	_, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
}

func TestOperatorClient_NoAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		assert.Empty(t, authHeader)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{
		BaseURL:    server.URL,
		AuthMethod: "",
	})
	_, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
}

func TestOperatorClient_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{
				"code":    "CLUSTER_NOT_FOUND",
				"message": "cluster not found",
			},
		})
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Get(context.Background(), "/clusters/nonexistent")
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	apiErr, ok := err.(*APIError)
	require.True(t, ok)
	assert.Equal(t, http.StatusNotFound, apiErr.StatusCode)
	assert.Equal(t, "CLUSTER_NOT_FOUND", apiErr.Code)
	assert.Equal(t, "cluster not found", apiErr.Message)
	assert.Contains(t, apiErr.Error(), "API error 404")
	assert.Contains(t, apiErr.Error(), "CLUSTER_NOT_FOUND")
}

func TestOperatorClient_APIError_NoBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Get(context.Background(), "/clusters")
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	apiErr, ok := err.(*APIError)
	require.True(t, ok)
	assert.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
	// Message should be the raw body (empty in this case)
	assert.Empty(t, apiErr.Message)
}

func TestOperatorClient_APIError_NonJSONBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("Bad Gateway"))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Get(context.Background(), "/clusters")
	require.Error(t, err)
	require.NotNil(t, resp)

	apiErr, ok := err.(*APIError)
	require.True(t, ok)
	assert.Equal(t, "Bad Gateway", apiErr.Message)
}

func TestOperatorClient_NonJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("plain text response"))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Get(context.Background(), "/clusters")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Nil(t, resp.Body) // JSON parsing failed, Body should be nil
	assert.Equal(t, "plain text response", string(resp.RawBody))
}

func TestOperatorClient_PostWithNilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		// Content-Type should not be set for nil body
		assert.Empty(t, r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	resp, err := client.Post(context.Background(), "/clusters/test/start", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestOperatorClient_ConnectionError(t *testing.T) {
	client := NewOperatorClient(ClientConfig{
		BaseURL: "http://127.0.0.1:1", // Port 1 should not be listening
		Timeout: 100 * time.Millisecond,
	})
	resp, err := client.Get(context.Background(), "/clusters")
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "executing request")
}

func TestOperatorClient_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	resp, err := client.Get(ctx, "/clusters")
	require.Error(t, err)
	assert.Nil(t, resp)
}

func TestOperatorClient_MarshalError(t *testing.T) {
	client := NewOperatorClient(ClientConfig{BaseURL: "http://localhost:8443"})
	// Channels cannot be marshaled to JSON
	resp, err := client.Post(context.Background(), "/clusters", make(chan int))
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "marshaling request body")
}

func TestAPIError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *APIError
		contains []string
	}{
		{
			name:     "full error",
			err:      &APIError{StatusCode: 404, Code: "NOT_FOUND", Message: "resource not found"},
			contains: []string{"404", "NOT_FOUND", "resource not found"},
		},
		{
			name:     "empty code",
			err:      &APIError{StatusCode: 500, Code: "", Message: "internal error"},
			contains: []string{"500", "internal error"},
		},
		{
			name:     "empty message",
			err:      &APIError{StatusCode: 429, Code: "RATE_LIMITED", Message: ""},
			contains: []string{"429", "RATE_LIMITED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errStr := tt.err.Error()
			for _, c := range tt.contains {
				assert.Contains(t, errStr, c)
			}
		})
	}
}

func TestClusterPath(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		namespace string
		expected  string
	}{
		{
			name:      "without namespace",
			cluster:   "my-cluster",
			namespace: "",
			expected:  "/clusters/my-cluster",
		},
		{
			name:      "with namespace",
			cluster:   "my-cluster",
			namespace: "production",
			expected:  "/clusters/my-cluster?namespace=production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClusterPath(tt.cluster, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClustersPath(t *testing.T) {
	assert.Equal(t, "/clusters", ClustersPath())
}

func TestClusterStatusPath(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		namespace string
		expected  string
	}{
		{
			name:      "without namespace",
			cluster:   "my-cluster",
			namespace: "",
			expected:  "/clusters/my-cluster/status",
		},
		{
			name:      "with namespace",
			cluster:   "my-cluster",
			namespace: "default",
			expected:  "/clusters/my-cluster/status?namespace=default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClusterStatusPath(tt.cluster, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClusterActionPath(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		action    string
		namespace string
		expected  string
	}{
		{
			name:      "start without namespace",
			cluster:   "my-cluster",
			action:    "start",
			namespace: "",
			expected:  "/clusters/my-cluster/start",
		},
		{
			name:      "stop with namespace",
			cluster:   "my-cluster",
			action:    "stop",
			namespace: "prod",
			expected:  "/clusters/my-cluster/stop?namespace=prod",
		},
		{
			name:      "restart",
			cluster:   "test",
			action:    "restart",
			namespace: "",
			expected:  "/clusters/test/restart",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClusterActionPath(tt.cluster, tt.action, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClusterSubresourcePath(t *testing.T) {
	tests := []struct {
		name        string
		cluster     string
		subresource string
		namespace   string
		expected    string
	}{
		{
			name:        "segments without namespace",
			cluster:     "my-cluster",
			subresource: "segments",
			namespace:   "",
			expected:    "/clusters/my-cluster/segments",
		},
		{
			name:        "config with namespace",
			cluster:     "my-cluster",
			subresource: "config",
			namespace:   "default",
			expected:    "/clusters/my-cluster/config?namespace=default",
		},
		{
			name:        "sessions",
			cluster:     "test",
			subresource: "sessions",
			namespace:   "",
			expected:    "/clusters/test/sessions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClusterSubresourcePath(tt.cluster, tt.subresource, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAPIResponse_Fields(t *testing.T) {
	resp := &APIResponse{
		StatusCode: 200,
		Body:       map[string]interface{}{"key": "value"},
		RawBody:    []byte(`{"key":"value"}`),
	}
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "value", resp.Body["key"])
	assert.Equal(t, `{"key":"value"}`, string(resp.RawBody))
}

func TestOperatorClient_RedirectPrevention(t *testing.T) {
	redirectCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCount++
		if redirectCount == 1 {
			http.Redirect(w, r, "/other", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewOperatorClient(ClientConfig{BaseURL: server.URL})
	// The client should not follow redirects (returns last response)
	resp, err := client.Get(context.Background(), "/clusters")
	// Should get the redirect response, not follow it
	require.NoError(t, err) // 302 < 400, so no API error
	assert.Equal(t, http.StatusFound, resp.StatusCode)
}
