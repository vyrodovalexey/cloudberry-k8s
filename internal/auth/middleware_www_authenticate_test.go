package auth

// Tests for T8/C3: every 401 from the auth middleware carries a generic body
// (no config-state disclosure — the concrete reason lives in logs/spans only)
// plus a WWW-Authenticate challenge advertising the configured scheme(s).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/httpjson"
)

// serve401 drives one request through the middleware handler chain.
func serve401(mw *AuthMiddleware, mutate func(*http.Request)) *httptest.ResponseRecorder {
	handler := mw.Handler()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decode401 decodes the error envelope from a 401 response.
func decode401(t *testing.T, rec *httptest.ResponseRecorder) httpjson.ErrorEnvelope {
	t.Helper()
	var envelope httpjson.ErrorEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&envelope))
	return envelope
}

func TestAuthMiddleware_Unauthorized_GenericBodyAndChallenge(t *testing.T) {
	basicOnly := &mockProvider{typeName: authMethodBasic}
	oidcOnly := &mockProvider{typeName: authMethodOIDC}

	tests := []struct {
		name          string
		basic         Provider
		oidc          Provider
		header        string
		wantChallenge string
		leakedText    string // internal reason that must NOT reach the client
	}{
		{
			name:          "missing header no providers",
			header:        "",
			wantChallenge: `Basic realm="cloudberry"`,
			leakedText:    "missing Authorization header",
		},
		{
			name:          "unknown scheme with basic configured",
			basic:         basicOnly,
			header:        "Digest username=x",
			wantChallenge: `Basic realm="cloudberry"`,
			leakedText:    "unsupported authorization type",
		},
		{
			name:          "basic not configured with oidc configured",
			oidc:          oidcOnly,
			header:        "Basic dXNlcjpwYXNz",
			wantChallenge: "Bearer",
			leakedText:    "basic auth not configured",
		},
		{
			name:          "bearer not configured with basic configured",
			basic:         basicOnly,
			header:        "Bearer tok",
			wantChallenge: `Basic realm="cloudberry"`,
			leakedText:    "OIDC auth not configured",
		},
		{
			name:          "both providers advertise both schemes",
			basic:         basicOnly,
			oidc:          oidcOnly,
			header:        "Digest username=x",
			wantChallenge: `Basic realm="cloudberry", Bearer`,
			leakedText:    "unsupported authorization type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logBuf, nil))
			recorder := &countingRecorder{}
			mw := NewAuthMiddleware(tt.basic, tt.oidc, logger, recorder)

			rec := serve401(mw, func(r *http.Request) {
				if tt.header != "" {
					r.Header.Set(headerAuthorization, tt.header)
				}
			})

			require.Equal(t, http.StatusUnauthorized, rec.Code)
			envelope := decode401(t, rec)
			assert.Equal(t, "UNAUTHORIZED", envelope.Error.Code)
			assert.Equal(t, msgAuthRequired, envelope.Error.Message,
				"401 body must be the generic message only")
			assert.NotContains(t, envelope.Error.Message, tt.leakedText,
				"internal reason must not be disclosed to the client")
			assert.Equal(t, tt.wantChallenge, rec.Header().Get(headerWWWAuthenticate))

			// The concrete reason stays observable server-side.
			assert.Contains(t, logBuf.String(), tt.leakedText,
				"the concrete reason must be logged")

			// Honest metrics: exactly one failure per rejected request.
			require.Len(t, recorder.authAttempts, 1)
			assert.Equal(t, "failure", recorder.authAttempts[0].result)
		})
	}
}

func TestAuthMiddleware_AuthenticateFailure_KeepsBodyAddsChallenge(t *testing.T) {
	basicProvider := &mockProvider{err: fmt.Errorf("bad credentials"), typeName: authMethodBasic}
	recorder := &countingRecorder{}
	mw := NewAuthMiddleware(basicProvider, nil, nil, recorder)

	rec := serve401(mw, func(r *http.Request) {
		r.SetBasicAuth("admin", "wrong")
	})

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	envelope := decode401(t, rec)
	assert.Equal(t, "authentication failed", envelope.Error.Message)
	assert.NotContains(t, envelope.Error.Message, "bad credentials")
	assert.Equal(t, `Basic realm="cloudberry"`, rec.Header().Get(headerWWWAuthenticate))

	// Honest metrics: exactly one failure with the resolved method.
	require.Len(t, recorder.authAttempts, 1)
	assert.Equal(t, authMethodBasic, recorder.authAttempts[0].method)
	assert.Equal(t, "failure", recorder.authAttempts[0].result)
}

func TestGuestHandler_Unauthorized_ChallengePresent(t *testing.T) {
	basicProvider := &mockProvider{typeName: authMethodBasic}
	mw := NewAuthMiddleware(basicProvider, nil, nil, nil)

	t.Run("guest disabled missing header", func(t *testing.T) {
		handler := mw.GuestHandler(func(*http.Request) bool { return false })(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Equal(t, http.StatusUnauthorized, rec.Code)
		envelope := decode401(t, rec)
		assert.Equal(t, msgAuthRequired, envelope.Error.Message)
		assert.Equal(t, `Basic realm="cloudberry"`, rec.Header().Get(headerWWWAuthenticate))
	})

	t.Run("guest enabled write method rejected", func(t *testing.T) {
		handler := mw.GuestHandler(func(*http.Request) bool { return true })(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))

		require.Equal(t, http.StatusUnauthorized, rec.Code)
		envelope := decode401(t, rec)
		assert.Equal(t, "authentication required for write operations", envelope.Error.Message)
		assert.Equal(t, `Basic realm="cloudberry"`, rec.Header().Get(headerWWWAuthenticate))
	})
}

func TestChallengeValue(t *testing.T) {
	basicProvider := &mockProvider{typeName: authMethodBasic}
	oidcProvider := &mockProvider{typeName: authMethodOIDC}

	tests := []struct {
		name  string
		basic Provider
		oidc  Provider
		want  string
	}{
		{name: "none configured falls back to basic", want: `Basic realm="cloudberry"`},
		{name: "basic only", basic: basicProvider, want: `Basic realm="cloudberry"`},
		{name: "oidc only", oidc: oidcProvider, want: "Bearer"},
		{name: "both", basic: basicProvider, oidc: oidcProvider,
			want: `Basic realm="cloudberry", Bearer`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := NewAuthMiddleware(tt.basic, tt.oidc, nil, nil)
			assert.Equal(t, tt.want, mw.challengeValue())
		})
	}
}
