package auth

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

const (
	headerAuthorization = "Authorization"
	prefixBasic         = "Basic "
	prefixBearer        = "Bearer "
	authMethodBasic     = "basic"
	authMethodOIDC      = "oidc"
)

// Middleware is an HTTP middleware function.
type Middleware func(http.Handler) http.Handler

// AuthMiddleware creates an authentication middleware that routes requests
// to the appropriate provider based on the Authorization header.
type AuthMiddleware struct {
	basicProvider Provider
	oidcProvider  Provider
	logger        *slog.Logger
	recorder      metrics.Recorder
}

// NewAuthMiddleware creates a new AuthMiddleware.
func NewAuthMiddleware(
	basicProvider Provider,
	oidcProvider Provider,
	logger *slog.Logger,
	recorder metrics.Recorder,
) *AuthMiddleware {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuthMiddleware{
		basicProvider: basicProvider,
		oidcProvider:  oidcProvider,
		logger:        logger,
		recorder:      recorder,
	}
}

// Handler returns the authentication middleware handler.
func (m *AuthMiddleware) Handler() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provider, method, err := m.resolveProvider(r)
			if err != nil {
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHORIZED", err.Error())
				return
			}

			identity, authErr := provider.Authenticate(r.Context(), r)
			if authErr != nil {
				m.recordFailure(method, authErr, r.RemoteAddr)
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication failed")
				return
			}

			if m.recorder != nil {
				m.recorder.RecordAuthAttempt(method, "success")
			}

			ctx := ContextWithIdentity(r.Context(), identity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// resolveProvider determines the auth provider based on the Authorization header.
func (m *AuthMiddleware) resolveProvider(r *http.Request) (Provider, string, error) {
	authHeader := r.Header.Get(headerAuthorization)
	if authHeader == "" {
		return nil, "", fmt.Errorf("missing Authorization header")
	}

	switch {
	case strings.HasPrefix(authHeader, prefixBasic):
		if m.basicProvider == nil {
			return nil, "", fmt.Errorf("basic auth not configured")
		}
		return m.basicProvider, authMethodBasic, nil
	case strings.HasPrefix(authHeader, prefixBearer):
		if m.oidcProvider == nil {
			return nil, "", fmt.Errorf("OIDC auth not configured")
		}
		return m.oidcProvider, authMethodOIDC, nil
	default:
		return nil, "", fmt.Errorf("unsupported authorization type")
	}
}

// recordFailure records an authentication failure.
func (m *AuthMiddleware) recordFailure(method string, err error, remoteAddr string) {
	if m.recorder != nil {
		m.recorder.RecordAuthAttempt(method, "failure")
	}
	m.logger.Warn("authentication failed",
		"method", method,
		"error", err,
		"remote_addr", remoteAddr,
	)
}

// authMethodGuest is the authentication method name for guest access.
const authMethodGuest = "guest"

// guestUsername is the username assigned to guest identities.
const guestUsername = "guest"

// GuestHandler returns middleware that allows unauthenticated access with a guest identity.
// When guestAccessEnabled returns true and no Authorization header is present,
// a guest identity with PermissionBasic is created for read-only methods (GET, HEAD, OPTIONS).
// When guestAccessEnabled returns false, missing auth returns 401.
// POST/PUT/DELETE methods always require authentication regardless of guestAccess.
func (m *AuthMiddleware) GuestHandler(guestAccessEnabled func(r *http.Request) bool) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If Authorization header is present, use normal auth flow.
			if r.Header.Get(headerAuthorization) != "" {
				m.Handler()(next).ServeHTTP(w, r)
				return
			}

			// No auth header — check if guest access is allowed.
			if !guestAccessEnabled(r) {
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing Authorization header")
				return
			}

			// Guest access only for read-only methods (GET, HEAD, OPTIONS).
			if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHORIZED",
					"authentication required for write operations")
				return
			}

			// Create guest identity with Basic permission.
			guestIdentity := &Identity{
				Username:   guestUsername,
				Permission: PermissionBasic,
				AuthMethod: authMethodGuest,
			}

			m.logger.Info("guest access granted",
				"remote_addr", r.RemoteAddr,
				"path", r.URL.Path,
				"method", r.Method,
			)

			ctx := ContextWithIdentity(r.Context(), guestIdentity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequirePermission creates a middleware that enforces a minimum permission level.
func RequirePermission(level PermissionLevel, loggers ...*slog.Logger) Middleware {
	var logger *slog.Logger
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity := IdentityFromContext(r.Context())
			if identity == nil {
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHORIZED", "not authenticated")
				return
			}

			if identity.Permission < level {
				if logger != nil {
					logger.Warn("permission denied",
						"username", identity.Username,
						"method", identity.AuthMethod,
						"source_ip", r.RemoteAddr,
						"required_permission", level.String(),
						"actual_permission", identity.Permission.String(),
						"path", r.URL.Path,
						"http_method", r.Method,
					)
				}
				writeErrorResponse(
					w, http.StatusForbidden, "FORBIDDEN",
					"insufficient permissions: requires "+level.String(),
				)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// SecurityHeaders adds security headers to all responses.
func SecurityHeaders() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Security-Policy", "default-src 'self'")
			w.Header().Set("Permissions-Policy", "camera=(), microphone=()")
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("X-XSS-Protection", "1; mode=block")
			next.ServeHTTP(w, r)
		})
	}
}

// errorResponse represents an API error response.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

// errorDetail contains error details.
type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeErrorResponse writes a JSON error response.
func writeErrorResponse(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := errorResponse{
		Error: errorDetail{
			Code:    code,
			Message: message,
		},
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode auth error response", "error", err)
	}
}
