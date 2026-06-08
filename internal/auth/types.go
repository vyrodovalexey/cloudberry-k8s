// Package auth provides authentication and authorization for the cloudberry operator API.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// PermissionLevel represents the user's access tier.
type PermissionLevel int

const (
	// PermissionSelfOnly allows viewing own queries and sessions.
	PermissionSelfOnly PermissionLevel = iota
	// PermissionBasic allows viewing cluster state.
	PermissionBasic
	// PermissionOperatorBasic allows basic operations and viewing all sessions.
	PermissionOperatorBasic
	// PermissionOperator allows cluster operations.
	PermissionOperator
	// PermissionAdmin allows full access.
	PermissionAdmin
)

// String returns the string representation of a PermissionLevel.
func (p PermissionLevel) String() string {
	switch p {
	case PermissionSelfOnly:
		return "Self Only"
	case PermissionBasic:
		return "Basic"
	case PermissionOperatorBasic:
		return "Operator Basic"
	case PermissionOperator:
		return "Operator"
	case PermissionAdmin:
		return "Admin"
	default:
		return fmt.Sprintf("Unknown(%d)", int(p))
	}
}

// ParsePermissionLevel parses a string into a PermissionLevel.
func ParsePermissionLevel(s string) PermissionLevel {
	switch s {
	case "Self Only":
		return PermissionSelfOnly
	case "Basic":
		return PermissionBasic
	case "Operator Basic":
		return PermissionOperatorBasic
	case "Operator":
		return PermissionOperator
	case "Admin":
		return PermissionAdmin
	default:
		return PermissionSelfOnly
	}
}

// Identity represents an authenticated user.
type Identity struct {
	// Username is the authenticated user's name.
	Username string
	// Email is the user's email address.
	Email string
	// Groups are the user's group memberships.
	Groups []string
	// Roles are the user's role assignments.
	Roles []string
	// Permission is the resolved permission level.
	Permission PermissionLevel
	// AuthMethod is the authentication method used ("basic" or "oidc").
	AuthMethod string
	// TokenExpiry is the token expiration time (for OIDC).
	TokenExpiry time.Time
}

// Provider defines the authentication provider interface.
type Provider interface {
	// Authenticate validates the request and returns the authenticated identity.
	Authenticate(ctx context.Context, r *http.Request) (*Identity, error)
	// Type returns the provider type name.
	Type() string
}

type identityContextKey struct{}

// ContextWithIdentity adds an Identity to the context.
func ContextWithIdentity(ctx context.Context, identity *Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, identity)
}

// IdentityFromContext extracts an Identity from the context.
// Returns nil if no identity is found.
func IdentityFromContext(ctx context.Context) *Identity {
	identity, _ := ctx.Value(identityContextKey{}).(*Identity)
	return identity
}
