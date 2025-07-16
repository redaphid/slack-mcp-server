package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// authKey is a custom context key for storing the auth token.
type authKey struct{}

// withAuthKey adds an auth key to the context.
func withAuthKey(ctx context.Context, auth string) context.Context {
	return context.WithValue(ctx, authKey{}, auth)
}

// Authenticate checks if the request is authenticated based on the provided context.
func validateToken(ctx context.Context) (bool, error) {
	// no configured token means no authentication
	keyA := os.Getenv("SLACK_MCP_SSE_API_KEY")
	if keyA == "" {
		return true, nil
	}

	keyB, ok := ctx.Value(authKey{}).(string)
	if !ok {
		return false, fmt.Errorf("missing auth")
	}

	if strings.HasPrefix(keyB, "Bearer ") {
		keyB = strings.TrimPrefix(keyB, "Bearer ")
	}

	if subtle.ConstantTimeCompare([]byte(keyA), []byte(keyB)) != 1 {
		return false, fmt.Errorf("invalid auth token")
	}

	return true, nil
}

// AuthFromRequest extracts the auth token from the request headers.
func AuthFromRequest(ctx context.Context, r *http.Request) context.Context {
	return withAuthKey(ctx, r.Header.Get("Authorization"))
}

// BuildMiddleware creates a middleware function that ensures authentication based on the provided transport type.
func BuildMiddleware(transport string) server.ToolHandlerMiddleware {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if authenticated, err := IsAuthenticated(ctx, transport); !authenticated {
				return nil, err
			}

			return next(ctx, req)
		}
	}
}

// IsAuthenticated public api
func IsAuthenticated(ctx context.Context, transport string) (bool, error) {
	if transport == "stdio" {
		return true, nil
	} else if transport == "sse" {
		authenticated, err := validateToken(ctx)

		if err != nil {
			return false, fmt.Errorf("authentication error: %w", err)
		}

		if !authenticated {
			return false, fmt.Errorf("unauthorized request")
		}

		return true, nil
	} else {
		return false, fmt.Errorf("unknown transport type: %s", transport)
	}
}
