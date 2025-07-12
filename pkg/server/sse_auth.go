package server

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

// authFromRequest extracts the auth token from the request headers.
func authFromRequest(ctx context.Context, r *http.Request) context.Context {
	return withAuthKey(ctx, r.Header.Get("Authorization"))
}

// Authenticate checks if the request is authenticated based on the provided context.
func authenticate(ctx context.Context) (bool, error) {
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

// public api middleware that checks for authentication
func buildMiddleware(transport string) server.ToolHandlerMiddleware {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if transport == "stdio" {
				return next(ctx, req)
			} else if transport == "sse" {
				authenticated, err := authenticate(ctx)

				if err != nil {
					return nil, fmt.Errorf("authentication error: %w", err)
				}

				if !authenticated {
					return nil, fmt.Errorf("unauthorized request")
				}

				return next(ctx, req)
			} else {
				return nil, fmt.Errorf("unknown transport type: %s", transport)
			}
		}
	}
}
