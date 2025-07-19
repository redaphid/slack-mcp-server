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
	"go.uber.org/zap"
)

// authKey is a custom context key for storing the auth token.
type authKey struct{}

// withAuthKey adds an auth key to the context.
func withAuthKey(ctx context.Context, auth string) context.Context {
	return context.WithValue(ctx, authKey{}, auth)
}

// Authenticate checks if the request is authenticated based on the provided context.
func validateToken(ctx context.Context, logger *zap.Logger) (bool, error) {
	// no configured token means no authentication
	keyA := os.Getenv("SLACK_MCP_SSE_API_KEY")
	if keyA == "" {
		logger.Debug("No SSE API key configured, skipping authentication")
		return true, nil
	}

	keyB, ok := ctx.Value(authKey{}).(string)
	if !ok {
		logger.Warn("Missing auth token in context")
		return false, fmt.Errorf("missing auth")
	}

	logger.Debug("Validating auth token",
		zap.Bool("has_bearer_prefix", strings.HasPrefix(keyB, "Bearer ")),
	)

	if strings.HasPrefix(keyB, "Bearer ") {
		keyB = strings.TrimPrefix(keyB, "Bearer ")
	}

	if subtle.ConstantTimeCompare([]byte(keyA), []byte(keyB)) != 1 {
		logger.Warn("Invalid auth token provided")
		return false, fmt.Errorf("invalid auth token")
	}

	logger.Debug("Auth token validated successfully")
	return true, nil
}

// AuthFromRequest extracts the auth token from the request headers.
func AuthFromRequest(logger *zap.Logger) func(context.Context, *http.Request) context.Context {
	return func(ctx context.Context, r *http.Request) context.Context {
		authHeader := r.Header.Get("Authorization")
		logger.Debug("Extracting auth from request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Bool("has_auth_header", authHeader != ""),
		)
		return withAuthKey(ctx, authHeader)
	}
}

// BuildMiddleware creates a middleware function that ensures authentication based on the provided transport type.
func BuildMiddleware(transport string, logger *zap.Logger) server.ToolHandlerMiddleware {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			logger.Debug("Auth middleware invoked",
				zap.String("transport", transport),
				zap.String("tool", req.Params.Name),
			)

			if authenticated, err := IsAuthenticated(ctx, transport, logger); !authenticated {
				logger.Error("Authentication failed",
					zap.String("transport", transport),
					zap.String("tool", req.Params.Name),
					zap.Error(err),
				)
				return nil, err
			}

			logger.Debug("Authentication successful",
				zap.String("transport", transport),
				zap.String("tool", req.Params.Name),
			)

			return next(ctx, req)
		}
	}
}

// IsAuthenticated public api
func IsAuthenticated(ctx context.Context, transport string, logger *zap.Logger) (bool, error) {
	switch transport {
	case "stdio":
		logger.Debug("STDIO transport - no authentication required")
		return true, nil

	case "sse":
		logger.Debug("SSE transport - validating token")
		authenticated, err := validateToken(ctx, logger)

		if err != nil {
			logger.Error("SSE authentication error", zap.Error(err))
			return false, fmt.Errorf("authentication error: %w", err)
		}

		if !authenticated {
			logger.Warn("SSE unauthorized request")
			return false, fmt.Errorf("unauthorized request")
		}

		logger.Debug("SSE authentication successful")
		return true, nil

	default:
		logger.Error("Unknown transport type", zap.String("transport", transport))
		return false, fmt.Errorf("unknown transport type: %s", transport)
	}
}
