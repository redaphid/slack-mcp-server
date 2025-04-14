package server

import (
	"fmt"
	"github.com/korotovsky/slack-mcp-server/internal/handler"
	"github.com/korotovsky/slack-mcp-server/internal/provider"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type MCPServer struct {
	server *server.MCPServer
}

func NewMCPServer(provider *provider.ApiProvider) *MCPServer {
	s := server.NewMCPServer(
		"Slack MCP Server",
		"1.0.0",
		server.WithLogging(),
		server.WithRecovery(),
	)

	conversationsHandler := handler.NewConversationsHandler(provider)

	s.AddTool(mcp.NewTool("conversationsHistory",
		mcp.WithDescription("Get messages from the channel"),
		mcp.WithString("channel",
			mcp.Required(),
			mcp.Description("Name of the channel"),
		),
		mcp.WithString("limit",
			mcp.DefaultString("28"),
			mcp.Description("Limit of messages to fetch"),
		),
	), conversationsHandler.ConversationsHistoryHandler)

	return &MCPServer{
		server: s,
	}
}

func (s *MCPServer) ServeSSE(addr string) *server.SSEServer {
	return server.NewSSEServer(s.server,
		server.WithBaseURL(fmt.Sprintf("http://%s", addr)),
		server.WithSSEContextFunc(authFromRequest),
	)
}

func (s *MCPServer) ServeStdio() error {
	return server.ServeStdio(s.server)
}
