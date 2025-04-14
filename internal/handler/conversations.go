package handler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/korotovsky/slack-mcp-server/internal/provider"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
)

type ConversationsHandler struct {
	apiProvider *provider.ApiProvider
}

func NewConversationsHandler(apiProvider *provider.ApiProvider) *ConversationsHandler {
	return &ConversationsHandler{
		apiProvider: apiProvider,
	}
}

func (ch *ConversationsHandler) ConversationsHistoryHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	channel, ok := request.Params.Arguments["channel"].(string)
	if !ok {
		return nil, errors.New("channel must be a string")
	}

	api, err := ch.apiProvider.Provide()
	if err != nil {
		return nil, err
	}

	params := slack.GetConversationHistoryParameters{
		ChannelID: channel,
		Limit:     3,
	}
	messages, err := api.GetConversationHistoryContext(ctx, &params)
	if err != nil {
		return nil, err
	}

	data, err := json.MarshalIndent(messages.Messages, "", "    ")
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(string(data)), nil
}
