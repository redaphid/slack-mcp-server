package handler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/korotovsky/slack-mcp-server/internal/provider"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
	"strconv"
)

type Message struct {
	User    string `json:"user"`
	Text    string `json:"text"`
	Channel string `json:"channel"`
	Time    string `json:"time"`
}

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

	limit, ok := request.Params.Arguments["limit"].(string)
	if !ok {
		return nil, errors.New("channel must be a string")
	}

	api, err := ch.apiProvider.Provide()
	if err != nil {
		return nil, err
	}

	var (
		messagesLimit int
	)
	if limit != "" {
		messagesLimit, err = strconv.Atoi(limit)
		if err != nil {
			return nil, errors.New("can not parse limit")
		}
	}

	params := slack.GetConversationHistoryParameters{
		ChannelID: channel,
		Limit:     messagesLimit,
	}
	messages, err := api.GetConversationHistoryContext(ctx, &params)
	if err != nil {
		return nil, err
	}

	var messageList []Message
	for _, message := range messages.Messages {
		messageList = append(messageList, Message{
			User:    message.User,
			Text:    message.Text,
			Channel: channel,
			Time:    message.Timestamp,
		})
	}

	data, err := json.MarshalIndent(messageList, "", "    ")
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(string(data)), nil
}
