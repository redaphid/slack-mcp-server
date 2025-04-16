package handler

import (
	"context"
	"errors"
	"github.com/gocarina/gocsv"
	"github.com/korotovsky/slack-mcp-server/internal/provider"
	"github.com/korotovsky/slack-mcp-server/internal/text"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
	"strconv"
)

type Message struct {
	UserID   string `json:"userID"`
	UserName string `json:"userUser"`
	RealName string `json:"realName"`
	Channel  string `json:"channelID"`
	Text     string `json:"text"`
	Time     string `json:"time"`
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
	channel, ok := request.Params.Arguments["channelID"].(string)
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

	usersMap := ch.apiProvider.ProvideUsersMap()

	var messageList []Message
	for _, message := range messages.Messages {
		textTokenized := text.ProcessText(message.Text)
		user, ok := usersMap[message.User]
		if !ok {
			// TODO: add periodic refetch of users
			continue
		}

		messageList = append(messageList, Message{
			UserID:   message.User,
			UserName: user.Name,
			RealName: user.RealName,
			Text:     textTokenized,
			Channel:  channel,
			Time:     message.Timestamp,
		})
	}

	csvBytes, err := gocsv.MarshalBytes(&messageList)
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(string(csvBytes)), nil
}
