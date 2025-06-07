package handler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/korotovsky/slack-mcp-server/pkg/provider"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/slack-go/slack"
)

var AllChanTypes = []string{"mpim", "im", "public_channel", "private_channel"}
var PubChanType = "public_channel"

type Channel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	Purpose     string `json:"purpose"`
	MemberCount int    `json:"memberCount"`
	Cursor      string `json:"cursor"`
}

type ChannelsHandler struct {
	apiProvider *provider.ApiProvider
	validTypes  map[string]bool
}

func NewChannelsHandler(apiProvider *provider.ApiProvider) *ChannelsHandler {
	validTypes := make(map[string]bool, len(AllChanTypes))
	for _, v := range AllChanTypes {
		validTypes[v] = true
	}

	return &ChannelsHandler{
		apiProvider: apiProvider,
		validTypes:  validTypes,
	}
}

func (ch *ChannelsHandler) ChannelsHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sortType := request.GetString("sort", "popularity")
	types := request.GetString("channel_types", PubChanType)

	// MCP Inspector v0.14.0 has issues with Slice type
	// introspection, so some type simplification makes sense here
	channelTypes := []string{}
	for _, t := range strings.Split(types, ",") {
		t = strings.TrimSpace(t)
		if ch.validTypes[t] {
			channelTypes = append(channelTypes, t)
		}
	}

	cursor := request.GetString("cursor", "")
	limit := request.GetInt("limit", 0)
	if limit == 0 && cursor == "" {
		return nil, fmt.Errorf("One of limit or cursor needs to be provided")
	}
	if limit == 0 {
		limit = 100
	}

	api, err := ch.apiProvider.Provide()
	if err != nil {
		return nil, err
	}

	var channelList []Channel

	params := &slack.GetConversationsParameters{
		Types:           channelTypes,
		Limit:           limit,
		ExcludeArchived: true,
		Cursor:          cursor,
	}
	var (
		total   int
		nextcur string
	)
	for {
		var chans []slack.Channel

		chans, nextcur, err = api.GetConversationsContext(ctx, params)
		if err != nil {
			break
		}

		if l := len(chans); l > 0 {
			for _, channel := range chans {
				channelList = append(channelList, Channel{
					ID:          channel.ID,
					Name:        "#" + channel.Name,
					Topic:       channel.Topic.Value,
					Purpose:     channel.Purpose.Value,
					MemberCount: channel.NumMembers,
				})
			}

			total += l
			params.Limit -= l
		}

		if total >= limit {
			log.Printf("channels fetch limit reached %v", total)
			break
		}

		if nextcur == "" {
			log.Printf("channels fetch exhausted")
			break
		}
		params.Cursor = nextcur
	}

	switch sortType {
	case "popularity":
		sort.Slice(channelList, func(i, j int) bool {
			return channelList[i].MemberCount > channelList[j].MemberCount
		})
	default:
		// pass
	}

	if len(channelList) > 0 && nextcur != "" {
		channelList[len(channelList)-1].Cursor = nextcur
	}

	csvBytes, err := gocsv.MarshalBytes(&channelList)
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(string(csvBytes)), nil
}
