package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/korotovsky/slack-mcp-server/pkg/transport"
	"github.com/slack-go/slack"
)

var defaultUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
var AllChanTypes = []string{"mpim", "im", "public_channel", "private_channel"}
var PubChanType = "public_channel"

type ChannelsCache struct {
	Channels    map[string]slack.Channel `json:"channels"`
	ChannelsInv map[string]string        `json:"channels_inv"`
}

type ApiProvider struct {
	boot   func() *slack.Client
	client *slack.Client

	users      map[string]slack.User
	usersCache string

	channels      map[string]slack.Channel
	channelsInv   map[string]string
	channelsCache string
}

func New() *ApiProvider {
	// Check for XOXP token first (User OAuth)
	xoxpToken := os.Getenv("SLACK_MCP_XOXP_TOKEN")
	if xoxpToken != "" {
		return newWithXOXP(xoxpToken)
	}

	// Fall back to XOXC/XOXD tokens (session-based)
	xoxcToken := os.Getenv("SLACK_MCP_XOXC_TOKEN")
	xoxdToken := os.Getenv("SLACK_MCP_XOXD_TOKEN")

	if xoxcToken == "" || xoxdToken == "" {
		panic("Authentication required: Either SLACK_MCP_XOXP_TOKEN (User OAuth) or both SLACK_MCP_XOXC_TOKEN and SLACK_MCP_XOXD_TOKEN (session-based) environment variables must be provided")
	}

	return newWithXOXC(xoxcToken, xoxdToken)
}

func newWithXOXP(token string) *ApiProvider {
	usersCache := os.Getenv("SLACK_MCP_USERS_CACHE")
	if usersCache == "" {
		usersCache = ".users_cache.json"
	}

	channelsCache := os.Getenv("SLACK_MCP_CHANNELS_CACHE")
	if channelsCache == "" {
		channelsCache = ".channels_cache.json"
	}

	return &ApiProvider{
		boot: func() *slack.Client {
			api := slack.New(token)
			res, err := api.AuthTest()
			if err != nil {
				panic(err)
			} else {
				log.Printf("Authenticated as: %s\n", res)
			}

			return api
		},

		users:      make(map[string]slack.User),
		usersCache: usersCache,

		channels:      make(map[string]slack.Channel),
		channelsInv:   map[string]string{},
		channelsCache: channelsCache,
	}
}

func newWithXOXC(token, cookie string) *ApiProvider {
	usersCache := os.Getenv("SLACK_MCP_USERS_CACHE")
	if usersCache == "" {
		usersCache = ".users_cache.json"
	}

	channelsCache := os.Getenv("SLACK_MCP_CHANNELS_CACHE")
	if channelsCache == "" {
		channelsCache = ".channels_cache.json"
	}

	return &ApiProvider{
		boot: func() *slack.Client {
			api := slack.New(token,
				withHTTPClientOption(cookie),
			)
			res, err := api.AuthTest()
			if err != nil {
				panic(err)
			} else {
				log.Printf("Authenticated as: %s\n", res)
			}

			api = slack.New(token,
				withHTTPClientOption(cookie),
				withTeamEndpointOption(res.URL),
			)

			return api
		},

		users:      make(map[string]slack.User),
		usersCache: usersCache,

		channels:      make(map[string]slack.Channel),
		channelsInv:   map[string]string{},
		channelsCache: channelsCache,
	}
}

func (ap *ApiProvider) Provide() (*slack.Client, error) {
	if ap.client == nil {
		ap.client = ap.boot()
	}

	return ap.client, nil
}

func (ap *ApiProvider) RefreshUsers(ctx context.Context) error {
	if data, err := ioutil.ReadFile(ap.usersCache); err == nil {
		var cachedUsers []slack.User
		if err := json.Unmarshal(data, &cachedUsers); err != nil {
			log.Printf("Failed to unmarshal %s: %v; will refetch", ap.usersCache, err)
		} else {
			for _, u := range cachedUsers {
				ap.users[u.ID] = u
			}
			log.Printf("Loaded %d users from cache %q", len(cachedUsers), ap.usersCache)
			return nil
		}
	}

	optionLimit := slack.GetUsersOptionLimit(1000)

	client, err := ap.Provide()
	if err != nil {
		return err
	}

	users, err := client.GetUsersContext(ctx,
		optionLimit,
	)
	if err != nil {
		log.Printf("Failed to fetch users: %v", err)
		return err
	}

	for _, user := range users {
		ap.users[user.ID] = user
	}

	if data, err := json.MarshalIndent(users, "", "  "); err != nil {
		log.Printf("Failed to marshal users for cache: %v", err)
	} else {
		if err := ioutil.WriteFile(ap.usersCache, data, 0644); err != nil {
			log.Printf("Failed to write cache file %q: %v", ap.usersCache, err)
		} else {
			log.Printf("Wrote %d users to cache %q", len(users), ap.usersCache)
		}
	}

	return nil
}

func (ap *ApiProvider) RefreshChannels(ctx context.Context) error {
	if data, err := ioutil.ReadFile(ap.channelsCache); err == nil {
		var cachedChannels []slack.Channel
		if err := json.Unmarshal(data, &cachedChannels); err != nil {
			log.Printf("Failed to unmarshal %s: %v; will refetch", ap.usersCache, err)
		} else {
			for _, c := range cachedChannels {
				ap.channels[c.ID] = c
			}
			log.Printf("Loaded %d channels from cache %q", len(cachedChannels), ap.usersCache)
			return nil
		}
	}

	channels := ap.GetChannels(ctx, AllChanTypes)

	if data, err := json.MarshalIndent(channels, "", "  "); err != nil {
		log.Printf("Failed to marshal channels for cache: %v", err)
	} else {
		if err := ioutil.WriteFile(ap.channelsCache, data, 0644); err != nil {
			log.Printf("Failed to write cache file %q: %v", ap.channelsCache, err)
		} else {
			log.Printf("Wrote %d channels to cache %q", len(channels), ap.channelsCache)
		}
	}

	return nil
}

func (ap *ApiProvider) GetChannels(ctx context.Context, channelTypes []string) []slack.Channel {
	if len(channelTypes) == 0 {
		channelTypes = AllChanTypes
	}

	params := &slack.GetConversationsParameters{
		Types:           AllChanTypes,
		Limit:           999,
		ExcludeArchived: true,
	}
	var (
		total   int
		nextcur string
		err     error
	)

	client, err := ap.Provide()
	if err != nil {
		return nil
	}

	for {
		var chans []slack.Channel

		chans, nextcur, err = client.GetConversationsContext(ctx, params)
		if err != nil {
			break
		}

		if l := len(chans); l > 0 {
			for _, channel := range chans {
				ap.channels[channel.ID] = channel
				ap.channelsInv["#"+channel.Name] = channel.ID
			}

			total += l
			params.Limit -= l
		}

		if nextcur == "" {
			log.Printf("channels fetch exhausted")
			break
		}

		params.Cursor = nextcur
	}

	var res []slack.Channel
	for _, t := range channelTypes {
		for _, channel := range ap.channels {
			if t == "public_channel" && !channel.IsPrivate {
				res = append(res, channel)
			}
			if t == "private_channel" && channel.IsPrivate {
				res = append(res, channel)
			}
			if t == "im" && channel.IsIM {
				res = append(res, channel)
			}
			if t == "mpim" && channel.IsMpIM {
				res = append(res, channel)
			}
		}
	}

	return res
}

func (ap *ApiProvider) ProvideUsersMap() map[string]slack.User {
	return ap.users
}

func (ap *ApiProvider) ProvideChannelsMaps() *ChannelsCache {
	return &ChannelsCache{
		Channels:    ap.channels,
		ChannelsInv: ap.channelsInv,
	}
}

func withHTTPClientOption(cookie string) func(c *slack.Client) {
	return func(c *slack.Client) {
		var proxy func(*http.Request) (*url.URL, error)
		if proxyURL := os.Getenv("SLACK_MCP_PROXY"); proxyURL != "" {
			parsed, err := url.Parse(proxyURL)
			if err != nil {
				log.Fatalf("Failed to parse proxy URL: %v", err)
			}

			proxy = http.ProxyURL(parsed)
		} else {
			proxy = nil
		}

		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}

		if localCertFile := os.Getenv("SLACK_MCP_SERVER_CA"); localCertFile != "" {
			certs, err := ioutil.ReadFile(localCertFile)
			if err != nil {
				log.Fatalf("Failed to append %q to RootCAs: %v", localCertFile, err)
			}

			if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
				log.Println("No certs appended, using system certs only")
			}
		}

		insecure := false
		if os.Getenv("SLACK_MCP_SERVER_CA_INSECURE") != "" {
			if localCertFile := os.Getenv("SLACK_MCP_SERVER_CA"); localCertFile != "" {
				log.Fatalf("Variable SLACK_MCP_SERVER_CA is at the same time with SLACK_MCP_SERVER_CA_INSECURE")
			}
			insecure = true
		}

		customHTTPTransport := &http.Transport{
			Proxy: proxy,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecure,
				RootCAs:            rootCAs,
			},
		}

		userAgent := defaultUA
		if os.Getenv("SLACK_MCP_USER_AGENT") != "" {
			userAgent = os.Getenv("SLACK_MCP_USER_AGENT")
		}

		client := &http.Client{
			Transport: transport.New(
				customHTTPTransport,
				userAgent,
				cookie,
			),
		}

		slack.OptionHTTPClient(client)(c)
	}
}

func withTeamEndpointOption(url string) slack.Option {
	return func(c *slack.Client) {
		slack.OptionAPIURL(url + "api/")(c)
	}
}
