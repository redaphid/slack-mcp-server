package provider

import (
	"crypto/tls"
	"github.com/korotovsky/slack-mcp-server/internal/transport"
	"github.com/slack-go/slack"
	"log"
	"net/http"
	"net/url"
	"os"
)

type ApiProvider struct {
	boot   func() *slack.Client
	client *slack.Client
}

func New() *ApiProvider {
	token := os.Getenv("SLACK_XOXC_TOKEN")
	if token == "" {
		panic("SLACK_XOXC_TOKEN environment variable is required")
	}

	cookie := os.Getenv("SLACK_XOXD_TOKEN")
	if cookie == "" {
		panic("SLACK_XOXD_TOKEN environment variable is required")
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
				withTeamEndpointOption(res.TeamID),
			)

			return api
		},
	}
}

func (ap *ApiProvider) Provide() (*slack.Client, error) {
	if ap.client == nil {
		ap.client = ap.boot()
	}

	return ap.client, nil
}

func withHTTPClientOption(cookie string) func(c *slack.Client) {
	return func(c *slack.Client) {
		var proxy func(*http.Request) (*url.URL, error)
		if proxyURL := os.Getenv("HTTP_PROXY"); proxyURL != "" {
			parsed, err := url.Parse(proxyURL)
			if err != nil {
				log.Fatalf("Failed to parse proxy URL: %v", err)
			}

			proxy = http.ProxyURL(parsed)
		} else {
			proxy = nil
		}

		customHTTPTransport := &http.Transport{
			Proxy: proxy,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}

		client := &http.Client{
			Transport: transport.New(
				customHTTPTransport,
				"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
				cookie,
			),
		}

		slack.OptionHTTPClient(client)(c)
	}
}

func withTeamEndpointOption(teamName string) slack.Option {
	return func(c *slack.Client) {
		slack.OptionAPIURL("https://" + teamName + ".slack.com/api/")(c)
	}
}
