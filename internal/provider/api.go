package provider

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/korotovsky/slack-mcp-server/internal/transport"
	"github.com/slack-go/slack"
	"io/ioutil"
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
	token := os.Getenv("SLACK_MCP_XOXC_TOKEN")
	if token == "" {
		panic("SLACK_MCP_XOXC_TOKEN environment variable is required")
	}

	cookie := os.Getenv("SLACK_MCP_XOXD_TOKEN")
	if cookie == "" {
		panic("SLACK_MCP_XOXD_TOKEN environment variable is required")
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

func withTeamEndpointOption(url string) slack.Option {
	return func(c *slack.Client) {
		slack.OptionAPIURL(url + "api/")(c)
	}
}
