package handler

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/korotovsky/slack-mcp-server/pkg/test/util"
	"github.com/openai/openai-go/packages/param"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/responses"
)

func TestMCPServerIntegration(t *testing.T) {
	sseKey := os.Getenv("SLACK_MCP_SSE_API_KEY")
	if sseKey == "" {
		t.Fatalf("SLACK_MCP_SSE_API_KEY not set")
	}
	mcp, err := util.SetupMCP(sseKey)
	if err != nil {
		t.Fatalf("Failed to set up MCP server: %v", err)
	}
	fwd, err := util.SetupForwarding(context.Background(), "http://"+mcp.Host+":"+strconv.Itoa(mcp.Port))
	if err != nil {
		t.Fatalf("Failed to set up ngrok forwarding: %v", err)
	}
	defer fwd.Shutdown()
	defer mcp.Shutdown()

	apiKey := os.Getenv("SLACK_MCP_OPENAI_API")
	require.NotEmpty(t, apiKey, "SLACK_MCP_OPENAI_API must be set for integration tests")
	require.NotEmpty(t, fwd.URL.Host, "Host must be set for integration tests")

	client := openai.NewClient(option.WithAPIKey(apiKey))
	ctx := context.Background()

	type tc struct {
		name                        string
		input                       string
		expectedToolOutputSubstring string
		expectedLLMOutputSubstring  string
	}

	cases := []tc{
		{
			name:                        "ListSlackChannels",
			input:                       "Give me list of slack channels.",
			expectedToolOutputSubstring: `"id":`,
			expectedLLMOutputSubstring:  "general",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := responses.ResponseNewParams{
				Model: "gpt-4.1-mini",
				Tools: []responses.ToolUnionParam{
					{
						OfMcp: &responses.ToolMcpParam{
							ServerLabel: "slack-mcp-server",
							ServerURL:   fmt.Sprintf("%s://%s/sse", fwd.URL.Scheme, fwd.URL.Host),
							RequireApproval: responses.ToolMcpRequireApprovalUnionParam{
								OfMcpToolApprovalSetting: param.NewOpt("never"),
							},
							Headers: map[string]string{
								"Authorization": "Bearer " + sseKey,
							},
						},
					},
				},
				Input: responses.ResponseNewParamsInputUnion{
					OfString: openai.String(tc.input),
				},
			}

			resp, err := client.Responses.New(ctx, params)
			require.NoError(t, err, "API call failed")

			// 1) Aggregate all tool‑output items into one blob
			var rawToolOutput strings.Builder
			for _, out := range resp.Output {
				if out.Type == "mcp_tool_output" {
					//rawToolOutput.WriteString(out.Content)
					rawToolOutput.WriteString("")
				}
			}
			toolOutput := rawToolOutput.String()
			assert.Contains(t, toolOutput, tc.expectedToolOutputSubstring,
				"tool output should include %q, got: %s", tc.expectedToolOutputSubstring, toolOutput)

			// 2) Check the final model answer
			llmOutput := resp.OutputText()
			assert.Contains(t, llmOutput, tc.expectedLLMOutputSubstring,
				"LLM output should include %q, got: %s", tc.expectedLLMOutputSubstring, llmOutput)
		})
	}
}
