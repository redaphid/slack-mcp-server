# Slack MCP Server

Model Context Protocol (MCP) server for Slack Workspaces. This integration supports both Stdio and SSE transports, proxy settings and does not require any permissions or bots being created or approved by Workspace admins 😏.

### Feature Demo

...

## Setup Guide

### 1. Authentication Setup

...

### 2. Installation

Choose one of these installation methods:

#### 2.1. Docker

```bash
git clone https://github.com/korotovsky/slack-mcp-server.git
cd slack-mcp-server
docker build -t slack-mcp-server .
```

#### 2.2. Docker Compose

```bash
git clone https://github.com/korotovsky/slack-mcp-server.git
cd slack-mcp-server
cp .env.example .env
nano .env # Edit .env file with your tokens from step 1 of the setup guide
docker-compose build
```

### 3. Configuration and Usage

You can configure the MCP server using command line arguments and environment variables.

Add the following to your `claude_desktop_config.json`:

Option 1 with `stdio` transport:
```json
{
  "mcpServers": {
    "slack": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "SLACK_XOXC_TOKEN",
        "-e",
        "SLACK_XOXD_TOKEN",
        "slack-mcp-server",
        "--transport",
        "stdio"
      ],
      "env": {
        "SLACK_XOXC_TOKEN": "xoxc-...",
        "SLACK_XOXD_TOKEN": "xoxd-..."
      }
    }
  }
}
```

#### Console Arguments

| Argument              | Required ? | Description                                                              |
|-----------------------|------------|--------------------------------------------------------------------------|
| `--transport` or `-t` | Yes        | Select transport for the MCP Server, possible values are: `stdio`, `sse` |

#### Environment Variables

| Variable                 | Required ? | Default     | Description                                                                   |
|--------------------------|------------|-------------|-------------------------------------------------------------------------------|
| `SLACK_XOXC_TOKEN`       | Yes        | `nil`       | Authentication data token field `token` from POST data field-set (`xoxc-...`) |
| `SLACK_XOXD_TOKEN`       | Yes        | `nil`       | Authentication data token from cookie `d` (`xoxd-...`)                        |
| `SLACK_MCP_SERVER_PORT`  | No         | `3001`      | Port for the MCP server to listen on                                          |
| `SLACK_MCP_SERVER_HOST`  | No         | `127.0.0.1` | Host for the MCP server to listen on                                          |
| `SLACK_MCP_SERVER_PROXY` | No         | `nil`       | Proxy URL for the MCP server to use                                           |
| `SLACK_SSE_API_KEY`      | No         | `nil`       | Authorization Bearer token when `transport` is `sse`                          |

## Available Tools

| Tool                    | Description                   |
|-------------------------|-------------------------------|
| `conversations.history` | Get messages from the channel |

### Debugging Tools

```bash
# View logs
tail -n 20 -f ~/Library/Logs/Claude/mcp*.log
```

## Security

- Never share API tokens
- Keep .env files secure and private

## License

Licensed under MIT - see [LICENSE](LICENSE) file. This is not an official Slack product.
