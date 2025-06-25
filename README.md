# Slack MCP Server

Model Context Protocol (MCP) server for Slack Workspaces. The most powerful MCP Slack server — supports Stdio and SSE transports, proxy settings, DMs, Group DMs, Smart History fetch (by date or count), may work via OAuth or in complete stealth mode with no permissions and scopes in Workspace 😏.

> [!IMPORTANT]  
> We need your support! Each month, over 10,000 engineers visit this repository, and more than 2,000 are already using it.
> 
> If you appreciate the work our [contributors](https://github.com/korotovsky/slack-mcp-server/graphs/contributors) have put into this project, please consider giving the repository a star.

This feature-rich Slack MCP Server has:
- **Stealth mode**: Run the server without any additional permissions or bot installations.
- **OAuth mode**: Use secure OAuth tokens for secure access without needing to refresh or extract tokens from the browser.
- **Channel and thread support**: Fetch messages from channels and threads, including activity messages.
- **DM and Group DM support**: Retrieve direct messages and group direct messages.
- **Smart History**: Fetch messages with pagination by date (d1, 7d, 1m) or message count.
- **Stdio and SSE transports**: Use the server with any MCP client that supports Stdio or SSE transports.
- **Proxy support**: Configure the server to use a proxy for outgoing requests.

### Feature Demo

![ezgif-316311ee04f444](https://github.com/user-attachments/assets/35dc9895-e695-4e56-acdc-1a46d6520ba0)

## Tools

1. `conversations_history`
  - Get messages from the channel by channelID
  - Required inputs:
    - `channel_id` (string): ID of the channel in format Cxxxxxxxxxx or its name starting with #... aka #general.
    - `include_activity_messages` (bool, default: false): If true, the response will include activity messages such as 'channel_join' or 'channel_leave'. Default is boolean false.
    - `cursor` (string, default: ""): Cursor for pagination. Use the value of the last row and column in the response as next_cursor field returned from the previous request.
    - `limit` (string, default: 28): Limit of messages to fetch.
  - Returns: List of messages with timestamps, user IDs, and text content

2. `conversations_replies`
  - Get a thread of messages posted to a conversation by channelID and thread_ts
  - Required inputs:
    - `channel_id` (string): ID of the channel in format Cxxxxxxxxxx or its name starting with #... aka #general.
    - `thread_ts` (string): Unique identifier of either a thread’s parent message or a message in the thread. ts must be the timestamp in format 1234567890.123456 of an existing message with 0 or more replies.
    - `include_activity_messages` (bool, default: false): If true, the response will include activity messages such as 'channel_join' or 'channel_leave'. Default is boolean false.
    - `cursor` (string, default: ""): Cursor for pagination. Use the value of the last row and column in the response as next_cursor field returned from the previous request.
    - `limit` (string, default: 28): Limit of messages to fetch.
  - Returns: List of replies with timestamps, user IDs, and text content

3. `channels_list`
  - Get list of channels
  - Required inputs:
    - `channel_types` (string): Comma-separated channel types. Allowed values: 'mpim', 'im', 'public_channel', 'private_channel'. Example: 'public_channel,private_channel,im'.
    - `sort` (string): Type of sorting. Allowed values: 'popularity' - sort by number of members/participants in each channel.
    - `limit` (number, default: 100): Limit of channels to fetch.
    - `cursor` (string): Cursor for pagination. Use the value of the last row and column in the response as next_cursor field returned from the previous request.
  - Returns: List of channels

## Setup Guide

- [Authentication Setup](docs/01-authentication-setup.md)
- [Installation](docs/02-installation.md)
- [Configuration and Usage](docs/03-configuration-and-usage.md)

### Debugging Tools

```bash
# Run the inspector with stdio transport
npx @modelcontextprotocol/inspector go run mcp/mcp-server.go --transport stdio

# View logs
tail -n 20 -f ~/Library/Logs/Claude/mcp*.log
```

## Security

- Never share API tokens
- Keep .env files secure and private

## License

Licensed under MIT - see [LICENSE](LICENSE) file. This is not an official Slack product.
