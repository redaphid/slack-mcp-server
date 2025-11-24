# Slack Sync Tool

The `slack-sync` tool automatically syncs Slack messages to a local SQLite database. It's designed to run on a schedule (via cron) to keep your local database up-to-date with new messages.

## Features

- **Incremental sync**: Only fetches new messages since the last sync
- **Automatic retry**: Handles Slack rate limits gracefully
- **Users & channels**: Syncs user and channel metadata
- **Thread support**: Captures threaded replies
- **Configurable**: Custom database path and schema

## Building

```bash
go build -o slack-sync ./cmd/sync
```

## Usage

### Manual Sync

Run a one-time sync:

```bash
./slack-sync -db ~/THE_SINK/slack/sibi.db -schema schema.sql
```

### Options

- `-db`: Path to SQLite database (default: `~/THE_SINK/slack/sibi.db`)
- `-schema`: Path to schema SQL file (default: `schema.sql`)

### Setting Up Automated Sync

To run the sync every 10 minutes via cron:

```bash
./scripts/setup-cron
```

This will:
1. Add a cron entry that runs every 10 minutes
2. Log output to `/tmp/slack-sync.log`
3. Use the database at `~/THE_SINK/slack/sibi.db`

### Monitoring

View sync logs:

```bash
tail -f /tmp/slack-sync.log
```

Check database stats:

```bash
sqlite3 ~/THE_SINK/slack/sibi.db "SELECT COUNT(*) as messages, (SELECT COUNT(*) FROM slack_users) as users, (SELECT COUNT(*) FROM slack_channels) as channels FROM slack_messages;"
```

### Managing the Cron Job

View current cron jobs:

```bash
crontab -l
```

Edit cron jobs (to remove or modify):

```bash
crontab -e
```

## How It Works

1. **First Run**: Syncs all accessible messages from all channels
2. **Subsequent Runs**: Only syncs messages newer than the most recent message in the database
3. **Rate Limiting**: Automatically detects Slack rate limits and waits the specified duration before retrying
4. **Incremental**: Uses `INSERT OR IGNORE` to avoid duplicates

## Database Structure

The sync creates three tables:

- `slack_messages`: All messages with text, timestamps, thread info
- `slack_users`: User profiles (name, email, avatar, etc.)
- `slack_channels`: Channel metadata (name, description, type, member count)

## Environment Variables

The sync tool requires Slack authentication tokens in `.env`:

```
SLACK_MCP_XOXC_TOKEN=xoxc-...
SLACK_MCP_XOXD_TOKEN=xoxd-...
```

These are automatically loaded from the project root's `.env` file.

## Performance

- Syncs ~1000-2000 messages per minute (depending on thread depth)
- Respects Slack's rate limits (typically triggers every ~50-100 API calls)
- Minimal CPU usage when no new messages exist

## Troubleshooting

**Problem**: Cron job not running

**Solution**: Check cron logs:
```bash
grep CRON /var/log/system.log  # macOS
```

**Problem**: Permission denied

**Solution**: Ensure the binary is executable and paths are correct:
```bash
chmod +x ./slack-sync
ls -l ~/THE_SINK/slack/
```

**Problem**: No new messages synced

**Solution**: The sync only fetches messages newer than the last sync. Check the log:
```bash
tail /tmp/slack-sync.log
```

Look for the `"since"` timestamp in the logs - it shows what timestamp it's syncing from.
