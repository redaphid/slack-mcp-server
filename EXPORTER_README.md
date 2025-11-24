# Slack Exporter

Exports all historical and current Slack messages to a SQLite database.

## Features

- ✅ **Incremental updates** - Only fetches new messages since last export (unless `-full` flag used)
- ✅ **Progress bar** - Visual progress indicator showing current channel and completion
- ✅ Tracks export progress per channel
- ✅ Resumes automatically if interrupted
- ✅ Can be run repeatedly to stay up to date (uses INSERT OR REPLACE)
- ✅ Excludes #kubernetes-events spam channel
- ✅ Handles rate limiting automatically

## Usage

### Basic Export

```bash
go run cmd/exporter/main.go \
  -db ~/THE_SINK/slack/sibi.slack.db \
  -schema ./schema.sql
```

### Options

- `-db <path>` - Database path (default: `./slack_export.db`)
- `-schema <path>` - Schema file path (default: `./schema.sql`)
- `-full` - Fetch ALL messages (not just new ones since last export)
- `-reset` - Reset all progress and start from scratch

### Check Export Status

```bash
./scripts/export-status ~/THE_SINK/slack/sibi.slack.db
```

Shows:
- Which channels are completed/in progress/failed
- Message counts per channel
- Total messages in database

### Running Repeatedly

The exporter can be safely run multiple times:
1. **Incremental by default** - Only fetches messages since last export (much faster!)
2. Uses `INSERT OR REPLACE` - won't create duplicates
3. Progress bar shows real-time progress
4. Progress table tracks completion status

**To stay up to date**, run the exporter periodically (e.g., hourly via cron):

```bash
# Add to crontab - runs hourly, incremental updates only
0 * * * * ~/bin/slack-exporter -db ~/THE_SINK/slack/sibi.slack.db -schema ~/bin/slack-sync-schema.sql >> /tmp/slack-export.log 2>&1
```

**For a full re-export** (fetch all messages from beginning):

```bash
go run cmd/exporter/main.go \
  -db ~/THE_SINK/slack/sibi.slack.db \
  -schema ./schema.sql \
  -full
```

### Resume After Interruption

If the exporter is interrupted (Ctrl+C, crash, etc.):
- Just run it again - it will continue where it left off
- Progress table tracks which channels completed
- Completed channels are re-exported (to catch new messages)
- Failed channels are retried

### Reset and Start Over

```bash
go run cmd/exporter/main.go \
  -db ~/THE_SINK/slack/sibi.slack.db \
  -schema ./schema.sql \
  -reset
```

This clears the progress table but keeps existing messages.

## How It Works

1. **Users & Channels**: Exports all users and channels first
2. **Messages**: Iterates through each channel (with progress bar):
   - Checks latest message timestamp in database (unless `-full` flag)
   - Fetches only new messages via Slack API (incremental)
   - Marks channel as "in_progress" in `export_progress` table
   - Fetches thread replies for threaded messages
   - Marks channel as "completed" when done
   - If error occurs, marks as "failed" with error message
   - Updates progress bar after each channel
3. **Rate Limiting**: Automatically detects and waits for Slack rate limits
4. **Duplicates**: Uses `INSERT OR REPLACE` so re-running is safe

### Incremental vs Full Export

**Incremental (default)**:
- Checks `MAX(timestamp)` for each channel in database
- Only fetches messages newer than latest stored message
- Fast for keeping database up to date (seconds to minutes)
- Safe to run frequently (hourly/daily)

**Full (`-full` flag)**:
- Fetches all messages from beginning of time
- Slower but ensures nothing is missed
- Use for initial export or if you suspect data gaps
- Takes minutes to hours depending on workspace size

## Progress Tracking

The `export_progress` table tracks:
- `channel_id` - Channel identifier
- `channel_name` - Channel name for readability
- `status` - Current status: `pending`, `in_progress`, `completed`, `failed`
- `message_count` - Number of messages exported (updated every 100 messages)
- `started_at` - When export started for this channel
- `completed_at` - When export completed
- `error_message` - Error details if failed

This makes it easy to:
- Monitor long-running exports
- Resume after crashes
- Identify problematic channels
- Track overall progress
