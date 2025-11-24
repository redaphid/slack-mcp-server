#!/bin/bash
# Helper script to generate backfill data from exported SQLite database
#
# Usage: ./scripts/backfill_helper.sh <database_path> [ears_endpoint_url]
#
# This script queries the exported SQLite database and generates JSON
# payloads compatible with the ears project's /slack/events endpoint.
#
# Example:
#   ./scripts/backfill_helper.sh slack_export.db
#   ./scripts/backfill_helper.sh slack_export.db http://localhost:8787/slack/events

set -e

DB_PATH="${1:-./slack_export.db}"
ENDPOINT_URL="${2:-http://localhost:8787/slack/events}"

if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database file not found: $DB_PATH"
    echo "Usage: $0 <database_path> [ears_endpoint_url]"
    exit 1
fi

echo "Exporting messages from: $DB_PATH"
echo "Target endpoint: $ENDPOINT_URL"
echo ""

# Query to get all messages ordered by timestamp
QUERY="
SELECT
    event_id,
    user_id,
    channel_id,
    text,
    timestamp,
    thread_ts,
    parent_user_id,
    blocks
FROM slack_messages
ORDER BY timestamp ASC;
"

# Count total messages
TOTAL=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM slack_messages;")
echo "Total messages to export: $TOTAL"
echo ""

# Generate JSON for each message and optionally POST to endpoint
COUNTER=0

sqlite3 -separator $'\t' "$DB_PATH" "$QUERY" | while IFS=$'\t' read -r event_id user_id channel_id text timestamp thread_ts parent_user_id blocks; do
    COUNTER=$((COUNTER + 1))

    # Build JSON payload
    JSON=$(cat <<EOF
{
  "message": {
    "event_id": "$event_id",
    "event": {
      "type": "message",
      "channel": "$channel_id",
      "user": "$user_id",
      "text": $(echo "$text" | jq -Rs .),
      "ts": "$timestamp"
EOF
    )

    # Add thread_ts if present
    if [ -n "$thread_ts" ]; then
        JSON="$JSON,
      \"thread_ts\": \"$thread_ts\""
    fi

    # Add parent_user_id if present
    if [ -n "$parent_user_id" ]; then
        JSON="$JSON,
      \"parent_user_id\": \"$parent_user_id\""
    fi

    # Close JSON
    JSON="$JSON
    }
  }
}"

    # Print progress every 100 messages
    if [ $((COUNTER % 100)) -eq 0 ]; then
        echo "Progress: $COUNTER / $TOTAL messages"
    fi

    # Uncomment to POST to endpoint (currently just prints)
    # curl -X POST "$ENDPOINT_URL" \
    #   -H "Content-Type: application/json" \
    #   -d "$JSON"

    # For now, just write to a file
    echo "$JSON" >> backfill_payloads.jsonl
done

echo ""
echo "✓ Export complete!"
echo "  Generated: backfill_payloads.jsonl"
echo ""
echo "To backfill the ears project:"
echo "  1. Review backfill_payloads.jsonl"
echo "  2. Uncomment the curl line in this script"
echo "  3. Update ENDPOINT_URL and run again"
echo ""
echo "Or use the following command to POST all payloads:"
echo "  cat backfill_payloads.jsonl | while read line; do curl -X POST \"$ENDPOINT_URL\" -H \"Content-Type: application/json\" -d \"\$line\"; done"
