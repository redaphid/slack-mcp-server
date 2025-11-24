package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultDBPath     = "./slack_export.db"
	defaultEndpoint   = "http://localhost:8787/slack/events"
	defaultBatchSize  = 100
	defaultRateLimit  = 10 // requests per second
)

type Config struct {
	DBPath      string
	Endpoint    string
	DryRun      bool
	BatchSize   int
	RateLimit   int
	StartOffset int
	Limit       int
}

type SlackEvent struct {
	Type         string  `json:"type"`
	Channel      string  `json:"channel"`
	User         string  `json:"user"`
	Text         *string `json:"text,omitempty"`
	Timestamp    string  `json:"ts"`
	ThreadTS     *string `json:"thread_ts,omitempty"`
	ParentUserID *string `json:"parent_user_id,omitempty"`
}

type SlackMessage struct {
	EventID string     `json:"event_id"`
	Event   SlackEvent `json:"event"`
}

type SlackEventPayload struct {
	Message SlackMessage `json:"message"`
}

type Message struct {
	EventID      string
	UserID       string
	ChannelID    string
	Text         sql.NullString
	Timestamp    string
	ThreadTS     sql.NullString
	ParentUserID sql.NullString
	Blocks       string
}

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// Not fatal - .env is optional if env vars are set elsewhere
		fmt.Fprintf(os.Stderr, "Note: .env file not loaded (this is fine if env vars are set): %v\n", err)
	}

	var config Config
	flag.StringVar(&config.DBPath, "db", defaultDBPath, "Path to SQLite database file")
	flag.StringVar(&config.Endpoint, "endpoint", defaultEndpoint, "Ears /slack/events endpoint URL")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Don't actually POST, just show what would be sent")
	flag.IntVar(&config.BatchSize, "batch", defaultBatchSize, "Number of messages to process in one batch")
	flag.IntVar(&config.RateLimit, "rate", defaultRateLimit, "Maximum requests per second")
	flag.IntVar(&config.StartOffset, "offset", 0, "Start from this message offset (for resuming)")
	flag.IntVar(&config.Limit, "limit", 0, "Maximum number of messages to process (0 = all)")
	flag.Parse()

	logger, err := newLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Slack backfill helper",
		zap.String("db_path", config.DBPath),
		zap.String("endpoint", config.Endpoint),
		zap.Bool("dry_run", config.DryRun),
		zap.Int("offset", config.StartOffset),
		zap.Int("limit", config.Limit),
	)

	if err := runBackfill(config, logger); err != nil {
		logger.Fatal("Backfill failed", zap.Error(err))
	}

	logger.Info("Backfill completed successfully!")
}

func runBackfill(config Config, logger *zap.Logger) error {
	// Open database
	db, err := sql.Open("sqlite3", config.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Count total messages
	var total int
	err = db.QueryRow("SELECT COUNT(*) FROM slack_messages").Scan(&total)
	if err != nil {
		return fmt.Errorf("failed to count messages: %w", err)
	}

	logger.Info("Messages in database", zap.Int("total", total))

	// Build query with pagination
	query := `
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
		ORDER BY timestamp ASC
		LIMIT ? OFFSET ?
	`

	// Determine how many to process
	limit := config.Limit
	if limit == 0 {
		limit = total - config.StartOffset
	}

	logger.Info("Starting backfill",
		zap.Int("offset", config.StartOffset),
		zap.Int("limit", limit),
	)

	// Rate limiter
	rateLimiter := time.NewTicker(time.Second / time.Duration(config.RateLimit))
	defer rateLimiter.Stop()

	var stats struct {
		processed int
		succeeded int
		failed    int
	}

	// Process in batches
	for offset := config.StartOffset; offset < config.StartOffset+limit; offset += config.BatchSize {
		batchSize := config.BatchSize
		if offset+batchSize > config.StartOffset+limit {
			batchSize = config.StartOffset + limit - offset
		}

		rows, err := db.Query(query, batchSize, offset)
		if err != nil {
			return fmt.Errorf("failed to query messages: %w", err)
		}

		for rows.Next() {
			var msg Message
			err := rows.Scan(
				&msg.EventID,
				&msg.UserID,
				&msg.ChannelID,
				&msg.Text,
				&msg.Timestamp,
				&msg.ThreadTS,
				&msg.ParentUserID,
				&msg.Blocks,
			)
			if err != nil {
				logger.Error("Failed to scan message", zap.Error(err))
				stats.failed++
				continue
			}

			// Build payload
			payload := buildPayload(msg)

			if config.DryRun {
				// Just print the payload
				jsonBytes, _ := json.MarshalIndent(payload, "", "  ")
				logger.Info("Would send payload",
					zap.String("event_id", msg.EventID),
					zap.String("payload", string(jsonBytes)),
				)
				stats.succeeded++
			} else {
				// Wait for rate limiter
				<-rateLimiter.C

				// POST to endpoint
				if err := postPayload(config.Endpoint, payload, logger); err != nil {
					logger.Error("Failed to post message",
						zap.String("event_id", msg.EventID),
						zap.Error(err),
					)
					stats.failed++
					continue
				}

				stats.succeeded++
			}

			stats.processed++

			// Log progress
			if stats.processed%100 == 0 {
				logger.Info("Progress",
					zap.Int("processed", stats.processed),
					zap.Int("succeeded", stats.succeeded),
					zap.Int("failed", stats.failed),
					zap.Int("total", limit),
				)
			}
		}
		rows.Close()
	}

	logger.Info("Backfill complete",
		zap.Int("processed", stats.processed),
		zap.Int("succeeded", stats.succeeded),
		zap.Int("failed", stats.failed),
	)

	return nil
}

func buildPayload(msg Message) SlackEventPayload {
	event := SlackEvent{
		Type:      "message",
		Channel:   msg.ChannelID,
		User:      msg.UserID,
		Timestamp: msg.Timestamp,
	}

	if msg.Text.Valid {
		event.Text = &msg.Text.String
	}

	if msg.ThreadTS.Valid {
		event.ThreadTS = &msg.ThreadTS.String
	}

	if msg.ParentUserID.Valid {
		event.ParentUserID = &msg.ParentUserID.String
	}

	return SlackEventPayload{
		Message: SlackMessage{
			EventID: msg.EventID,
			Event:   event,
		},
	}
}

func postPayload(endpoint string, payload SlackEventPayload, logger *zap.Logger) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	resp, err := http.Post(endpoint, "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func newLogger() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return logger.With(zap.String("app", "slack-backfill")), nil
}
