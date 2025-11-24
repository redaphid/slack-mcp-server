package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/joho/godotenv"
	"github.com/korotovsky/slack-mcp-server/pkg/provider"
	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"
	"github.com/slack-go/slack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultDBPath     = "./slack_export.db"
	rateLimitBuffer   = 2 * time.Second // Extra time to add to Slack's retry-after
	fetchDelay        = 100 * time.Millisecond // Small delay between API calls
	maxJitter         = 200 * time.Millisecond // Maximum random jitter (0-200ms)
)

type Config struct {
	DBPath        string
	SchemaPath    string
	ResetProgress bool
	FullExport    bool
}

type Exporter struct {
	db         *sql.DB
	provider   *provider.ApiProvider
	logger     *zap.Logger
	ctx        context.Context
	fullExport bool

	stats struct {
		channels  int
		messages  int
		threads   int
		users     int
		errors    int
	}
}

func main() {
	// Seed random number generator for jitter
	rand.Seed(time.Now().UnixNano())

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// Not fatal - .env is optional if env vars are set elsewhere
		fmt.Fprintf(os.Stderr, "Note: .env file not loaded (this is fine if env vars are set): %v\n", err)
	}

	var config Config
	flag.StringVar(&config.DBPath, "db", defaultDBPath, "Path to SQLite database file")
	flag.StringVar(&config.SchemaPath, "schema", "./schema.sql", "Path to schema.sql file")
	flag.BoolVar(&config.ResetProgress, "reset", false, "Reset export progress and start from scratch")
	flag.BoolVar(&config.FullExport, "full", false, "Fetch all messages (not just incremental updates)")
	flag.Parse()

	logger, err := newLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Slack data exporter",
		zap.String("db_path", config.DBPath),
		zap.String("schema_path", config.SchemaPath),
		zap.Bool("reset_progress", config.ResetProgress),
		zap.Bool("full_export", config.FullExport),
	)

	// Initialize database
	db, err := initDatabase(config.DBPath, config.SchemaPath, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to initialize database: %v\n", err)
		logger.Fatal("Failed to initialize database", zap.Error(err))
	}
	defer db.Close()

	// Initialize provider
	p := provider.New("stdio", logger)

	// Create exporter
	exporter := &Exporter{
		db:         db,
		provider:   p,
		logger:     logger,
		ctx:        context.Background(),
		fullExport: config.FullExport,
	}

	// Reset progress if requested
	if config.ResetProgress {
		logger.Info("Resetting export progress...")
		if err := exporter.resetProgress(); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: Failed to reset progress: %v\n", err)
			logger.Fatal("Failed to reset progress", zap.Error(err))
		}
	}

	// Wait for provider to be ready
	logger.Info("Waiting for Slack provider to initialize...")
	if err := exporter.waitForReady(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Provider failed to initialize (check Slack tokens): %v\n", err)
		logger.Fatal("Provider failed to become ready", zap.Error(err))
	}

	// Export users
	logger.Info("Exporting users...")
	if err := exporter.exportUsers(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to export users: %v\n", err)
		logger.Fatal("Failed to export users", zap.Error(err))
	}

	// Export channels
	logger.Info("Exporting channels...")
	if err := exporter.exportChannels(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to export channels: %v\n", err)
		logger.Fatal("Failed to export channels", zap.Error(err))
	}

	// Export messages from all channels
	logger.Info("Exporting messages from all channels...")
	if err := exporter.exportAllMessages(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to export messages: %v\n", err)
		logger.Fatal("Failed to export messages", zap.Error(err))
	}

	// Print stats
	exporter.printStats()
	logger.Info("Export completed successfully!")
	fmt.Fprintf(os.Stderr, "SUCCESS: Export completed! Messages: %d, Channels: %d, Errors: %d\n",
		exporter.stats.messages, exporter.stats.channels, exporter.stats.errors)
}

func initDatabase(dbPath, schemaPath string, logger *zap.Logger) (*sql.DB, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Open database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Read and execute schema
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	if _, err := db.Exec(string(schema)); err != nil {
		return nil, fmt.Errorf("failed to execute schema: %w", err)
	}

	logger.Info("Database initialized", zap.String("path", dbPath))
	return db, nil
}

func (e *Exporter) waitForReady() error {
	// First refresh users and channels
	e.logger.Info("Refreshing users cache...")
	if err := e.provider.RefreshUsers(e.ctx); err != nil {
		return fmt.Errorf("failed to refresh users: %w", err)
	}

	e.logger.Info("Refreshing channels cache...")
	if err := e.provider.RefreshChannels(e.ctx); err != nil {
		return fmt.Errorf("failed to refresh channels: %w", err)
	}

	// Wait for readiness
	maxAttempts := 30
	for i := 0; i < maxAttempts; i++ {
		ready, _ := e.provider.IsReady()
		if ready {
			e.logger.Info("Provider is ready")
			return nil
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("provider did not become ready after %d seconds", maxAttempts)
}

func (e *Exporter) exportUsers() error {
	usersMap := e.provider.ProvideUsersMap()
	users := usersMap.Users

	stmt, err := e.db.Prepare(`
		INSERT OR REPLACE INTO slack_users
		(user_id, name, display_name, real_name, email, is_bot, avatar_url, profile_url)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, user := range users {
		isBot := 0
		if user.IsBot {
			isBot = 1
		}

		avatarURL := user.Profile.Image72
		profileURL := fmt.Sprintf("https://slack.com/app_redirect?team=%s&user=%s", user.TeamID, user.ID)

		_, err := stmt.Exec(
			user.ID,
			user.Name,
			user.Profile.DisplayName,
			user.RealName,
			user.Profile.Email,
			isBot,
			avatarURL,
			profileURL,
		)
		if err != nil {
			e.logger.Warn("Failed to insert user",
				zap.String("user_id", user.ID),
				zap.Error(err),
			)
			e.stats.errors++
			continue
		}
		e.stats.users++
	}

	e.logger.Info("Users exported", zap.Int("count", e.stats.users))
	return nil
}

func (e *Exporter) exportChannels() error {
	channelsMap := e.provider.ProvideChannelsMaps()
	channels := channelsMap.Channels

	stmt, err := e.db.Prepare(`
		INSERT OR REPLACE INTO slack_channels
		(channel_id, name, description, channel_type, is_private, member_count)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, channel := range channels {
		channelType := determineChannelType(channel)
		isPrivate := 0
		if channel.IsPrivate {
			isPrivate = 1
		}

		description := channel.Purpose
		if description == "" {
			description = channel.Topic
		}

		_, err := stmt.Exec(
			channel.ID,
			channel.Name,
			description,
			channelType,
			isPrivate,
			channel.MemberCount,
		)
		if err != nil {
			e.logger.Warn("Failed to insert channel",
				zap.String("channel_id", channel.ID),
				zap.Error(err),
			)
			e.stats.errors++
			continue
		}
		e.stats.channels++
	}

	e.logger.Info("Channels exported", zap.Int("count", e.stats.channels))
	return nil
}

func determineChannelType(channel provider.Channel) string {
	if channel.IsIM {
		return "im"
	}
	if channel.IsMpIM {
		return "mpim"
	}
	if channel.IsPrivate {
		return "private_channel"
	}
	return "public_channel"
}

func (e *Exporter) exportAllMessages() error {
	channelsMap := e.provider.ProvideChannelsMaps()
	channels := channelsMap.Channels

	// Filter out kubernetes-events
	var filteredChannels []provider.Channel
	for _, channel := range channels {
		if channel.Name == "#kubernetes-events" {
			continue
		}
		filteredChannels = append(filteredChannels, channel)
	}

	// Create progress bar
	bar := progressbar.NewOptions(len(filteredChannels),
		progressbar.OptionSetDescription("Exporting channels"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	for _, channel := range filteredChannels {
		// Update progress bar description with current channel
		bar.Describe(fmt.Sprintf("Exporting %s", channel.Name))

		e.logger.Info("Exporting messages from channel",
			zap.String("channel_id", channel.ID),
			zap.String("channel_name", channel.Name),
		)

		// Mark as in_progress
		if err := e.markChannelStarted(channel.ID, channel.Name); err != nil {
			e.logger.Warn("Failed to mark channel as started",
				zap.String("channel_id", channel.ID),
				zap.Error(err),
			)
		}

		if err := e.exportChannelMessages(channel.ID, channel.Name); err != nil {
			e.logger.Error("Failed to export channel messages",
				zap.String("channel_id", channel.ID),
				zap.String("channel_name", channel.Name),
				zap.Error(err),
			)
			e.stats.errors++
			// Mark as failed
			e.markChannelFailed(channel.ID, err.Error())
			// Update progress bar anyway
			bar.Add(1)
			// Continue with next channel
			continue
		}

		// Mark as completed
		if err := e.markChannelCompleted(channel.ID); err != nil {
			e.logger.Warn("Failed to mark channel as completed",
				zap.String("channel_id", channel.ID),
				zap.Error(err),
			)
		}

		// Update progress bar
		bar.Add(1)
	}

	fmt.Println() // New line after progress bar
	return nil
}

func (e *Exporter) exportChannelMessages(channelID, channelName string) error {
	var cursor string
	pageNum := 0
	messagesInChannel := 0

	// Get latest timestamp for incremental export
	var oldestTimestamp string
	if !e.fullExport {
		var ts sql.NullString
		err := e.db.QueryRow("SELECT MAX(timestamp) FROM slack_messages WHERE channel_id = ?", channelID).Scan(&ts)
		if err != nil && err != sql.ErrNoRows {
			e.logger.Warn("Failed to get latest timestamp, doing full export for this channel",
				zap.String("channel_id", channelID),
				zap.Error(err),
			)
		}
		if ts.Valid {
			oldestTimestamp = ts.String
			e.logger.Info("Incremental export from timestamp",
				zap.String("channel_name", channelName),
				zap.String("since", oldestTimestamp),
			)
		}
	}

	for {
		pageNum++
		e.logger.Debug("Fetching message page",
			zap.String("channel_id", channelID),
			zap.Int("page", pageNum),
			zap.String("cursor", cursor),
		)

		params := &slack.GetConversationHistoryParameters{
			ChannelID: channelID,
			Cursor:    cursor,
			Inclusive: false,
			Oldest:    oldestTimestamp, // Only fetch messages newer than this
		}

		var history *slack.GetConversationHistoryResponse
		var err error

		// Add small delay + jitter before fetch to avoid hitting rate limits
		jitter := time.Duration(rand.Int63n(int64(maxJitter)))
		time.Sleep(fetchDelay + jitter)

		// Retry loop for rate limits
		for {
			history, err = e.provider.Slack().GetConversationHistoryContext(e.ctx, params)
			if err != nil {
				// Check if it's a rate limit error
				if e.handleRateLimitError(err) {
					// Rate limit handled, retry
					continue
				}
				// Other error, return it
				return fmt.Errorf("failed to get conversation history: %w", err)
			}
			// Success, break out of retry loop
			break
		}

		if len(history.Messages) == 0 {
			break
		}

		// Store messages
		for _, msg := range history.Messages {
			if err := e.storeMessage(channelID, msg, ""); err != nil {
				e.logger.Warn("Failed to store message",
					zap.String("channel_id", channelID),
					zap.String("timestamp", msg.Timestamp),
					zap.Error(err),
				)
				e.stats.errors++
				continue
			}
			messagesInChannel++

			// If this is a thread parent, fetch thread replies
			if msg.ReplyCount > 0 && msg.ThreadTimestamp == msg.Timestamp {
				if err := e.exportThreadReplies(channelID, msg.Timestamp, msg.User); err != nil {
					e.logger.Warn("Failed to export thread replies",
						zap.String("channel_id", channelID),
						zap.String("thread_ts", msg.Timestamp),
						zap.Error(err),
					)
					e.stats.errors++
				}
			}
		}

		// Update progress periodically
		if messagesInChannel%100 == 0 {
			e.logger.Info("Progress",
				zap.String("channel_name", channelName),
				zap.Int("messages", messagesInChannel),
			)
		}

		if !history.HasMore {
			break
		}

		cursor = history.ResponseMetaData.NextCursor
		if cursor == "" {
			break
		}
	}

	return nil
}

func (e *Exporter) exportThreadReplies(channelID, threadTS, parentUserID string) error {
	var cursor string
	pageNum := 0

	for {
		pageNum++
		e.logger.Debug("Fetching thread replies",
			zap.String("channel_id", channelID),
			zap.String("thread_ts", threadTS),
			zap.Int("page", pageNum),
		)

		params := &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Cursor:    cursor,
		}

		var msgs []slack.Message
		var hasMore bool
		var nextCursor string
		var err error

		// Add small delay + jitter before fetch to avoid hitting rate limits
		jitter := time.Duration(rand.Int63n(int64(maxJitter)))
		time.Sleep(fetchDelay + jitter)

		// Retry loop for rate limits
		for {
			msgs, hasMore, nextCursor, err = e.provider.Slack().GetConversationRepliesContext(e.ctx, params)
			if err != nil {
				// Check if it's a rate limit error
				if e.handleRateLimitError(err) {
					// Rate limit handled, retry
					continue
				}
				// Other error, return it
				return fmt.Errorf("failed to get thread replies: %w", err)
			}
			// Success, break out of retry loop
			break
		}

		// Skip the first message if it's the parent (already stored)
		startIdx := 0
		if len(msgs) > 0 && msgs[0].Timestamp == threadTS {
			startIdx = 1
		}

		// Store thread replies
		for i := startIdx; i < len(msgs); i++ {
			if err := e.storeMessage(channelID, msgs[i], parentUserID); err != nil {
				e.logger.Warn("Failed to store thread reply",
					zap.String("channel_id", channelID),
					zap.String("thread_ts", threadTS),
					zap.String("timestamp", msgs[i].Timestamp),
					zap.Error(err),
				)
				e.stats.errors++
				continue
			}
		}

		if !hasMore {
			break
		}

		cursor = nextCursor
		if cursor == "" {
			break
		}
	}

	e.stats.threads++
	return nil
}

func (e *Exporter) storeMessage(channelID string, msg slack.Message, parentUserID string) error {
	// Generate event_id (unique identifier)
	eventID := fmt.Sprintf("%s-%s", msg.Timestamp, channelID)

	// Determine user_id (could be user or bot)
	userID := msg.User
	if userID == "" && msg.BotID != "" {
		userID = msg.BotID
	}
	if userID == "" {
		return fmt.Errorf("message has no user_id or bot_id")
	}

	// Serialize blocks to JSON
	blocksJSON := "[]"
	if len(msg.Blocks.BlockSet) > 0 {
		if data, err := json.Marshal(msg.Blocks.BlockSet); err == nil {
			blocksJSON = string(data)
		}
	}

	// Determine thread_ts and parent_user_id
	var threadTS *string
	var parentUID *string
	if msg.ThreadTimestamp != "" && msg.ThreadTimestamp != msg.Timestamp {
		threadTS = &msg.ThreadTimestamp
		if parentUserID != "" {
			parentUID = &parentUserID
		}
	}

	stmt, err := e.db.Prepare(`
		INSERT OR REPLACE INTO slack_messages
		(id, event_id, user_id, channel_id, text, timestamp, thread_ts, parent_user_id, blocks)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		eventID,
		eventID,
		userID,
		channelID,
		msg.Text,
		msg.Timestamp,
		threadTS,
		parentUID,
		blocksJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	e.stats.messages++
	return nil
}

// handleRateLimitError checks if an error is a rate limit error and sleeps if needed
// Returns true if we should retry, false otherwise
func (e *Exporter) handleRateLimitError(err error) bool {
	var rateLimitErr *slack.RateLimitedError
	if errors.As(err, &rateLimitErr) {
		// Add jitter to avoid thundering herd
		jitter := time.Duration(rand.Int63n(int64(maxJitter)))
		waitTime := rateLimitErr.RetryAfter + rateLimitBuffer + jitter
		e.logger.Info("Rate limited by Slack, waiting before retry",
			zap.Duration("retry_after", rateLimitErr.RetryAfter),
			zap.Duration("buffer", rateLimitBuffer),
			zap.Duration("jitter", jitter),
			zap.Duration("total_wait", waitTime),
		)
		time.Sleep(waitTime)
		return true
	}
	return false
}

func (e *Exporter) printStats() {
	fmt.Println("\n=== Export Statistics ===")
	fmt.Printf("Users:     %d\n", e.stats.users)
	fmt.Printf("Channels:  %d\n", e.stats.channels)
	fmt.Printf("Messages:  %d\n", e.stats.messages)
	fmt.Printf("Threads:   %d\n", e.stats.threads)
	fmt.Printf("Errors:    %d\n", e.stats.errors)
	fmt.Println("========================\n")
}

func (e *Exporter) resetProgress() error {
	_, err := e.db.Exec("DELETE FROM export_progress")
	if err != nil {
		return fmt.Errorf("failed to reset progress: %w", err)
	}
	e.logger.Info("Export progress reset")
	return nil
}

func (e *Exporter) getChannelStatus(channelID string) (string, error) {
	var status string
	err := e.db.QueryRow("SELECT status FROM export_progress WHERE channel_id = ?", channelID).Scan(&status)
	if err == sql.ErrNoRows {
		return "pending", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to get channel status: %w", err)
	}
	return status, nil
}

func (e *Exporter) markChannelStarted(channelID, channelName string) error {
	_, err := e.db.Exec(`
		INSERT INTO export_progress (channel_id, channel_name, status, started_at)
		VALUES (?, ?, 'in_progress', datetime('now'))
		ON CONFLICT(channel_id) DO UPDATE SET
			status = 'in_progress',
			started_at = datetime('now'),
			error_message = NULL
	`, channelID, channelName)
	return err
}

func (e *Exporter) markChannelCompleted(channelID string) error {
	_, err := e.db.Exec(`
		UPDATE export_progress
		SET status = 'completed', completed_at = datetime('now')
		WHERE channel_id = ?
	`, channelID)
	return err
}

func (e *Exporter) markChannelFailed(channelID, errorMsg string) error {
	_, err := e.db.Exec(`
		UPDATE export_progress
		SET status = 'failed', error_message = ?
		WHERE channel_id = ?
	`, errorMsg, channelID)
	return err
}

func newLogger() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return logger.With(zap.String("app", "slack-exporter")), nil
}
