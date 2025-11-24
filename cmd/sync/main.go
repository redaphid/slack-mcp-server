package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/korotovsky/slack-mcp-server/pkg/provider"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slack-go/slack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultDBPath     = "~/THE_SINK/slack/sibi.db"
	defaultSchemaPath = "schema.sql"
	rateLimitBuffer   = 2 * time.Second
)

type Syncer struct {
	db       *sql.DB
	provider *provider.ApiProvider
	logger   *zap.Logger
	ctx      context.Context
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "Note: .env file not loaded: %v\n", err)
	}

	var dbPath string
	var schemaPath string
	flag.StringVar(&dbPath, "db", defaultDBPath, "Path to SQLite database")
	flag.StringVar(&schemaPath, "schema", defaultSchemaPath, "Path to schema SQL file")
	flag.Parse()

	// Expand ~ to home directory
	if dbPath[:2] == "~/" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get home directory: %v\n", err)
			os.Exit(1)
		}
		dbPath = filepath.Join(home, dbPath[2:])
	}

	// Create directory if it doesn't exist
	dbDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create directory %s: %v\n", dbDir, err)
		os.Exit(1)
	}

	// Set up logger
	logConfig := zap.NewProductionConfig()
	logConfig.EncoderConfig.TimeKey = "timestamp"
	logConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logConfig.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	logConfig.InitialFields = map[string]interface{}{
		"app": "slack-sync",
	}
	logger, err := logConfig.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Slack sync", zap.String("db_path", dbPath), zap.String("schema_path", schemaPath))

	ctx := context.Background()

	// Initialize database
	db, err := initDatabase(dbPath, schemaPath, logger)
	if err != nil {
		logger.Fatal("Failed to initialize database", zap.Error(err))
	}
	defer db.Close()

	// Initialize Slack provider
	slackProvider := provider.New("stdio", logger)

	// Wait for provider to be ready and refresh caches
	if err := initSlackProvider(ctx, logger, slackProvider); err != nil {
		logger.Fatal("Failed to initialize Slack provider", zap.Error(err))
	}

	syncer := &Syncer{
		db:       db,
		provider: slackProvider,
		logger:   logger,
		ctx:      ctx,
	}

	if err := syncer.sync(); err != nil {
		logger.Fatal("Sync failed", zap.Error(err))
	}

	logger.Info("Sync completed successfully")
}

func initDatabase(dbPath, schemaPath string, logger *zap.Logger) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Read and execute schema
	schemaSQL, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	if _, err := db.Exec(string(schemaSQL)); err != nil {
		return nil, fmt.Errorf("failed to execute schema: %w", err)
	}

	logger.Info("Database initialized", zap.String("path", dbPath))
	return db, nil
}

func initSlackProvider(ctx context.Context, logger *zap.Logger, p *provider.ApiProvider) error {
	logger.Info("Waiting for Slack provider to initialize...")
	logger.Info("Refreshing users cache...")
	if err := p.RefreshUsers(ctx); err != nil {
		return fmt.Errorf("failed to refresh users cache: %w", err)
	}

	logger.Info("Refreshing channels cache...")
	if err := p.RefreshChannels(ctx); err != nil {
		return fmt.Errorf("failed to refresh channels cache: %w", err)
	}

	logger.Info("Provider is ready")
	return nil
}

func (s *Syncer) sync() error {
	// Sync users
	if err := s.syncUsers(); err != nil {
		return fmt.Errorf("failed to sync users: %w", err)
	}

	// Sync channels
	if err := s.syncChannels(); err != nil {
		return fmt.Errorf("failed to sync channels: %w", err)
	}

	// Get the most recent message timestamp to determine where to start syncing
	var lastTimestamp sql.NullString
	err := s.db.QueryRow("SELECT MAX(timestamp) FROM slack_messages").Scan(&lastTimestamp)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get last message timestamp: %w", err)
	}

	sinceTimestamp := ""
	if lastTimestamp.Valid {
		sinceTimestamp = lastTimestamp.String
	}

	s.logger.Info("Starting message sync", zap.String("since", sinceTimestamp))

	// Sync messages from all channels
	channelsMap := s.provider.ProvideChannelsMaps()
	for channelID, channel := range channelsMap.Channels {
		// Skip kubernetes-events channel
		if channel.Name == "#kubernetes-events" {
			s.logger.Debug("Skipping excluded channel", zap.String("channel_name", channel.Name))
			continue
		}

		if err := s.syncChannelMessages(channelID, channel.Name, sinceTimestamp); err != nil {
			s.logger.Error("Failed to sync channel", zap.String("channel_id", channelID), zap.String("channel_name", channel.Name), zap.Error(err))
			continue
		}
	}

	return nil
}

func (s *Syncer) syncUsers() error {
	s.logger.Info("Syncing users...")

	usersMap := s.provider.ProvideUsersMap()
	users := usersMap.Users

	stmt, err := s.db.Prepare(`
		INSERT OR REPLACE INTO slack_users (user_id, name, display_name, real_name, email, is_bot, avatar_url, profile_url)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	count := 0
	for userID, user := range users {
		displayName := user.Profile.DisplayName
		realName := user.RealName
		email := user.Profile.Email
		avatarURL := user.Profile.Image192
		profileURL := fmt.Sprintf("https://sibi-workspace.slack.com/team/%s", userID)

		_, err := stmt.Exec(
			userID,
			user.Name,
			displayName,
			realName,
			email,
			user.IsBot,
			avatarURL,
			profileURL,
		)
		if err != nil {
			return err
		}
		count++
	}

	s.logger.Info("Users synced", zap.Int("count", count))
	return nil
}

func (s *Syncer) syncChannels() error {
	s.logger.Info("Syncing channels...")

	channelsMap := s.provider.ProvideChannelsMaps()
	channels := channelsMap.Channels

	stmt, err := s.db.Prepare(`
		INSERT OR REPLACE INTO slack_channels (channel_id, name, description, channel_type, is_private, member_count)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	count := 0
	for channelID, channel := range channels {
		channelType := "unknown"
		if channel.IsIM {
			channelType = "dm"
		} else if channel.IsMpIM {
			channelType = "group_dm"
		} else if channel.IsPrivate {
			channelType = "private_channel"
		} else {
			channelType = "public_channel"
		}

		_, err := stmt.Exec(
			channelID,
			channel.Name,
			channel.Purpose,
			channelType,
			channel.IsPrivate,
			channel.MemberCount,
		)
		if err != nil {
			return err
		}
		count++
	}

	s.logger.Info("Channels synced", zap.Int("count", count))
	return nil
}

func (s *Syncer) syncChannelMessages(channelID, channelName, sinceTimestamp string) error {
	s.logger.Debug("Syncing channel messages", zap.String("channel_id", channelID), zap.String("channel_name", channelName), zap.String("since", sinceTimestamp))

	var cursor string
	newMessages := 0

	for {
		params := &slack.GetConversationHistoryParameters{
			ChannelID: channelID,
			Cursor:    cursor,
			Inclusive: false,
			Oldest:    sinceTimestamp, // Only fetch messages newer than this
		}

		var history *slack.GetConversationHistoryResponse
		var err error

		// Retry loop for rate limits
		for {
			history, err = s.provider.Slack().GetConversationHistoryContext(s.ctx, params)
			if err != nil {
				if s.handleRateLimitError(err) {
					continue
				}
				return fmt.Errorf("failed to get conversation history: %w", err)
			}
			break
		}

		for _, msg := range history.Messages {
			if err := s.saveMessage(channelID, &msg); err != nil {
				s.logger.Error("Failed to save message", zap.Error(err))
				continue
			}
			newMessages++

			// If this message has replies, fetch them too
			if msg.ReplyCount > 0 {
				if err := s.syncThreadReplies(channelID, msg.Timestamp, sinceTimestamp); err != nil {
					s.logger.Error("Failed to sync thread replies", zap.String("thread_ts", msg.Timestamp), zap.Error(err))
				}
			}
		}

		if !history.HasMore {
			break
		}
		cursor = history.ResponseMetaData.NextCursor
	}

	if newMessages > 0 {
		s.logger.Info("Synced channel messages", zap.String("channel_name", channelName), zap.Int("new_messages", newMessages))
	}

	return nil
}

func (s *Syncer) syncThreadReplies(channelID, threadTS, sinceTimestamp string) error {
	var cursor string

	for {
		params := &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Cursor:    cursor,
			Oldest:    sinceTimestamp,
		}

		var replies []slack.Message
		var hasMore bool
		var nextCursor string
		var err error

		for {
			replies, hasMore, nextCursor, err = s.provider.Slack().GetConversationRepliesContext(s.ctx, params)
			if err != nil {
				if s.handleRateLimitError(err) {
					continue
				}
				return fmt.Errorf("failed to get thread replies: %w", err)
			}
			break
		}

		for _, msg := range replies {
			if msg.Timestamp == threadTS {
				continue // Skip the parent message
			}
			if err := s.saveMessage(channelID, &msg); err != nil {
				s.logger.Error("Failed to save reply", zap.Error(err))
			}
		}

		if !hasMore {
			break
		}
		cursor = nextCursor
	}

	return nil
}

func (s *Syncer) saveMessage(channelID string, msg *slack.Message) error {
	stmt, err := s.db.Prepare(`
		INSERT OR IGNORE INTO slack_messages (id, event_id, user_id, channel_id, text, timestamp, thread_ts, parent_user_id, blocks)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	eventID := msg.Timestamp + "-" + channelID

	var threadTS *string
	if msg.ThreadTimestamp != "" && msg.ThreadTimestamp != msg.Timestamp {
		threadTS = &msg.ThreadTimestamp
	}

	var parentUserID *string
	if msg.ParentUserId != "" {
		parentUserID = &msg.ParentUserId
	}

	blocks := "[]"
	if len(msg.Blocks.BlockSet) > 0 {
		if blocksJSON, err := msg.Blocks.MarshalJSON(); err == nil {
			blocks = string(blocksJSON)
		}
	}

	_, err = stmt.Exec(
		eventID,
		eventID,
		msg.User,
		channelID,
		msg.Text,
		msg.Timestamp,
		threadTS,
		parentUserID,
		blocks,
	)

	return err
}

func (s *Syncer) handleRateLimitError(err error) bool {
	var rateLimitErr *slack.RateLimitedError
	if errors.As(err, &rateLimitErr) {
		waitTime := rateLimitErr.RetryAfter + rateLimitBuffer
		s.logger.Info("Rate limited by Slack, waiting before retry",
			zap.Duration("retry_after", rateLimitErr.RetryAfter),
			zap.Duration("total_wait", waitTime),
		)
		time.Sleep(waitTime)
		return true
	}
	return false
}
