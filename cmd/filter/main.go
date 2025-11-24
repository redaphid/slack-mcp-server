package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const defaultSchemaPath = "./schema.sql"

type Config struct {
	SourceDB   string
	OutputDB   string
	Channels   []string
	SchemaPath string
}

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "Note: .env file not loaded (this is fine if env vars are set): %v\n", err)
	}

	var channelsFlag string
	var config Config

	flag.StringVar(&config.SourceDB, "source", "", "Path to source SQLite database (required)")
	flag.StringVar(&config.OutputDB, "output", "", "Path to output SQLite database (required)")
	flag.StringVar(&channelsFlag, "channels", "", "Comma-separated list of channel IDs or names (e.g., 'C123,general,#random')")
	flag.StringVar(&config.SchemaPath, "schema", defaultSchemaPath, "Path to schema.sql file")
	flag.Parse()

	if config.SourceDB == "" || config.OutputDB == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "\nError: Both -source and -output are required\n")
		os.Exit(1)
	}

	if channelsFlag == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "\nError: -channels is required\n")
		os.Exit(1)
	}

	// Parse channels
	config.Channels = parseChannels(channelsFlag)

	logger, err := newLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Slack database filter",
		zap.String("source", config.SourceDB),
		zap.String("output", config.OutputDB),
		zap.Strings("channels", config.Channels),
	)

	if err := filterDatabase(config, logger); err != nil {
		logger.Fatal("Filter failed", zap.Error(err))
	}

	logger.Info("Filter completed successfully!")
}

func parseChannels(channelsFlag string) []string {
	parts := strings.Split(channelsFlag, ",")
	var channels []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			channels = append(channels, part)
		}
	}
	return channels
}

func filterDatabase(config Config, logger *zap.Logger) error {
	// Open source database
	sourceDB, err := sql.Open("sqlite3", config.SourceDB)
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer sourceDB.Close()

	// Create output database
	outputDB, err := initOutputDatabase(config.OutputDB, config.SchemaPath, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize output database: %w", err)
	}
	defer outputDB.Close()

	// Get channel IDs that match the provided names/IDs
	channelIDs, err := resolveChannelIDs(sourceDB, config.Channels, logger)
	if err != nil {
		return fmt.Errorf("failed to resolve channel IDs: %w", err)
	}

	if len(channelIDs) == 0 {
		return fmt.Errorf("no matching channels found")
	}

	logger.Info("Found matching channels", zap.Int("count", len(channelIDs)))

	// Copy channels
	if err := copyChannels(sourceDB, outputDB, channelIDs, logger); err != nil {
		return fmt.Errorf("failed to copy channels: %w", err)
	}

	// Copy messages
	messageCount, err := copyMessages(sourceDB, outputDB, channelIDs, logger)
	if err != nil {
		return fmt.Errorf("failed to copy messages: %w", err)
	}

	// Get unique user IDs from copied messages
	userIDs, err := getUniqueUserIDs(outputDB, logger)
	if err != nil {
		return fmt.Errorf("failed to get user IDs: %w", err)
	}

	// Copy users
	if err := copyUsers(sourceDB, outputDB, userIDs, logger); err != nil {
		return fmt.Errorf("failed to copy users: %w", err)
	}

	logger.Info("Filter complete",
		zap.Int("channels", len(channelIDs)),
		zap.Int("messages", messageCount),
		zap.Int("users", len(userIDs)),
	)

	return nil
}

func initOutputDatabase(dbPath, schemaPath string, logger *zap.Logger) (*sql.DB, error) {
	// Remove existing file if it exists
	if _, err := os.Stat(dbPath); err == nil {
		logger.Info("Removing existing output database", zap.String("path", dbPath))
		if err := os.Remove(dbPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing database: %w", err)
		}
	}

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

	logger.Info("Output database initialized", zap.String("path", dbPath))
	return db, nil
}

func resolveChannelIDs(db *sql.DB, channels []string, logger *zap.Logger) ([]string, error) {
	// Build query to find channels by ID or name (with or without # or @ prefix)
	placeholders := make([]string, len(channels))
	args := make([]interface{}, len(channels)*4)
	for i, ch := range channels {
		// Match by channel_id, exact name, name with #, or name with @
		placeholders[i] = "(channel_id = ? OR name = ? OR name = ? OR name = ?)"
		args[i*4] = ch
		args[i*4+1] = ch
		args[i*4+2] = "#" + strings.TrimPrefix(ch, "#")
		args[i*4+3] = "@" + strings.TrimPrefix(ch, "@")
	}

	query := fmt.Sprintf(`
		SELECT channel_id, name
		FROM slack_channels
		WHERE %s
	`, strings.Join(placeholders, " OR "))

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query channels: %w", err)
	}
	defer rows.Close()

	var channelIDs []string
	channelMap := make(map[string]string) // id -> name

	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("failed to scan channel: %w", err)
		}
		channelIDs = append(channelIDs, id)
		channelMap[id] = name
	}

	for id, name := range channelMap {
		logger.Info("Matched channel", zap.String("id", id), zap.String("name", name))
	}

	return channelIDs, nil
}

func copyChannels(sourceDB, outputDB *sql.DB, channelIDs []string, logger *zap.Logger) error {
	placeholders := make([]string, len(channelIDs))
	args := make([]interface{}, len(channelIDs))
	for i, id := range channelIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT channel_id, name, description, channel_type, is_private, member_count, created_at, updated_at
		FROM slack_channels
		WHERE channel_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := sourceDB.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query channels: %w", err)
	}
	defer rows.Close()

	stmt, err := outputDB.Prepare(`
		INSERT INTO slack_channels
		(channel_id, name, description, channel_type, is_private, member_count, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	count := 0
	for rows.Next() {
		var id, name, desc, chanType, createdAt, updatedAt string
		var isPrivate, memberCount int
		if err := rows.Scan(&id, &name, &desc, &chanType, &isPrivate, &memberCount, &createdAt, &updatedAt); err != nil {
			return fmt.Errorf("failed to scan channel: %w", err)
		}

		if _, err := stmt.Exec(id, name, desc, chanType, isPrivate, memberCount, createdAt, updatedAt); err != nil {
			return fmt.Errorf("failed to insert channel: %w", err)
		}
		count++
	}

	logger.Info("Copied channels", zap.Int("count", count))
	return nil
}

func copyMessages(sourceDB, outputDB *sql.DB, channelIDs []string, logger *zap.Logger) (int, error) {
	placeholders := make([]string, len(channelIDs))
	args := make([]interface{}, len(channelIDs))
	for i, id := range channelIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, event_id, user_id, channel_id, text, timestamp, thread_ts, parent_user_id, blocks, created_at
		FROM slack_messages
		WHERE channel_id IN (%s)
		ORDER BY timestamp ASC
	`, strings.Join(placeholders, ","))

	rows, err := sourceDB.Query(query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	stmt, err := outputDB.Prepare(`
		INSERT INTO slack_messages
		(id, event_id, user_id, channel_id, text, timestamp, thread_ts, parent_user_id, blocks, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	count := 0
	for rows.Next() {
		var id, eventID, userID, channelID, timestamp, blocks, createdAt string
		var text, threadTS, parentUserID sql.NullString

		if err := rows.Scan(&id, &eventID, &userID, &channelID, &text, &timestamp, &threadTS, &parentUserID, &blocks, &createdAt); err != nil {
			return 0, fmt.Errorf("failed to scan message: %w", err)
		}

		if _, err := stmt.Exec(id, eventID, userID, channelID, text, timestamp, threadTS, parentUserID, blocks, createdAt); err != nil {
			return 0, fmt.Errorf("failed to insert message: %w", err)
		}
		count++

		if count%1000 == 0 {
			logger.Info("Copying messages...", zap.Int("count", count))
		}
	}

	logger.Info("Copied messages", zap.Int("count", count))
	return count, nil
}

func getUniqueUserIDs(db *sql.DB, logger *zap.Logger) ([]string, error) {
	rows, err := db.Query(`
		SELECT DISTINCT user_id FROM slack_messages
		UNION
		SELECT DISTINCT parent_user_id FROM slack_messages WHERE parent_user_id IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query user IDs: %w", err)
	}
	defer rows.Close()

	var userIDs []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("failed to scan user ID: %w", err)
		}
		userIDs = append(userIDs, userID)
	}

	logger.Info("Found unique users", zap.Int("count", len(userIDs)))
	return userIDs, nil
}

func copyUsers(sourceDB, outputDB *sql.DB, userIDs []string, logger *zap.Logger) error {
	if len(userIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(userIDs))
	args := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT user_id, name, display_name, real_name, email, is_bot, avatar_url, profile_url, created_at, updated_at
		FROM slack_users
		WHERE user_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := sourceDB.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query users: %w", err)
	}
	defer rows.Close()

	stmt, err := outputDB.Prepare(`
		INSERT INTO slack_users
		(user_id, name, display_name, real_name, email, is_bot, avatar_url, profile_url, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	count := 0
	for rows.Next() {
		var userID, name, createdAt, updatedAt string
		var displayName, realName, email, avatarURL, profileURL sql.NullString
		var isBot int

		if err := rows.Scan(&userID, &name, &displayName, &realName, &email, &isBot, &avatarURL, &profileURL, &createdAt, &updatedAt); err != nil {
			return fmt.Errorf("failed to scan user: %w", err)
		}

		if _, err := stmt.Exec(userID, name, displayName, realName, email, isBot, avatarURL, profileURL, createdAt, updatedAt); err != nil {
			return fmt.Errorf("failed to insert user: %w", err)
		}
		count++
	}

	logger.Info("Copied users", zap.Int("count", count))
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

	return logger.With(zap.String("app", "slack-filter")), nil
}
