package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

const defaultDBPath = "./slack_export.db"

type Server struct {
	db *sql.DB
}

type Channel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	IsPrivate   bool   `json:"is_private"`
	MemberCount int    `json:"member_count"`
}

type User struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	RealName    string `json:"real_name"`
	Email       string `json:"email"`
	IsBot       bool   `json:"is_bot"`
	AvatarURL   string `json:"avatar_url"`
}

type Message struct {
	ID           string  `json:"id"`
	EventID      string  `json:"event_id"`
	UserID       string  `json:"user_id"`
	ChannelID    string  `json:"channel_id"`
	Text         *string `json:"text"`
	Timestamp    string  `json:"timestamp"`
	ThreadTS     *string `json:"thread_ts"`
	ParentUserID *string `json:"parent_user_id"`
	CreatedAt    string  `json:"created_at"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "Note: .env file not loaded: %v\n", err)
	}

	var dbPath string
	var port int
	flag.StringVar(&dbPath, "db", defaultDBPath, "Path to SQLite database")
	flag.IntVar(&port, "port", 8888, "Port to listen on")
	flag.Parse()

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	srv := &Server{db: db}

	http.HandleFunc("/api/channels", srv.handleChannels)
	http.HandleFunc("/api/messages", srv.handleMessages)
	http.HandleFunc("/api/users", srv.handleUsers)
	http.HandleFunc("/api/stats", srv.handleStats)
	http.Handle("/", http.FileServer(http.Dir("./static")))

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting Slack data viewer on http://localhost%s\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT channel_id, name, description, channel_type, is_private, member_count
		FROM slack_channels
		ORDER BY name
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	channels := []Channel{}
	for rows.Next() {
		var c Channel
		var isPrivate int
		err := rows.Scan(&c.ID, &c.Name, &c.Description, &c.Type, &isPrivate, &c.MemberCount)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		c.IsPrivate = isPrivate == 1
		channels = append(channels, c)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(channels)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	channelID := r.URL.Query().Get("channel_id")
	if channelID == "" {
		http.Error(w, "channel_id required", http.StatusBadRequest)
		return
	}

	rows, err := s.db.Query(`
		SELECT id, event_id, user_id, channel_id, text, timestamp, thread_ts, parent_user_id, created_at
		FROM slack_messages
		WHERE channel_id = ?
		ORDER BY timestamp DESC
		LIMIT 100
	`, channelID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	messages := []Message{}
	for rows.Next() {
		var m Message
		var text, threadTS, parentUserID sql.NullString
		err := rows.Scan(&m.ID, &m.EventID, &m.UserID, &m.ChannelID, &text, &m.Timestamp, &threadTS, &parentUserID, &m.CreatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if text.Valid {
			m.Text = &text.String
		}
		if threadTS.Valid {
			m.ThreadTS = &threadTS.String
		}
		if parentUserID.Valid {
			m.ParentUserID = &parentUserID.String
		}
		messages = append(messages, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT user_id, name, display_name, real_name, email, is_bot, avatar_url
		FROM slack_users
		ORDER BY name
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	users := []User{}
	for rows.Next() {
		var u User
		var displayName, realName, email, avatarURL sql.NullString
		var isBot int
		err := rows.Scan(&u.ID, &u.Name, &displayName, &realName, &email, &isBot, &avatarURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if displayName.Valid {
			u.DisplayName = displayName.String
		}
		if realName.Valid {
			u.RealName = realName.String
		}
		if email.Valid {
			u.Email = email.String
		}
		if avatarURL.Valid {
			u.AvatarURL = avatarURL.String
		}
		u.IsBot = isBot == 1
		users = append(users, u)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var messageCount, userCount, channelCount int

	err := s.db.QueryRow("SELECT COUNT(*) FROM slack_messages").Scan(&messageCount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.db.QueryRow("SELECT COUNT(*) FROM slack_users").Scan(&userCount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.db.QueryRow("SELECT COUNT(*) FROM slack_channels").Scan(&channelCount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats := map[string]int{
		"messages": messageCount,
		"users":    userCount,
		"channels": channelCount,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
