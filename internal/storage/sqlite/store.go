// Package sqlite provides the embedded SQLite delivery store.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/five-zombie/agent-pipe/internal/id"
	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
	_ "modernc.org/sqlite"
)

var ErrPipeNotFound = errors.New("pipe not found")
var ErrNotMember = errors.New("client is not a member of pipe")
var ErrIdempotencyConflict = errors.New("client_message_id already belongs to a different message")
var ErrQueueFull = errors.New("pending message queue is full")

const (
	MaxPayloadBytes    = 1 << 20
	MaxPendingMessages = 10000
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db}
	if err := store.configure(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) configure(ctx context.Context) error {
	pragmas := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure sqlite: %w", err)
		}
	}
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pipes (
    id TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipe_members (
    pipe_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    name TEXT NOT NULL,
    joined_at TEXT NOT NULL,
    PRIMARY KEY (pipe_id, client_id),
    FOREIGN KEY (pipe_id) REFERENCES pipes(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    pipe_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL,
    client_message_id TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    reply_to TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    claimed_by TEXT,
    claim_until TEXT,
    delivery_seq INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (pipe_id) REFERENCES pipes(id) ON DELETE CASCADE
);
`)
	if err != nil {
		return fmt.Errorf("create sqlite schema: %w", err)
	}
	if err := s.ensureMessageColumn(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS messages_delivery_idx
    ON messages (pipe_id, receiver_id, status, claim_until, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS messages_idempotency_idx
    ON messages (sender_id, client_message_id)
    WHERE client_message_id <> '';
`); err != nil {
		return fmt.Errorf("create message indexes: %w", err)
	}
	return nil
}

func (s *Store) ensureMessageColumn(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(messages)`)
	if err != nil {
		return fmt.Errorf("inspect message schema: %w", err)
	}
	defer rows.Close()
	foundClientMessageID := false
	foundDeliverySeq := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("read message schema: %w", err)
		}
		if name == "client_message_id" {
			foundClientMessageID = true
		}
		if name == "delivery_seq" {
			foundDeliverySeq = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate message schema: %w", err)
	}
	if !foundClientMessageID {
		if _, err := s.db.ExecContext(ctx,
			`ALTER TABLE messages ADD COLUMN client_message_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate message schema: %w", err)
		}
	}
	if !foundDeliverySeq {
		if _, err := s.db.ExecContext(ctx,
			`ALTER TABLE messages ADD COLUMN delivery_seq INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("migrate delivery sequence: %w", err)
		}
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) CreatePipe(ctx context.Context, req pipe.CreateRequest) (pipe.Pipe, error) {
	if req.ClientID == "" || req.Desc == "" {
		return pipe.Pipe{}, errors.New("client_id and decs are required")
	}
	pipeID := req.ID
	if pipeID == "" {
		var err error
		pipeID, err = id.New()
		if err != nil {
			return pipe.Pipe{}, err
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return pipe.Pipe{}, fmt.Errorf("begin create pipe: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pipes (id, description, created_at) VALUES (?, ?, ?)`,
		pipeID, req.Desc, now); err != nil {
		return pipe.Pipe{}, fmt.Errorf("create pipe: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pipe_members (pipe_id, client_id, name, joined_at) VALUES (?, ?, ?, ?)`,
		pipeID, req.ClientID, req.Name, now); err != nil {
		return pipe.Pipe{}, fmt.Errorf("join creator: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return pipe.Pipe{}, fmt.Errorf("commit pipe: %w", err)
	}
	return pipe.Pipe{ID: pipeID, Desc: req.Desc, CreatedAt: now}, nil
}

func (s *Store) JoinPipe(ctx context.Context, pipeID string, req pipe.JoinRequest) (pipe.Pipe, error) {
	if req.ClientID == "" || req.Name == "" {
		return pipe.Pipe{}, errors.New("client_id and name are required")
	}
	var result pipe.Pipe
	err := s.db.QueryRowContext(ctx,
		`SELECT id, description, created_at FROM pipes WHERE id = ?`, pipeID).
		Scan(&result.ID, &result.Desc, &result.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return pipe.Pipe{}, ErrPipeNotFound
	}
	if err != nil {
		return pipe.Pipe{}, fmt.Errorf("find pipe: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO pipe_members (pipe_id, client_id, name, joined_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (pipe_id, client_id) DO UPDATE SET name = excluded.name
`, pipeID, req.ClientID, req.Name, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return pipe.Pipe{}, fmt.Errorf("join pipe: %w", err)
	}
	return result, nil
}

func (s *Store) CreateMessage(ctx context.Context, req message.SendRequest) (message.Receipt, error) {
	if req.PipeID == "" || req.SenderID == "" || req.ReceiverID == "" || req.ClientMessageID == "" || req.Payload == "" {
		return message.Receipt{}, errors.New("pipe_id, sender_id, receiver_id, client_message_id, and payload are required")
	}
	if len([]byte(req.Payload)) > MaxPayloadBytes {
		return message.Receipt{}, fmt.Errorf("payload exceeds %d bytes", MaxPayloadBytes)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return message.Receipt{}, fmt.Errorf("begin message transaction: %w", err)
	}
	defer tx.Rollback()
	if !memberQuery(ctx, tx, req.PipeID, req.SenderID) {
		return message.Receipt{}, ErrNotMember
	}
	if !memberQuery(ctx, tx, req.PipeID, req.ReceiverID) {
		return message.Receipt{}, errors.New("receiver is not a member of pipe")
	}

	var existing message.Receipt
	var existingSender, existingReceiver, existingPayload, existingReply string
	err = tx.QueryRowContext(ctx, `
SELECT id, pipe_id, sender_id, receiver_id, client_message_id, payload,
       reply_to, status, created_at
FROM messages WHERE sender_id = ? AND client_message_id = ?
`, req.SenderID, req.ClientMessageID).Scan(
		&existing.ID, &existing.PipeID, &existingSender, &existingReceiver,
		&existing.ClientMessageID, &existingPayload, &existingReply,
		&existing.Status, &existing.CreatedAt,
	)
	if err == nil {
		if existing.PipeID != req.PipeID || existingReceiver != req.ReceiverID ||
			existingPayload != req.Payload || existingReply != req.ReplyTo {
			return message.Receipt{}, ErrIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return message.Receipt{}, fmt.Errorf("check duplicate message: %w", err)
	}

	var pending int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE status = ?`, message.StatusPending).Scan(&pending); err != nil {
		return message.Receipt{}, fmt.Errorf("count pending messages: %w", err)
	}
	if pending >= MaxPendingMessages {
		return message.Receipt{}, ErrQueueFull
	}

	messageID, err := id.New()
	if err != nil {
		return message.Receipt{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `
INSERT INTO messages
    (id, pipe_id, sender_id, receiver_id, client_message_id, payload, reply_to, status, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`, messageID, req.PipeID, req.SenderID, req.ReceiverID, req.ClientMessageID,
		req.Payload, req.ReplyTo, message.StatusPending, now)
	if err != nil {
		return message.Receipt{}, fmt.Errorf("store message: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return message.Receipt{}, fmt.Errorf("commit message: %w", err)
	}
	return message.Receipt{ID: messageID, PipeID: req.PipeID, ClientMessageID: req.ClientMessageID, Status: message.StatusPending, CreatedAt: now}, nil
}

func (s *Store) Status(ctx context.Context, messageID string) (message.StatusResponse, error) {
	var result message.StatusResponse
	err := s.db.QueryRowContext(ctx, `
SELECT id, pipe_id, sender_id, receiver_id, client_message_id, reply_to, status, created_at,
       COALESCE(sent_at, ''), delivery_seq
FROM messages WHERE id = ?
`, messageID).Scan(
		&result.ID, &result.PipeID, &result.SenderID, &result.ReceiverID,
		&result.ClientMessageID, &result.ReplyTo, &result.Status, &result.CreatedAt, &result.SentAt,
		&result.DeliverySeq,
	)
	if err != nil {
		return message.StatusResponse{}, err
	}
	return result, nil
}

func (s *Store) ClaimNext(ctx context.Context, pipeID, receiverID, owner string, lease time.Duration) (message.Message, error) {
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	untilText := now.Add(lease).Format(time.RFC3339Nano)
	var result message.Message
	err := s.db.QueryRowContext(ctx, `
UPDATE messages
SET claimed_by = ?, claim_until = ?,
    delivery_seq = CASE WHEN delivery_seq = 0 THEN
        (SELECT COALESCE(MAX(delivery_seq), 0) + 1 FROM messages)
        ELSE delivery_seq END
WHERE id = (
    SELECT id FROM messages
    WHERE pipe_id = ? AND receiver_id = ? AND status = ?
      AND (claim_until IS NULL OR claim_until < ?)
    ORDER BY created_at, id
    LIMIT 1
)
RETURNING id, pipe_id, sender_id, receiver_id, client_message_id, payload, reply_to, status,
          created_at, COALESCE(sent_at, ''), delivery_seq
`, owner, untilText, pipeID, receiverID, message.StatusPending, nowText).Scan(
		&result.ID, &result.PipeID, &result.SenderID, &result.ReceiverID,
		&result.ClientMessageID, &result.Payload, &result.ReplyTo, &result.Status,
		&result.CreatedAt, &result.SentAt, &result.DeliverySeq,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return message.Message{}, nil
	}
	if err != nil {
		return message.Message{}, fmt.Errorf("claim message: %w", err)
	}
	return result, nil
}

func (s *Store) MarkSent(ctx context.Context, messageID, owner string) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE messages
SET status = ?, sent_at = ?, claimed_by = NULL, claim_until = NULL
WHERE id = ? AND status = ? AND claimed_by = ?
`, message.StatusSent, time.Now().UTC().Format(time.RFC3339Nano), messageID,
		message.StatusPending, owner)
	if err != nil {
		return fmt.Errorf("mark message sent: %w", err)
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return errors.New("message claim is no longer owned")
	}
	return nil
}

func (s *Store) ReplaySent(ctx context.Context, pipeID, receiverID string, afterSeq int64, limit int) ([]message.Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, pipe_id, sender_id, receiver_id, client_message_id, payload,
       reply_to, status, created_at, COALESCE(sent_at, ''), delivery_seq
FROM messages
WHERE pipe_id = ? AND receiver_id = ? AND status = ? AND delivery_seq > ?
ORDER BY delivery_seq
LIMIT ?
`, pipeID, receiverID, message.StatusSent, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("replay sent messages: %w", err)
	}
	defer rows.Close()
	var result []message.Message
	for rows.Next() {
		var item message.Message
		if err := rows.Scan(
			&item.ID, &item.PipeID, &item.SenderID, &item.ReceiverID,
			&item.ClientMessageID, &item.Payload, &item.ReplyTo, &item.Status,
			&item.CreatedAt, &item.SentAt, &item.DeliverySeq,
		); err != nil {
			return nil, fmt.Errorf("scan replay message: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replay messages: %w", err)
	}
	return result, nil
}

func (s *Store) ReleaseClaims(ctx context.Context, owner string) error {
	if owner == "" {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
UPDATE messages SET claimed_by = NULL, claim_until = NULL
WHERE status = ? AND claimed_by = ?
`, message.StatusPending, owner); err != nil {
		return fmt.Errorf("release message claims: %w", err)
	}
	return nil
}

func (s *Store) IsMember(ctx context.Context, pipeID, clientID string) bool {
	return s.member(ctx, pipeID, clientID)
}

func (s *Store) Cleanup(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-retention).Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
DELETE FROM messages WHERE status = ? AND sent_at IS NOT NULL AND sent_at < ?
`, message.StatusSent, cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup sent messages: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count cleaned messages: %w", err)
	}
	return count, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func memberQuery(ctx context.Context, q queryer, pipeID, clientID string) bool {
	var one int
	err := q.QueryRowContext(ctx,
		`SELECT 1 FROM pipe_members WHERE pipe_id = ? AND client_id = ?`,
		pipeID, clientID).Scan(&one)
	return err == nil && one == 1
}

func (s *Store) member(ctx context.Context, pipeID, clientID string) bool {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM pipe_members WHERE pipe_id = ? AND client_id = ?`,
		pipeID, clientID).Scan(&one)
	return err == nil && one == 1
}
