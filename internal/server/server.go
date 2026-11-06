// Package server contains the HTTP and SSE surface of pipe-server.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/five-zombie/agent-pipe/internal/auth"
	"github.com/five-zombie/agent-pipe/internal/id"
	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
	"github.com/five-zombie/agent-pipe/internal/storage/sqlite"
)

const (
	claimLease      = 30 * time.Second
	pollInterval    = 250 * time.Millisecond
	maxRequestBytes = 2 << 20
)

type Server struct {
	Store  *sqlite.Store
	Auth   *auth.Verifier
	Logger *log.Logger
}

func New(store *sqlite.Store, verifier *auth.Verifier, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	if verifier == nil {
		verifier = auth.New(nil, "")
	}
	return &Server{Store: store, Auth: verifier, Logger: logger}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serveHTTP)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/pipes":
		s.createPipe(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/pipes/"):
		s.pipeRoute(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		s.createMessage(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/messages/"):
		s.messageRoute(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request, clientID string) bool {
	if err := s.Auth.Verify(r, clientID); err != nil {
		status := http.StatusUnauthorized
		if errors.Is(err, auth.ErrMissingIdentity) || errors.Is(err, auth.ErrIdentityMismatch) {
			status = http.StatusForbidden
		}
		writeError(w, status, err.Error())
		return false
	}
	return true
}

func (s *Server) createPipe(w http.ResponseWriter, r *http.Request) {
	var req pipe.CreateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.authorize(w, r, req.ClientID) {
		return
	}
	result, err := s.Store.CreatePipe(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) pipeRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/pipes/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		writeError(w, http.StatusNotFound, "pipe route not found")
		return
	}
	pipeID, action := parts[0], parts[1]
	switch {
	case r.Method == http.MethodPost && action == "join":
		s.joinPipe(w, r, pipeID)
	case r.Method == http.MethodGet && action == "events":
		s.events(w, r, pipeID)
	default:
		writeError(w, http.StatusNotFound, "pipe route not found")
	}
}

func (s *Server) joinPipe(w http.ResponseWriter, r *http.Request, pipeID string) {
	var req pipe.JoinRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.authorize(w, r, req.ClientID) {
		return
	}
	result, err := s.Store.JoinPipe(r.Context(), pipeID, req)
	if errors.Is(err, sqlite.ErrPipeNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createMessage(w http.ResponseWriter, r *http.Request) {
	var req message.SendRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.authorize(w, r, req.SenderID) {
		return
	}
	result, err := s.Store.CreateMessage(r.Context(), req)
	if errors.Is(err, sqlite.ErrNotMember) {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	if errors.Is(err, sqlite.ErrQueueFull) {
		writeError(w, http.StatusTooManyRequests, err.Error())
		return
	}
	if errors.Is(err, sqlite.ErrIdempotencyConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.Logger.Printf("message admitted id=%s pipe_id=%s sender_id=%s receiver_id=%s client_message_id=%s", result.ID, result.PipeID, req.SenderID, req.ReceiverID, req.ClientMessageID)
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) messageRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/messages/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "status" {
		writeError(w, http.StatusNotFound, "message route not found")
		return
	}
	clientID := r.URL.Query().Get("client_id")
	if !s.authorize(w, r, clientID) {
		return
	}
	result, err := s.Store.Status(r.Context(), parts[0])
	if errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusNotFound, "message not found")
		return
	}
	if result.SenderID != clientID && result.ReceiverID != clientID {
		writeError(w, http.StatusForbidden, "client cannot read this message status")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, pipeID string) {
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		writeError(w, http.StatusBadRequest, "client_id is required")
		return
	}
	if !s.authorize(w, r, clientID) {
		return
	}
	if !s.Store.IsMember(r.Context(), pipeID, clientID) {
		writeError(w, http.StatusForbidden, "client is not a member of pipe")
		return
	}
	lastEventID := r.Header.Get("Last-Event-ID")
	var afterSeq int64
	if lastEventID != "" {
		var err error
		afterSeq, err = strconv.ParseInt(lastEventID, 10, 64)
		if err != nil || afterSeq < 0 {
			writeError(w, http.StatusBadRequest, "Last-Event-ID must be a non-negative delivery sequence")
			return
		}
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ownerID, err := id.New()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create stream owner")
		return
	}
	owner := clientID + ":" + ownerID
	defer func() {
		if err := s.Store.ReleaseClaims(context.Background(), owner); err != nil {
			s.Logger.Printf("release stream claims client_id=%s: %v", clientID, err)
		}
	}()
	if lastEventID != "" {
		replay, err := s.Store.ReplaySent(r.Context(), pipeID, clientID, afterSeq, 100)
		if err != nil {
			s.Logger.Printf("replay messages client_id=%s: %v", clientID, err)
			return
		}
		for _, msg := range replay {
			if err := writeEvent(w, flusher, msg); err != nil {
				return
			}
		}
	}
	poll := time.NewTicker(pollInterval)
	heartbeat := time.NewTicker(10 * time.Second)
	defer poll.Stop()
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-poll.C:
			msg, err := s.Store.ClaimNext(r.Context(), pipeID, clientID, owner, claimLease)
			if err != nil {
				s.Logger.Printf("claim message for %s: %v", clientID, err)
				return
			}
			if msg.ID == "" {
				continue
			}
			if err := writeEvent(w, flusher, msg); err != nil {
				return
			}
			if err := s.Store.MarkSent(r.Context(), msg.ID, owner); err != nil {
				// The lease remains until it expires, so the message can be retried.
				s.Logger.Printf("mark message %s sent: %v", msg.ID, err)
			} else {
				s.Logger.Printf("message sent id=%s pipe_id=%s receiver_id=%s", msg.ID, msg.PipeID, msg.ReceiverID)
			}
		}
	}
}

func writeEvent(w http.ResponseWriter, flusher http.Flusher, msg message.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	eventID := strconv.FormatInt(msg.DeliverySeq, 10)
	if msg.DeliverySeq == 0 {
		eventID = msg.ID
	}
	if _, err := fmt.Fprintf(w, "event: message\nid: %s\ndata: %s\n\n", eventID, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
