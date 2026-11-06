package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/five-zombie/agent-pipe/internal/auth"
	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
	"github.com/five-zombie/agent-pipe/internal/server"
	"github.com/five-zombie/agent-pipe/internal/storage/sqlite"
	httptransport "github.com/five-zombie/agent-pipe/internal/transport/http"
)

func TestAuthenticatedDeliveryAndStatusOwnership(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir()+"/pipe.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	verifier := auth.New(map[string]string{
		"client-a": "token-a",
		"client-b": "token-b",
		"client-c": "token-c",
	}, "")
	testServer := httptest.NewServer(server.New(store, verifier, log.New(io.Discard, "", 0)).Handler())
	defer testServer.Close()

	clientA := httptransport.New(testServer.URL, "token-a", "client-a")
	clientB := httptransport.New(testServer.URL, "token-b", "client-b")
	clientC := httptransport.New(testServer.URL, "token-c", "client-c")

	created, err := clientA.CreatePipe(ctx, pipe.CreateRequest{
		ClientID: "client-a",
		Name:     "work",
		Desc:     "work pipe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientB.Join(ctx, created.ID, pipe.JoinRequest{ClientID: "client-b", Name: "work"}); err != nil {
		t.Fatal(err)
	}

	receipt, err := clientA.Send(ctx, message.SendRequest{
		PipeID:          created.ID,
		SenderID:        "client-a",
		ReceiverID:      "client-b",
		ClientMessageID: "client-a-message-1",
		Payload:         "hello from A",
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != message.StatusPending {
		t.Fatalf("expected pending receipt, got %q", receipt.Status)
	}
	duplicate, err := clientA.Send(ctx, message.SendRequest{
		PipeID:          created.ID,
		SenderID:        "client-a",
		ReceiverID:      "client-b",
		ClientMessageID: "client-a-message-1",
		Payload:         "hello from A",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.ID != receipt.ID {
		t.Fatalf("duplicate request created a new message: first=%s duplicate=%s", receipt.ID, duplicate.ID)
	}
	if _, err := clientA.Send(ctx, message.SendRequest{
		PipeID:          created.ID,
		SenderID:        "client-a",
		ReceiverID:      "client-b",
		ClientMessageID: "client-a-message-1",
		Payload:         "different payload",
	}); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	listenCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	body, err := clientB.Listen(listenCtx, created.ID, "client-b", "")
	if err != nil {
		t.Fatal(err)
	}
	event, err := readEvent(body)
	body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if event.ID != receipt.ID || event.Payload != "hello from A" {
		t.Fatalf("unexpected event: %+v", event)
	}

	var status message.StatusResponse
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status, err = clientA.Status(ctx, receipt.ID, "client-a")
		if err != nil {
			t.Fatal(err)
		}
		if status.Status == message.StatusSent {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.Status != message.StatusSent {
		t.Fatalf("expected sent status, got %q", status.Status)
	}

	replayCtx, replayCancel := context.WithTimeout(ctx, time.Second)
	replayBody, err := clientB.Listen(replayCtx, created.ID, "client-b", "0")
	if err != nil {
		replayCancel()
		t.Fatal(err)
	}
	replayed, err := readEvent(replayBody)
	replayBody.Close()
	replayCancel()
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != receipt.ID {
		t.Fatalf("expected replay of %s, got %s", receipt.ID, replayed.ID)
	}

	if _, err := clientC.Status(ctx, receipt.ID, "client-c"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected third client status denial, got %v", err)
	}

	wrongToken := httptransport.New(testServer.URL, "wrong-token", "client-a")
	if _, err := wrongToken.CreatePipe(ctx, pipe.CreateRequest{
		ClientID: "client-a",
		Name:     "blocked",
		Desc:     "blocked pipe",
	}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected invalid token rejection, got %v", err)
	}
}

func TestPendingMessageSurvivesServerRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := t.TempDir() + "/pipe.db"
	verifier := auth.New(map[string]string{
		"client-a": "token-a",
		"client-b": "token-b",
	}, "")

	store, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first := httptest.NewServer(server.New(store, verifier, log.New(io.Discard, "", 0)).Handler())
	clientA := httptransport.New(first.URL, "token-a", "client-a")
	clientB := httptransport.New(first.URL, "token-b", "client-b")
	created, err := clientA.CreatePipe(ctx, pipe.CreateRequest{
		ClientID: "client-a",
		Name:     "restart",
		Desc:     "restart persistence",
	})
	if err != nil {
		first.Close()
		store.Close()
		t.Fatal(err)
	}
	if _, err := clientB.Join(ctx, created.ID, pipe.JoinRequest{ClientID: "client-b", Name: "restart"}); err != nil {
		first.Close()
		store.Close()
		t.Fatal(err)
	}
	receipt, err := clientA.Send(ctx, message.SendRequest{
		PipeID:          created.ID,
		SenderID:        "client-a",
		ReceiverID:      "client-b",
		ClientMessageID: "restart-message-1",
		Payload:         "survives restart",
	})
	if err != nil {
		first.Close()
		store.Close()
		t.Fatal(err)
	}
	first.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second := httptest.NewServer(server.New(reopened, verifier, log.New(io.Discard, "", 0)).Handler())
	defer second.Close()
	clientB = httptransport.New(second.URL, "token-b", "client-b")

	listenCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	body, err := clientB.Listen(listenCtx, created.ID, "client-b", "")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	event, err := readEvent(body)
	body.Close()
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if event.ID != receipt.ID || event.Payload != "survives restart" {
		t.Fatalf("unexpected restarted event: %+v", event)
	}
}

func readEvent(body io.Reader) (message.Message, error) {
	scanner := bufio.NewScanner(body)
	var data string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "" && data != "" {
			var event message.Message
			if err := json.Unmarshal([]byte(data), &event); err != nil {
				return message.Message{}, err
			}
			return event, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return message.Message{}, err
	}
	return message.Message{}, errors.New("SSE stream ended before message")
}
