package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
	"github.com/five-zombie/agent-pipe/internal/storage/sqlite"
)

func TestClaimLeaseRecoveryAndCleanup(t *testing.T) {
	ctx := context.Background()
	dbPath := t.TempDir() + "/pipe.db"
	store, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	created, err := store.CreatePipe(ctx, pipe.CreateRequest{
		ClientID: "sender",
		Name:     "work",
		Desc:     "work pipe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.JoinPipe(ctx, created.ID, pipe.JoinRequest{ClientID: "receiver", Name: "work"}); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CreateMessage(ctx, message.SendRequest{
		PipeID:          created.ID,
		SenderID:        "sender",
		ReceiverID:      "receiver",
		ClientMessageID: "lease-message-1",
		Payload:         "payload",
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := store.ClaimNext(ctx, created.ID, "receiver", "owner-1", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != receipt.ID {
		t.Fatalf("expected first claim %s, got %s", receipt.ID, claimed.ID)
	}
	claimed, err = store.ClaimNext(ctx, created.ID, "receiver", "owner-2", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != "" {
		t.Fatalf("claimed message before lease expiry: %s", claimed.ID)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	store, err = sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	claimed, err = store.ClaimNext(ctx, created.ID, "receiver", "owner-2", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != receipt.ID {
		t.Fatalf("expected recovered claim %s, got %s", receipt.ID, claimed.ID)
	}
	if err := store.MarkSent(ctx, receipt.ID, "owner-2"); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ReplaySent(ctx, created.ID, "receiver", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].ID != receipt.ID || replayed[0].DeliverySeq == 0 {
		t.Fatalf("expected one replayable sent message, got %+v", replayed)
	}
	removed, err := store.Cleanup(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("expected one cleaned message, got %d", removed)
	}
}
