package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/five-zombie/agent-pipe/internal/auth"
	"github.com/five-zombie/agent-pipe/internal/server"
	"github.com/five-zombie/agent-pipe/internal/storage/sqlite"
)

func main() {
	addr := flag.String("addr", envOr("AIPIPE_ADDR", ":8080"), "HTTP listen address")
	dbPath := flag.String("db", envOr("AIPIPE_DB", "./data/pipe.db"), "SQLite database path")
	authToken := flag.String("auth-token", os.Getenv("AIPIPE_AUTH_TOKEN"), "Bearer token; empty disables auth")
	clientTokens := flag.String("client-tokens", os.Getenv("AIPIPE_CLIENT_TOKENS"), "client_id=token pairs separated by commas")
	clientTokensFile := flag.String("client-tokens-file", os.Getenv("AIPIPE_CLIENT_TOKENS_FILE"), "file containing client_id=token pairs")
	messageRetention := flag.Duration("message-retention", 24*time.Hour, "how long sent messages remain in SQLite")
	cleanupInterval := flag.Duration("cleanup-interval", time.Minute, "sent-message cleanup interval")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := sqlite.Open(context.Background(), *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	tokens, err := auth.ParseClientTokens(*clientTokens)
	if err != nil {
		log.Fatal(err)
	}
	if *clientTokensFile != "" {
		if *clientTokens != "" {
			log.Fatal("set only one of AIPIPE_CLIENT_TOKENS and AIPIPE_CLIENT_TOKENS_FILE")
		}
		tokens, err = auth.ParseClientTokensFile(*clientTokensFile)
		if err != nil {
			log.Fatal(err)
		}
	}
	service := server.New(store, auth.New(tokens, *authToken), log.Default())
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go cleanupLoop(shutdownCtx, store, *cleanupInterval, *messageRetention)
	serverErr := make(chan error, 1)
	go func() {
		log.Printf("pipe-server listening on %s", *addr)
		serverErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	case <-shutdownCtx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown incomplete: %v", err)
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func cleanupLoop(ctx context.Context, store *sqlite.Store, interval, retention time.Duration) {
	if interval <= 0 || retention < 0 {
		log.Printf("message cleanup disabled: interval=%s retention=%s", interval, retention)
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := store.Cleanup(ctx, retention)
			if err != nil {
				log.Printf("message cleanup failed: %v", err)
				continue
			}
			if count > 0 {
				log.Printf("message cleanup removed=%d", count)
			}
		}
	}
}
