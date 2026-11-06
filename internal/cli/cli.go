package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/five-zombie/agent-pipe/internal/config"
	"github.com/five-zombie/agent-pipe/internal/id"
	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
	httptransport "github.com/five-zombie/agent-pipe/internal/transport/http"
)

func Run(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		usage(out)
		return nil
	}
	switch args[0] {
	case "init":
		return runInit(out)
	case "list":
		return runList(out)
	case "create":
		return runCreate(ctx, args[1:], out, errOut)
	case "join":
		return runJoin(ctx, args[1:], out, errOut)
	case "listen":
		return runListen(ctx, args[1:], out, errOut)
	case "send":
		return runSend(ctx, args[1:], out, errOut, "send")
	case "reply":
		return runSend(ctx, args[1:], out, errOut, "reply")
	case "status":
		return runStatus(ctx, args[1:], out, errOut)
	case "help", "-h", "--help":
		usage(out)
		return nil
	default:
		usage(errOut)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runInit(out io.Writer) error {
	path := config.DefaultPath()
	_, created, err := config.Init(path)
	if err != nil {
		return err
	}
	if created {
		_, err = fmt.Fprintf(out, "created %s\n", path)
		return err
	}
	_, err = fmt.Fprintf(out, "skipped %s (already exists)\n", path)
	return err
}

func runList(out io.Writer) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if len(cfg.Pipes) == 0 {
		_, err := fmt.Fprintln(out, "no pipes registered")
		return err
	}
	if _, err := fmt.Fprintln(out, "id\tname\tdecs"); err != nil {
		return err
	}
	for _, pipe := range cfg.Pipes {
		if _, err := fmt.Fprintf(out, "%s\t%s\t%s\n", pipe.ID, pipe.Name, pipe.Desc); err != nil {
			return err
		}
	}
	return nil
}

func runCreate(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := newFlagSet("pipecli create", errOut)
	name := fs.String("name", "", "local pipe name")
	desc := fs.String("description", "", "pipe description")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *desc == "" {
		return errors.New("create requires --name and --description")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	client := newClient(cfg)
	created, err := client.CreatePipe(ctx, pipe.CreateRequest{
		ClientID: cfg.ClientID,
		Name:     *name,
		Desc:     *desc,
	})
	if err != nil {
		return err
	}
	added, err := config.AddPipe(config.DefaultPath(), cfg, config.Pipe{
		ID: created.ID, Name: *name, Desc: created.Desc,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "created %s (%s) registered=%t\n", created.ID, created.Desc, added)
	return err
}

func runJoin(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("join requires <pipe-id>")
	}
	pipeID := args[0]
	fs := newFlagSet("pipecli join", errOut)
	name := fs.String("name", "", "local pipe name")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("join requires --name")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	joined, err := newClient(cfg).Join(ctx, pipeID, pipe.JoinRequest{
		ClientID: cfg.ClientID,
		Name:     *name,
	})
	if err != nil {
		return err
	}
	added, err := config.AddPipe(config.DefaultPath(), cfg, config.Pipe{
		ID: pipeID, Name: *name, Desc: joined.Desc,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "joined %s registered=%t\n", pipeID, added)
	return err
}

func runListen(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("listen requires <pipe-name|pipe-id>")
	}
	target := args[0]
	fs := newFlagSet("pipecli listen", errOut)
	timeout := fs.Duration("t", 30*time.Second, "listen duration")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *timeout <= 0 {
		return errors.New("listen duration must be positive")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	registered, err := config.ResolvePipe(cfg, target)
	if err != nil {
		return err
	}
	listenCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client := newClient(cfg)
	lastEventID := ""
	seen := make(map[string]struct{})
	for listenCtx.Err() == nil {
		body, err := client.Listen(listenCtx, registered.ID, cfg.ClientID, lastEventID)
		if err != nil {
			if listenCtx.Err() != nil {
				return nil
			}
			if err := waitReconnect(listenCtx); err != nil {
				return nil
			}
			continue
		}
		err = printEvents(listenCtx, body, out, seen, &lastEventID)
		body.Close()
		if listenCtx.Err() != nil {
			return nil
		}
		if err != nil {
			if err := waitReconnect(listenCtx); err != nil {
				return nil
			}
		}
	}
	return nil
}

func runSend(ctx context.Context, args []string, out, errOut io.Writer, command string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s requires <pipe-name|pipe-id>", command)
	}
	target := args[0]
	fs := newFlagSet("pipecli "+command, errOut)
	receiver := fs.String("to", "", "receiver client_id")
	payload := fs.String("message", "", "message payload")
	replyTo := fs.String("reply-to", "", "message id being replied to")
	clientMessageID := fs.String("message-id", "", "idempotency key; generated when omitted")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *receiver == "" || *payload == "" {
		return fmt.Errorf("%s requires --to and --message", command)
	}
	if *clientMessageID == "" {
		var err error
		*clientMessageID, err = id.New()
		if err != nil {
			return err
		}
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	registered, err := config.ResolvePipe(cfg, target)
	if err != nil {
		return err
	}
	receipt, err := newClient(cfg).Send(ctx, message.SendRequest{
		PipeID: cfgPipeID(registered), SenderID: cfg.ClientID, ReceiverID: *receiver,
		ClientMessageID: *clientMessageID, Payload: *payload, ReplyTo: *replyTo,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", receipt.ID, receipt.ClientMessageID, receipt.PipeID, receipt.Status)
	return err
}

func runStatus(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("status requires <message-id>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	status, err := newClient(cfg).Status(ctx, args[0], cfg.ClientID)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}

func printEvents(ctx context.Context, body io.Reader, out io.Writer, seen map[string]struct{}, lastEventID *string) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	var data strings.Builder
	eventID := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if data.Len() == 0 {
				continue
			}
			var event message.Message
			if err := json.Unmarshal([]byte(data.String()), &event); err != nil {
				return fmt.Errorf("decode SSE message: %w", err)
			}
			if eventID != "" {
				*lastEventID = eventID
			} else if event.DeliverySeq > 0 {
				*lastEventID = fmt.Sprint(event.DeliverySeq)
			}
			if _, duplicate := seen[event.ID]; !duplicate {
				if err := json.NewEncoder(out).Encode(event); err != nil {
					return err
				}
				seen[event.ID] = struct{}{}
				if len(seen) > 10000 {
					for id := range seen {
						delete(seen, id)
						break
					}
				}
			}
			data.Reset()
			eventID = ""
		case strings.HasPrefix(line, "id:"):
			eventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	return nil
}

func waitReconnect(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func loadConfig() (config.Config, error) {
	return config.Load(config.DefaultPath())
}

func newClient(cfg config.Config) *httptransport.Client {
	return httptransport.New(cfg.Server.URL, cfg.Server.Auth.Token, cfg.ClientID)
}

func cfgPipeID(pipe config.Pipe) string {
	return pipe.ID
}

func newFlagSet(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}

func usage(out io.Writer) {
	_, _ = fmt.Fprintln(out, `pipecli commands:
  init
  list
  create --name <name> --description <desc>
  join <pipe-id> --name <name>
  listen <pipe-name|pipe-id> --t=30s
  send <pipe-name|pipe-id> --to <client-id> --message <payload> [--message-id <id>]
  reply <pipe-name|pipe-id> --to <client-id> --message <payload> --reply-to <message-id> [--message-id <id>]
  status <message-id>`)
}
