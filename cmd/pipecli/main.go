package main

import (
	"context"
	"fmt"
	"os"

	"github.com/five-zombie/agent-pipe/internal/cli"
)

func main() {
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "pipecli:", err)
		os.Exit(1)
	}
}
