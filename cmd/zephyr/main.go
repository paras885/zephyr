package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/zephyr-workflow/zephyr/pkg/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "zephyr:", err)
		os.Exit(1)
	}
}
