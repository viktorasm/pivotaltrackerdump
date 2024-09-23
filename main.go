package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"pivotaltrackerexport/clickup"
	"pivotaltrackerexport/tracker"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

const DEBUG = false

func main() {
	outDir := "out"
	if DEBUG {
		outDir = "debug_out"
	}
	err := os.MkdirAll(outDir, 0755)
	if err != nil {
		logger.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handleInterruptSignals(func() {
		cancel()
		logger.Println("cancel requested, stopping...")
	})

	trackerProject := os.Getenv("TRACKER_PROJECT")
	trackerToken := os.Getenv("TRACKER_TOKEN")
	clickupToken := os.Getenv("CLICKUP_TOKEN")
	if trackerProject != "" && trackerToken != "" {
		logger.Println("TRACKER_PROJECT and TRACKER_TOKEN detected, doing tracker export")
		tracker.Export(ctx, trackerToken, trackerProject, outDir)
	}

	if clickupToken != "" {
		logger.Println("CLICKUP_TOKEN detected, doing clickup import")
		clickup.Import(ctx, clickupToken)
	}

	logger.Println("done")
}

func handleInterruptSignals(done func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		_ = <-sigs
		done()
	}()
}
