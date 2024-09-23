package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	projectID := os.Getenv("TRACKER_PROJECT")
	trackerToken := os.Getenv("TRACKER_TOKEN")
	if projectID == "" || trackerToken == "" {
		logger.Fatalf("supply TRACKER_PROJECT and TRACKER_TOKEN as env variables first")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handleInterruptSignals(func() {
		cancel()
		logger.Println("cancel requested, stopping...")
	})

	tracker.Export(ctx, trackerToken, projectID, outDir)

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
