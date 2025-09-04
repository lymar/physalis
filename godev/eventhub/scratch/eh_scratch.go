//go:build scratch

package main

import (
	"log/slog"

	"github.com/lymar/physalis/internal/log"
)

// go run -tags=scratch ./eventhub/scratch

func main() {
	log.InitDevLog()

	slog.Debug("Info message")
	slog.Info("Info message")
	slog.Warn("Info message")
	slog.Error("Info message")
}
