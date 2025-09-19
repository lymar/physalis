package log

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

func InitDevLog() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.StampMilli,
		}),
	))

	slog.SetLogLoggerLevel(slog.LevelDebug)
}
