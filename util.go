package main

import (
	"io"
	"log/slog"
)

func closeOrLog(closer io.Closer, description string) {
	if err := closer.Close(); err != nil {
		slog.Error(
			"Failed",
			slog.String("to", description),
			slog.String("err", err.Error()),
		)
	}
}
