package main

import (
	"log/slog"
	"time"
)

func main() {
	// TODO: Generate a csv file with 1M records (parallelize this as well?)
	// each record should have an ID, Name, Email, Age
	// Save the file as data.csv

	const howMany = 1_000
	const parallel = false
	const path = "/tmp/testdata/data.csv"

	slog.Info("Starting CSV creation...")
	t0 := time.Now()
	if err := createCSVRecords(path, howMany, parallel); err != nil {
		return
	}
	slog.Info(
		"CSV creation completed",
		slog.Float64("duration", time.Since(t0).Seconds()),
	)

	// Process that file sequentially
	slog.Info("Starting processing CSV sequentially")
	t0 = time.Now()
	processCSVFileSequentially(path)
	slog.Info(
		"Sequential processing completed",
		slog.Float64("duration", time.Since(t0).Seconds()),
	)

	// TODO: Process that file using workers
	slog.Info("Starting processing CSV with worker pool")
	t0 = time.Now()
	processCSVWithWorkerPool(path)
	slog.Info(
		"Worker pool processing completed",
		slog.Float64("duration", time.Since(t0).Seconds()),
	)
}
