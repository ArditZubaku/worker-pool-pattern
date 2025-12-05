package main

import (
	"encoding/csv"
	"log/slog"
	"os"
)

func processCSVFileSequentially() {
	file, err := os.Open("testdata/data.csv")
	if err != nil {
		slog.Error("Error opening file", slog.String("err", err.Error()))
		return
	}
	defer closeOrLog(file, "")

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		slog.Error("Error reading CSV", slog.String("err", err.Error()))
	}

	// ID,Name,Email,Age
	for i, record := range records[1:] { // Skipping header row
		if job := parseRecord(record); job == nil {
			continue
		} else {
			processJob(i, job)
		}
	}
}
