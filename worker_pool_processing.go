package main

import (
	"encoding/csv"
	"log/slog"
	"os"
	"sync"
)

var (
	jobs = make(chan *Job, 100)
	wg   sync.WaitGroup
)

func processCSVWithWorkerPool(path string) {
	file, err := os.Open(path)
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

	numOfWorkers := 5
	createWorkerPool(numOfWorkers)

	go func() {
		for _, record := range records[1:] { // Skipping header row
			if job := parseRecord(record); job == nil {
				continue
			} else {
				jobs <- job
			}
		}
		close(jobs)
	}()
}

func createWorkerPool(numOfWorkers int) {
	wg.Add(numOfWorkers)
	for range numOfWorkers {
		go worker()
	}
	wg.Wait()
}

func worker() {
	for job := range jobs {
		processJob(0, job)
	}
	wg.Done()
}
