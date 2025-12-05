package main

import (
	"bufio"
	"encoding/csv"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

func createCSVRecords(howMany int, parallel bool) error {
	const path = "/tmp/testdata"
	if err := os.RemoveAll(path); err != nil {
		slog.Error("Error removing testdata directory", slog.String("err", err.Error()))
		return err
	}

	const filePath = path + "/data.csv"
	// Create testdata directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		slog.Error("Error creating testdata directory", slog.String("err", err.Error()))
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating file", slog.String("err", err.Error()))
		return err
	}
	defer closeOrLog(file, "file creation")

	// Add buffered writer for better I/O performance
	bufferedWriter := bufio.NewWriter(file)
	defer bufferedWriter.Flush()

	writer := csv.NewWriter(bufferedWriter)

	// Write header
	if err := writer.Write([]string{"ID", "Name", "Email", "Age"}); err != nil {
		slog.Error("Error writing header", slog.String("err", err.Error()))
		return err
	}

	if parallel {
		return createRecordsParallel(howMany, writer)
	} else {
		return createRecordsSequential(howMany, writer)
	}
}

func createRecordsSequential(n int, writer *csv.Writer) error {
	// Pre-allocate slice to avoid repeated allocations
	record := make([]string, 4)

	// Pre-compute age strings to avoid repeated conversions
	ageStrings := make([]string, 30)
	for i := range 30 {
		ageStrings[i] = strconv.Itoa(20 + i)
	}

	// Use string builder for more efficient string construction
	var nameBuilder, emailBuilder strings.Builder
	nameBuilder.Grow(20)  // Pre-allocate reasonable capacity
	emailBuilder.Grow(30) // Pre-allocate reasonable capacity

	// Write 1M records with periodic flushing
	for i := 1; i <= n; i++ {
		// Reuse the same slice, just update values
		record[0] = strconv.Itoa(i)

		// Efficient string building for name
		nameBuilder.Reset()
		nameBuilder.WriteString("Name")
		nameBuilder.WriteString(strconv.Itoa(i))
		record[1] = nameBuilder.String()

		// Efficient string building for email
		emailBuilder.Reset()
		emailBuilder.WriteString("user")
		emailBuilder.WriteString(strconv.Itoa(i))
		emailBuilder.WriteString("@example.com")
		record[2] = emailBuilder.String()

		record[3] = ageStrings[i%30] // Use pre-computed age string

		if err := writer.Write(record); err != nil {
			slog.Error("Error writing record", slog.String("err", err.Error()))
			return err
		}

		// Flush every 10,000 records to manage memory usage
		if i%10000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				slog.Error("Error flushing writer", slog.String("err", err.Error()))
				return err
			}
		}
	}

	// Final flush
	writer.Flush()
	if err := writer.Error(); err != nil {
		slog.Error("Error flushing writer", slog.String("err", err.Error()))
		return err
	}
	return nil
}

func createRecordsParallel(n int, writer *csv.Writer) error {
	const chunkSize = 10_000
	numWorkers := runtime.NumCPU()

	// Pre-compute age strings once
	ageStrings := make([]string, 30)
	for i := range 30 {
		ageStrings[i] = strconv.Itoa(20 + i)
	}

	// Channel for work chunks
	workCh := make(chan WorkChunk, numWorkers*2)
	resultCh := make(chan ChunkResult, numWorkers*2)

	// Start workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go generateRecords(workCh, resultCh, ageStrings, &wg)
	}

	// Send work chunks
	go func() {
		defer close(workCh)
		chunkID := 0
		for start := 1; start <= n; start += chunkSize {
			end := min(start+chunkSize-1, n)
			workCh <- WorkChunk{start: start, end: end, id: chunkID}
			chunkID++
		}
	}()

	// Close result channel when all workers done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results in order
	totalChunks := (n + chunkSize - 1) / chunkSize
	results := make([][][]string, totalChunks)

	for result := range resultCh {
		results[result.chunkID] = result.records
	}

	// Write results in order
	for chunkID, records := range results {
		for _, record := range records {
			if err := writer.Write(record); err != nil {
				slog.Error("Error writing record", slog.String("err", err.Error()))
				return err
			}
		}

		// Periodic flush
		if chunkID%10 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				slog.Error("Error flushing writer", slog.String("err", err.Error()))
				return err
			}
		}
	}

	// Final flush
	writer.Flush()
	return writer.Error()
}

type WorkChunk struct {
	start, end, id int
}

type ChunkResult struct {
	chunkID int
	records [][]string
}

func generateRecords(
	workCh <-chan WorkChunk,
	resultCh chan<- ChunkResult,
	ageStrings []string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Local string builders for this worker
	var nameBuilder, emailBuilder strings.Builder
	nameBuilder.Grow(20)
	emailBuilder.Grow(30)

	for chunk := range workCh {
		records := make([][]string, 0, chunk.end-chunk.start+1)

		for i := chunk.start; i <= chunk.end; i++ {
			// Generate record efficiently
			nameBuilder.Reset()
			nameBuilder.WriteString("Name")
			nameBuilder.WriteString(strconv.Itoa(i))

			emailBuilder.Reset()
			emailBuilder.WriteString("user")
			emailBuilder.WriteString(strconv.Itoa(i))
			emailBuilder.WriteString("@example.com")

			record := []string{
				strconv.Itoa(i),
				nameBuilder.String(),
				emailBuilder.String(),
				ageStrings[i%30],
			}

			records = append(records, record)
		}

		resultCh <- ChunkResult{
			chunkID: chunk.id,
			records: records,
		}
	}
}
