package main

import (
	"log/slog"
	"strconv"
)

func parseRecord(record []string) *Job {
	age, err := strconv.Atoi(record[3])
	if err != nil {
		slog.Error("Error converting age", slog.String("err", err.Error()))
		return nil
	}

	job := Job{
		ID:    record[0],
		Name:  record[1],
		Email: record[2],
		Age:   age,
	}

	return &job
}
