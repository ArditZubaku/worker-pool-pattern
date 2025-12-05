package main

import (
	"fmt"
	"time"
)

type Job struct {
	ID    string
	Name  string
	Email string
	Age   int
}

func processJob(id int, _ *Job) {
	fmt.Printf("Processing job with ID: %d\n", id)
	time.Sleep(10 * time.Millisecond)
}
