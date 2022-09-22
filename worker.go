package main

import (
	"context"
	"log"
)

type Worker struct {
	queue Queue
}

func NewWorker(queue Queue) *Worker {
	return &Worker{
		queue: queue,
	}
}

func (worker *Worker) Start(ctx context.Context, logger *log.Logger) {
	logger.Print("Starting worker")

	errChan := worker.queue.Consume(ctx, func(ctx context.Context, message string) error {
		logger.Printf("Worker has just processed the message: %s", message)
		return nil
	})

	for {
		select {
		case err := <-errChan:
			logger.Printf("failed to consume message: %s", err)
		case <-ctx.Done():
			logger.Print("stopping worker for context ended")
			return
		}
	}
}
