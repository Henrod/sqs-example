package main

import (
	"context"
	"flag"
	"fmt"
	"log"
)

func main() {
	serverType := flag.String("type", "worker", "server type: [api, worker]")
	flag.Parse()

	logger := log.Default()
	ctx := context.Background()

	queue, err := NewSQSQueue(ctx, "henrod")
	if err != nil {
		logger.Fatal(err)
	}

	switch *serverType {
	case "api":
		api := NewAPI(queue)
		err = api.Start(logger)

	case "worker":
		worker := NewWorker(queue)
		worker.Start(ctx, logger)

	default:
		err = fmt.Errorf("invalid server type: %s", *serverType)
	}

	if err != nil {
		logger.Fatal(err)
	}
}
