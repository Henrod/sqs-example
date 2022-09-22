package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type API struct {
	queue   Queue
	address string
}

type Job struct {
	Message string
}

func NewAPI(queue Queue) *API {
	return &API{queue: queue, address: "0.0.0.0:8080"}
}

func (api *API) CreateJob(w http.ResponseWriter, req *http.Request) {
	logger := log.Default()

	defer func() { _ = req.Body.Close() }()

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		logger.Printf("failed to read all: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	job := new(Job)
	err = json.Unmarshal(bodyBytes, job)
	if err != nil {
		logger.Printf("failed to unmarshal: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = api.queue.Produce(req.Context(), job.Message)
	if err != nil {
		logger.Printf("failed to produce: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (api *API) Start(logger *log.Logger) error {
	http.Handle("/jobs", otelhttp.NewHandler(http.HandlerFunc(api.CreateJob), "CreateJob"))

	logger.Printf("Starting HTTP API server: %s", api.address)
	err := http.ListenAndServe(api.address, nil)
	if err != nil {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}

	return nil
}
