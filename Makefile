.PHONY: api
api:
	go run ./... --type=api

.PHONY: worker
worker:
	go run ./... --type=worker

.PHONY: deps
deps:
	docker compose up -d

.PHONY: setup
setup:
	aws --endpoint-url=http://localhost:44566 sqs create-queue --queue-name henrod