.PHONY: test run up down
test:
	go test ./...
run:
	go run ./cmd/server
up:
	docker compose up -d --build
down:
	docker compose down
