.PHONY: all build run clean kafka-up kafka-down kafka-reset kafka-logs kafka-status kafka-zk-up kafka-zk-down tidy fmt vet test proto help

# Переменные
BINARY_NAME=bin/kafkacheck
GO=go
DOCKER_COMPOSE=docker compose

# Сборка
all: build

build:
	$(GO) build -o $(BINARY_NAME) ./cmd/server

run: build
	./$(BINARY_NAME)

clean:
	rm -rf bin
	$(GO) clean

# Kafka (KRaft режим - рекомендуется)
kafka-up:
	$(DOCKER_COMPOSE) up -d
	@echo "Kafka starting... UI will be available at http://localhost:8080"
	@echo "Wait ~30 seconds for brokers to initialize"

kafka-down:
	$(DOCKER_COMPOSE) down

kafka-reset:
	$(DOCKER_COMPOSE) down -v
	@echo "All Kafka data removed"

kafka-logs:
	$(DOCKER_COMPOSE) logs -f kafka1

kafka-status:
	$(DOCKER_COMPOSE) ps

# Kafka (Zookeeper режим - legacy)
kafka-zk-up:
	$(DOCKER_COMPOSE) -f docker-compose.zookeeper.yml up -d
	@echo "Kafka (Zookeeper mode) starting..."

kafka-zk-down:
	$(DOCKER_COMPOSE) -f docker-compose.zookeeper.yml down

# Разработка
tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test -race ./...

# Requires protoc and protoc-gen-go (go install google.golang.org/protobuf/cmd/protoc-gen-go@latest).
proto:
	protoc --go_out=. --go_opt=paths=source_relative pkg/api/message.proto

# Помощь
help:
	@echo "Available targets:"
	@echo "  build        - Build the application"
	@echo "  run          - Build and run"
	@echo "  clean        - Remove binaries"
	@echo ""
	@echo "  kafka-up     - Start Kafka (KRaft mode)"
	@echo "  kafka-down   - Stop Kafka"
	@echo "  kafka-reset  - Stop and remove all data"
	@echo "  kafka-logs   - View Kafka logs"
	@echo "  kafka-status - Check container status"
	@echo ""
	@echo "  kafka-zk-up  - Start Kafka (Zookeeper mode)"
	@echo "  kafka-zk-down- Stop Kafka (Zookeeper mode)"
	@echo ""
	@echo "  tidy         - Run go mod tidy"
	@echo "  fmt          - Format code"
	@echo "  vet          - Run go vet"
	@echo "  test         - Run tests"
	@echo "  proto        - Regenerate pkg/api from message.proto"
