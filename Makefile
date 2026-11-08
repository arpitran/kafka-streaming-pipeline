# Shortcuts for the local Kafka broker. Run `make` to list them.
# Use the Confluent broker instead with BROKER=confluent, e.g. `make up BROKER=confluent`.

BROKER ?= apache
N ?= 10
GROUP ?= order-tracker
BOOTSTRAP = --bootstrap-server localhost:9092

# The two images name their CLI tools differently.
ifeq ($(BROKER),confluent)
  kafka = docker exec kafka-confluent $(1)
  PROFILE = --profile confluent
else
  kafka = docker exec kafka /opt/kafka/bin/$(1).sh
  PROFILE = --profile apache
endif

.PHONY: help up stop down clean wait topics list group quorum logs produce consume

help: ## List the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-9s %s\n", $$1, $$2}'

up: ## Start the broker and wait until it is ready
	docker compose $(PROFILE) up -d
	@$(MAKE) --no-print-directory wait

wait:
	@echo "waiting for Kafka..."
	@until $(call kafka,kafka-broker-api-versions) $(BOOTSTRAP) >/dev/null 2>&1; do sleep 1; done
	@echo "Kafka is ready"

stop: ## Stop the broker (keeps data)
	docker compose stop

down: ## Remove the container (keeps data in the volume)
	docker compose $(PROFILE) down

clean: ## Remove the container AND delete its data volume
	docker compose $(PROFILE) down -v

topics: ## Create the orders and trips topics (3 partitions)
	$(call kafka,kafka-topics) $(BOOTSTRAP) --create --if-not-exists --topic orders --partitions 3
	$(call kafka,kafka-topics) $(BOOTSTRAP) --create --if-not-exists --topic trips --partitions 3

list: ## List topics
	@$(call kafka,kafka-topics) $(BOOTSTRAP) --list

group: ## Show partitions, offsets and lag for a consumer group (GROUP=order-tracker)
	@$(call kafka,kafka-consumer-groups) $(BOOTSTRAP) --describe --group $(GROUP)

quorum: ## Show the KRaft controller quorum
	@$(call kafka,kafka-metadata-quorum) $(BOOTSTRAP) describe --status

logs: ## Follow the broker logs
	docker compose $(PROFILE) logs -f

produce: ## Send N fake orders (N=10)
	uv run apps/order_producer.py $(N)

consume: ## Read orders as group order-tracker (Ctrl-C to stop)
	uv run apps/order_consumer.py
