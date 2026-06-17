# Local dev for v1 ingest. See VERIFY.md for the end-to-end runbook.
# Infra (Redpanda + Postgres) comes up with `make up`; the ingest service is gated
# behind the "ingest" compose profile and started separately, so it can be killed and
# restarted for the crash-resume test without disturbing infra (the cursor must survive).

COMPOSE := docker compose -f docker-compose.dev.yml

.DEFAULT_GOAL := help
.PHONY: help up down logs run-ingest cursor inject-dlq ch ch-count grafana

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start infra only (Redpanda + topics + Postgres + ClickHouse), detached
	$(COMPOSE) up -d redpanda redpanda-init postgres clickhouse

down: ## Stop everything and remove volumes (clean slate; resets the cursor)
	$(COMPOSE) down -v

logs: ## Tail the ingest service logs (JSON)
	$(COMPOSE) logs -f ingest

run-ingest: ## Run ingest in the FOREGROUND (Ctrl-C = graceful stop; rebuilds image)
	$(COMPOSE) --profile ingest up --build ingest

cursor: ## Print the persisted cursor row(s) from Postgres
	$(COMPOSE) exec -T postgres psql -U bsky -d bsky_ingest -c \
		"SELECT stream_name, cursor, updated_at FROM ingest_cursors ORDER BY stream_name;"

inject-dlq: ## Inject one malformed event through validate->produce_dlq into bsky.dlq.v1
	$(COMPOSE) --profile ingest run --rm --no-deps ingest uv run --no-dev python -m ingest.dev_inject_dlq

ch: ## Open an interactive clickhouse-client shell (DB bsky)
	$(COMPOSE) exec clickhouse clickhouse-client --database bsky

ch-count: ## Show raw vs deduped post counts (proves ReplacingMergeTree collapses replay dups)
	$(COMPOSE) exec -T clickhouse clickhouse-client --query \
		"SELECT count() AS raw FROM bsky.posts; SELECT count() AS deduped FROM bsky.posts FINAL;"

grafana: ## Start Grafana (profile-gated) and print the URL
	$(COMPOSE) --profile grafana up -d grafana
	@echo "Grafana: http://localhost:3000  (dashboard: Bluesky posts (v1))"
