.PHONY: help up up-constrained _ports down restart restart-constrained logs jupyter jupyter-stop \
       clean clean-all status build cdc-up cdc-down cdc-logs \
       monitoring-up monitoring-down monitoring-logs \
       dbt-build dbt-debug transpile-check airflow-up airflow-down airflow-logs airflow-clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker ──────────────────────────────────────────────────────────────────

build: ## Build Docker images
	docker compose build

up: ## Start all Docker services — TUNED profile (~3 GB Spark; the default)
	@mkdir -p .tmp/spark-events .tmp/local_iceberg_warehouse .tmp/local_delta_warehouse logs
	docker compose --env-file .env --env-file conf/profiles/tuned.env up -d
	@echo ""
	@echo "  Spark profile : TUNED       (conf/profiles/tuned.env)"
	@$(MAKE) --no-print-directory _ports

up-constrained: ## Start all Docker services — CONSTRAINED profile (~2 GB Spark, 2 cores; for OOM/spill modules)
	@mkdir -p .tmp/spark-events .tmp/local_iceberg_warehouse .tmp/local_delta_warehouse logs
	docker compose --env-file .env --env-file conf/profiles/constrained.env up -d
	@echo ""
	@echo "  Spark profile : CONSTRAINED (conf/profiles/constrained.env)"
	@echo "  Use this profile for the OOM / spill modules so failure is real but the host stays usable."
	@$(MAKE) --no-print-directory _ports

_ports: ## (internal) print service URLs
	@echo "  Spark Connect : sc://localhost:$${SPARK_CONNECT_PORT:-15002}"
	@echo "  Spark UI      : http://localhost:$${SPARK_UI_PORT:-4040}"
	@echo "  History Server: http://localhost:$${SPARK_HISTORY_PORT:-18080}"
	@echo "  Kafka UI      : http://localhost:$${KAFKA_UI_PORT:-8080}"
	@echo ""
	@echo "Run 'make jupyter' to start JupyterLab locally."

down: ## Stop all Docker services
	docker compose down

restart: down up ## Restart all services (tuned profile)

restart-constrained: down up-constrained ## Restart all services (constrained profile)

cdc-up: ## Start the CDC track services (Postgres + Kafka Connect/Debezium) — Phase 4, opt-in
	@mkdir -p .tmp/spark-events .tmp/local_iceberg_warehouse logs
	docker compose --env-file .env --env-file conf/profiles/tuned.env --profile cdc up -d
	@echo ""
	@echo "  Postgres      : localhost:$${POSTGRES_PORT:-5432}  (user/pass cdc/cdc, db inventory)"
	@echo "  Kafka Connect : http://localhost:$${CONNECT_PORT:-8083}  (Debezium REST API)"
	@echo ""
	@echo "  CDC adds Postgres + Kafka Connect (~1.3 GB). On an 8 GB laptop, stop optional"
	@echo "  services first:  docker compose stop spark-history kafka-ui"

cdc-down: ## Stop ONLY the CDC services (leaves the base stack running)
	docker compose stop postgres kafka-connect
	docker compose rm -f postgres kafka-connect

cdc-logs: ## Tail Kafka Connect logs
	docker compose logs -f kafka-connect

monitoring-up: ## Start the observability stack (Prometheus + Grafana + exporters) — CAP-3, opt-in
	docker compose --env-file .env --env-file conf/profiles/tuned.env --profile monitoring up -d
	@echo ""
	@echo "  Prometheus : http://localhost:$${PROMETHEUS_PORT:-9090}  (Status → Targets)"
	@echo "  Grafana    : http://localhost:$${GRAFANA_PORT:-3000}  (admin/admin; Prometheus datasource pre-provisioned)"
	@echo "  Exporters  : kafka :9308 · postgres :9187 (postgres-exporter needs 'make cdc-up')"
	@echo ""
	@echo "  Import Grafana dashboards by ID: Kafka 7589 · Postgres 9628 · Spark (search 'spark')."
	@echo "  For Spark metrics, ensure the server was (re)started after enabling spark.ui.prometheus.enabled."

monitoring-down: ## Stop ONLY the observability services
	docker compose stop prometheus grafana kafka-exporter postgres-exporter
	docker compose rm -f prometheus grafana kafka-exporter postgres-exporter

monitoring-logs: ## Tail Prometheus + Grafana logs
	docker compose logs -f prometheus grafana

logs: ## Tail Docker service logs
	docker compose logs -f

status: ## Show status of all services
	docker compose ps

# ── Local Development ───────────────────────────────────────────────────────

jupyter: jupyter-stop ## Start JupyterLab locally (kills existing session first)
	@set -a && [ -f .env ] && . ./.env && set +a && \
	PYTHONPATH="$(PWD)" \
	uv run jupyter lab \
		--ip=127.0.0.1 \
		--port=$${JUPYTER_PORT:-8888} \
		--no-browser \
		--ServerApp.token="$${JUPYTER_TOKEN:-}" \
		--ServerApp.password=""

jupyter-stop: ## Stop any running JupyterLab session
	@-pkill -f "jupyter-lab" 2>/dev/null && echo "Stopped existing JupyterLab." || true

# ── dbt (convenience — or: cd dbt && source ../.env && dbt <cmd>) ───────────

dbt-build: ## Full dbt pipeline: seed + run + test
	@set -a && [ -f .env ] && . ./.env && set +a && \
	cd dbt && uv run dbt build --profiles-dir .

dbt-debug: ## Verify dbt connection to Spark Thrift Server
	@set -a && [ -f .env ] && . ./.env && set +a && \
	cd dbt && uv run dbt debug --profiles-dir .

transpile-check: ## Validate every (transpiled) model on Spark with zero data — native dbt build --empty
	@set -a && [ -f dbt/.env ] && . ./dbt/.env && set +a && \
	cd dbt && uv run dbt build --empty

# ── Airflow ─────────────────────────────────────────────────────────────────

AIRFLOW_HOME := $(PWD)/airflow/.airflow_home
AIRFLOW_LOG  := $(AIRFLOW_HOME)/logs/standalone.log

airflow-up: airflow-down ## Start Airflow locally (standalone, UI at :5000)
	@mkdir -p $(AIRFLOW_HOME)/logs
	@echo ""
	@echo "Airflow starting in background..."
	@echo "  Web UI : http://localhost:$${AIRFLOW_PORT:-5000}"
	@echo "  Login  : airflow / airflow"
	@echo "  DAGs   : ./airflow/dags/"
	@echo "  Logs   : make airflow-logs"
	@echo ""
	@AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/airflow/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=false \
	AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$${AIRFLOW_PORT:-5000} \
	AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=airflow:admin \
	AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=$(PWD)/airflow/passwords.json \
	bash -c 'cd $(PWD)/airflow && nohup uv run airflow standalone >> $(AIRFLOW_LOG) 2>&1 &'

airflow-logs: ## Tail Airflow standalone logs
	@tail -f $(AIRFLOW_LOG)

airflow-down: ## Stop Airflow (kills all Airflow processes)
	@-pkill -f "$(PWD)/airflow/.venv/bin/airflow" 2>/dev/null && echo "Stopped Airflow." || echo "Airflow is not running."

airflow-clean: airflow-down ## Remove Airflow runtime state (DB, logs)
	rm -rf $(AIRFLOW_HOME)
	@echo "Cleaned Airflow state. Next 'make airflow-up' will reinitialize."

# ── Maintenance ─────────────────────────────────────────────────────────────

clean: ## Remove generated data (warehouses, checkpoints, events)
	rm -rf .tmp app/data/streaming_input/*.json
	@echo "Cleaned generated data. Run notebooks to regenerate."

clean-all: clean ## Remove all generated data + Docker volumes
	docker compose down -v
	@echo "Cleaned Docker volumes too."
