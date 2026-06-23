#!/bin/bash
set -e

cd /app

# Create required directories
mkdir -p .tmp/spark-events .tmp/local_iceberg_warehouse .tmp/local_delta_warehouse \
         .tmp/checkpoint_user_events .tmp/checkpoint_sales_enriched \
         .tmp/metastore .tmp/spark-warehouse app/data/streaming_input logs

# Pre-create Iceberg namespaces as directories (Hadoop catalog = filesystem).
# Iceberg doesn't auto-create 'default', so Thrift clients would get SCHEMA_NOT_FOUND.
mkdir -p .tmp/local_iceberg_warehouse/{default,analytics,staging,marts,seeds}

MODE="${1:-connect}"

case "$MODE" in
  connect)
    echo "=== Starting Spark Unified Server (Thrift + Connect) ==="
    echo "  Thrift JDBC : jdbc:hive2://localhost:${SPARK_THRIFT_PORT:-10000}"
    echo "  Connect gRPC: sc://localhost:${SPARK_CONNECT_PORT:-15002}"
    echo "  Spark UI    : http://localhost:4040"
    echo "  Profile     : driver.memory=${SPARK_DRIVER_MEMORY:-2g}, master=local[${SPARK_CORES:-*}]"
    echo ""
    echo "Both dbt (via Thrift) and notebooks (via Connect) share the same"
    echo "SparkContext — all jobs appear in a single Spark UI."

    # Start Thrift Server (HiveServer2) as the primary process.
    # Spark Connect is enabled via spark.plugins in spark-defaults.conf,
    # so it starts automatically in the same JVM.
    # --master and driver.memory come from the resource profile, set by the
    # SPARK_CORES / SPARK_DRIVER_MEMORY env vars (make up vs make up-constrained).
    $SPARK_HOME/sbin/start-thriftserver.sh \
      --master "local[${SPARK_CORES:-*}]" \
      --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-2g}" \
      --conf "spark.connect.grpc.binding.port=${SPARK_CONNECT_PORT:-15002}" \
      --conf "spark.hive.server2.thrift.port=${SPARK_THRIFT_PORT:-10000}" 2>&1

    sleep 5

    PID_FILE=$(ls /tmp/spark-*.pid 2>/dev/null | head -1)

    if [ -z "$PID_FILE" ]; then
      echo "ERROR: Spark Unified Server failed to start. Logs:"
      cat $SPARK_HOME/logs/*.out 2>/dev/null || true
      exit 1
    fi

    PID=$(cat "$PID_FILE")
    echo "Spark Unified Server running (PID: $PID)"

    # Forward logs to stdout
    tail -F $SPARK_HOME/logs/*.out 2>/dev/null &

    # Wait for process — exit if it dies (Docker will restart)
    while kill -0 "$PID" 2>/dev/null; do
      sleep 5
    done

    echo "Spark Unified Server stopped unexpectedly"
    exit 1
    ;;

  history)
    echo "=== Starting Spark History Server ==="
    echo "  UI: http://localhost:18080"

    $SPARK_HOME/sbin/start-history-server.sh 2>&1

    sleep 3

    PID_FILE=$(ls /tmp/spark-*.pid 2>/dev/null | head -1)

    if [ -z "$PID_FILE" ]; then
      echo "ERROR: History Server failed to start. Logs:"
      cat $SPARK_HOME/logs/*.out 2>/dev/null || true
      exit 1
    fi

    PID=$(cat "$PID_FILE")
    echo "History Server running (PID: $PID)"

    tail -F $SPARK_HOME/logs/*.out 2>/dev/null &

    while kill -0 "$PID" 2>/dev/null; do
      sleep 5
    done

    echo "History Server stopped unexpectedly"
    exit 1
    ;;

  *)
    exec "$@"
    ;;
esac
