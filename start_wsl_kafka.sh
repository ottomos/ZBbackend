#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose-multi-kafka.yml"
SESSION="kafka-multi"
PRODUCER_SCRIPT="$ROOT_DIR/KafkaProducerMultiBroker.py"
KAFKA_API_SCRIPT="$ROOT_DIR/kafka-api.js"

echo "Using ROOT_DIR: $ROOT_DIR"

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required but not installed. Install it and re-run." >&2
  exit 1
fi

for dependency in docker python3 node; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "$dependency is required but not found in PATH." >&2
    exit 1
  fi
done

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "Docker Compose is required but not available." >&2
  exit 1
fi

cd "$ROOT_DIR"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "Stopping existing tmux session '$SESSION'"
  tmux kill-session -t "$SESSION"
fi

echo "Bringing down existing compose stack (if any)..."
"${COMPOSE[@]}" -f "$COMPOSE_FILE" down

echo "Bringing up compose stack..."
"${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d

echo "Waiting 30 seconds for services to initialize..."
sleep 30

echo "Starting tmux session '$SESSION' with producer..."
printf -v producer_command 'exec python3 %q' "$PRODUCER_SCRIPT"
tmux new-session -d -s "$SESSION" -n producer -c "$ROOT_DIR" "$producer_command"

echo "Waiting 15 seconds before starting Kafka API..."
sleep 15

echo "Creating kafka-api window..."
printf -v kafka_command 'exec node %q' "$KAFKA_API_SCRIPT"
tmux new-window -t "$SESSION" -n kafka -c "$ROOT_DIR" "$kafka_command"

echo "All processes started in tmux session '$SESSION'. Attach with: tmux attach -t $SESSION"
