#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT="5006"
DEFAULT_SESSION_TOKEN_EXPIRATION="86400"

read_env_value() {
  local key="$1"
  local file="$2"

  awk -F= -v key="$key" '
    $0 ~ "^[[:space:]]*" key "=" {
      value = substr($0, index($0, "=") + 1)
      sub(/\r$/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      if (value ~ /^".*"$/ || value ~ /^'\''.*'\''$/) {
        value = substr(value, 2, length(value) - 2)
      }
      print value
    }
  ' "$file" | tail -n 1
}

cd "$ROOT_DIR"

if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  echo "Virtual environment not found: $VENV_DIR" >&2
  echo 'Create it first: python3.13 -m venv .venv && .venv/bin/pip install -e ".[dev,optimizer]"' >&2
  exit 1
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

ENV_HOST=""
ENV_PORT=""
ENV_SESSION_TOKEN_EXPIRATION=""
if [[ -f "$ROOT_DIR/.env" ]]; then
  ENV_HOST="$(read_env_value "MT_SERVICE_BOKEH_HOST" "$ROOT_DIR/.env")"
  ENV_PORT="$(read_env_value "MT_SERVICE_BOKEH_PORT" "$ROOT_DIR/.env")"
  ENV_SESSION_TOKEN_EXPIRATION="$(read_env_value "MT_SERVICE_BOKEH_SESSION_TOKEN_EXPIRATION" "$ROOT_DIR/.env")"
fi

BOKEH_HOST="${MT_SERVICE_BOKEH_HOST:-${ENV_HOST:-$DEFAULT_HOST}}"
BOKEH_PORT="${MT_SERVICE_BOKEH_PORT:-${ENV_PORT:-$DEFAULT_PORT}}"
SESSION_TOKEN_EXPIRATION="${MT_SERVICE_BOKEH_SESSION_TOKEN_EXPIRATION:-${ENV_SESSION_TOKEN_EXPIRATION:-$DEFAULT_SESSION_TOKEN_EXPIRATION}}"

fuser -k "${BOKEH_PORT}/tcp" >/dev/null 2>&1 || true

echo "Starting Bokeh UI: http://${BOKEH_HOST}:${BOKEH_PORT}/bokeh_app"

exec python -m bokeh serve src/bokeh_app \
  --address "$BOKEH_HOST" \
  --port "$BOKEH_PORT" \
  --session-token-expiration "$SESSION_TOKEN_EXPIRATION" \
  --allow-websocket-origin "${BOKEH_HOST}:${BOKEH_PORT}" \
  --allow-websocket-origin "127.0.0.1:${BOKEH_PORT}" \
  --allow-websocket-origin "localhost:${BOKEH_PORT}"
