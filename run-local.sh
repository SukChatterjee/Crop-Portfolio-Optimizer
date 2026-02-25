#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

BACKEND_PID_FILE="$RUN_DIR/backend.pid"
FRONTEND_PID_FILE="$RUN_DIR/frontend.pid"
BACKEND_LOG="$RUN_DIR/backend.log"
FRONTEND_LOG="$RUN_DIR/frontend.log"

NODE_BIN="$ROOT_DIR/.tools/node-v20.19.1-darwin-x64/bin"

is_running() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null
}

port_pid() {
  local port="$1"
  lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
}

start_backend() {
  if [[ -f "$BACKEND_PID_FILE" ]] && is_running "$(cat "$BACKEND_PID_FILE")"; then
    echo "Backend already running (PID $(cat "$BACKEND_PID_FILE"))."
    return
  fi

  echo "Starting backend on http://127.0.0.1:8000 ..."
<<<<<<< Updated upstream
  nohup bash -lc "cd '$ROOT_DIR/backend' && source .venv/bin/activate && uvicorn server:app --host 127.0.0.1 --port 8000" >"$BACKEND_LOG" 2>&1 &
=======
  # ensure we use the venv located at project root (not inside backend/)
  local venv_path="$ROOT_DIR/.venv/bin/activate"
  if [[ ! -f "$venv_path" ]]; then
    echo "Virtual environment not found at $venv_path. Create one with python3 -m venv .venv" >&2
    exit 1
  fi
  # if no Mongo URL specified, fall back to in-memory database for local dev
  local mongo_env=""
  if [[ -z "$(printenv MONGO_URL || true)" ]]; then
      echo "No MONGO_URL set, falling back to in-memory DB (USE_INMEMORY_DB=1)" >>"$BACKEND_LOG"
      mongo_env="export USE_INMEMORY_DB=1 && "
  fi
  nohup bash -lc "cd '$ROOT_DIR/backend' && source '$venv_path' && $mongo_env uvicorn server:app --host 127.0.0.1 --port 8000" >"$BACKEND_LOG" 2>&1 &
>>>>>>> Stashed changes
  echo $! >"$BACKEND_PID_FILE"
}

start_frontend() {
  if [[ -f "$FRONTEND_PID_FILE" ]] && is_running "$(cat "$FRONTEND_PID_FILE")"; then
    echo "Frontend already running (PID $(cat "$FRONTEND_PID_FILE"))."
    return
  fi

  local node_path=""
  if [[ -x "$NODE_BIN/node" ]] && [[ -x "$NODE_BIN/npm" ]]; then
    node_path="$NODE_BIN"
  else
    local sys_node sys_npm
    sys_node="$(command -v node || true)"
    sys_npm="$(command -v npm || true)"
    if [[ -n "$sys_node" ]] && [[ -n "$sys_npm" ]]; then
      echo "Portable Node not found at $NODE_BIN"
      echo "Using system Node at $sys_node"
    else
      echo "Portable Node not found at $NODE_BIN"
      echo "Install it in-project first, then retry."
      exit 1
    fi
  fi

  echo "Starting frontend on http://localhost:3000 ..."
<<<<<<< Updated upstream
  nohup bash -lc "cd '$ROOT_DIR/frontend' && export PATH=\"$NODE_BIN:\$PATH\" && npm start" >"$FRONTEND_LOG" 2>&1 &
=======
  local export_path=""
  if [[ -n "$node_path" ]]; then
    export_path="export PATH=\"$node_path:\$PATH\" &&"
  fi
  nohup bash -lc "cd '$ROOT_DIR/frontend' && $export_path npm start" >"$FRONTEND_LOG" 2>&1 &
>>>>>>> Stashed changes
  echo $! >"$FRONTEND_PID_FILE"
}

wait_for_backend() {
  for _ in {1..20}; do
    if curl -fsS "http://127.0.0.1:8000/api/" >/dev/null 2>&1; then
      echo "Backend is healthy."
      return 0
    fi
    sleep 1
  done
  echo "Backend did not become healthy. Check $BACKEND_LOG"
  return 1
}

wait_for_frontend() {
  for _ in {1..30}; do
    if curl -fsS "http://localhost:3000" >/dev/null 2>&1; then
      echo "Frontend is responding."
      return 0
    fi
    sleep 1
  done
  echo "Frontend did not become reachable. Check $FRONTEND_LOG"
  return 1
}

stop_pid_file() {
  local pid_file="$1"
  local name="$2"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file")"
    if is_running "$pid"; then
      echo "Stopping $name (PID $pid) ..."
      kill "$pid" || true
      sleep 1
      if is_running "$pid"; then
        kill -9 "$pid" || true
      fi
    fi
    rm -f "$pid_file"
  fi
}

stop_ports_fallback() {
  local pid
  pid="$(port_pid 8000)"
  if [[ -n "$pid" ]]; then
    echo "Stopping process on port 8000 (PID $pid) ..."
    kill "$pid" || true
  fi
  pid="$(port_pid 3000)"
  if [[ -n "$pid" ]]; then
    echo "Stopping process on port 3000 (PID $pid) ..."
    kill "$pid" || true
  fi
}

status() {
  local bpid="" fpid=""
  [[ -f "$BACKEND_PID_FILE" ]] && bpid="$(cat "$BACKEND_PID_FILE")"
  [[ -f "$FRONTEND_PID_FILE" ]] && fpid="$(cat "$FRONTEND_PID_FILE")"

  if [[ -n "$bpid" ]] && is_running "$bpid"; then
    echo "Backend: running (PID $bpid) http://127.0.0.1:8000"
  else
    echo "Backend: stopped"
  fi

  if [[ -n "$fpid" ]] && is_running "$fpid"; then
    echo "Frontend: running (PID $fpid) http://localhost:3000"
  else
    echo "Frontend: stopped"
  fi
}

cmd="${1:-}"
case "$cmd" in
  start)
    start_backend
    start_frontend
    wait_for_backend || true
    wait_for_frontend || true
    status
    ;;
  stop)
    stop_pid_file "$BACKEND_PID_FILE" "backend"
    stop_pid_file "$FRONTEND_PID_FILE" "frontend"
    stop_ports_fallback
    status
    ;;
  restart)
    "$0" stop
    "$0" start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
