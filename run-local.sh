#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

BACKEND_PID_FILE="$RUN_DIR/backend.pid"
FRONTEND_PID_FILE="$RUN_DIR/frontend.pid"
BACKEND_LOG="$RUN_DIR/backend.log"
FRONTEND_LOG="$RUN_DIR/frontend.log"

NODE_BIN="$ROOT_DIR/.tools/node-v20.19.1-darwin-arm64/bin"

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
  nohup bash -lc "cd '$ROOT_DIR/backend' && source .venv/bin/activate && uvicorn server:app --host 127.0.0.1 --port 8000" >"$BACKEND_LOG" 2>&1 &
  # Store actual listener PID when available; wrapper shell PID can exit early.
  sleep 1
  local listen_pid
  listen_pid="$(port_pid 8000)"
  if [[ -n "$listen_pid" ]]; then
    echo "$listen_pid" >"$BACKEND_PID_FILE"
  else
    echo $! >"$BACKEND_PID_FILE"
  fi
}

start_frontend() {
  if [[ -f "$FRONTEND_PID_FILE" ]] && is_running "$(cat "$FRONTEND_PID_FILE")"; then
    echo "Frontend already running (PID $(cat "$FRONTEND_PID_FILE"))."
    return
  fi

  if [[ ! -x "$NODE_BIN/node" ]] || [[ ! -x "$NODE_BIN/npm" ]]; then
    echo "Portable Node not found at $NODE_BIN"
    echo "Install it in-project first, then retry."
    exit 1
  fi

  echo "Starting frontend on http://localhost:3000 ..."
  nohup bash -lc "cd '$ROOT_DIR/frontend' && export PATH=\"$NODE_BIN:\$PATH\" && npm start" >"$FRONTEND_LOG" 2>&1 &
  # Store actual listener PID when available; wrapper shell PID can exit early.
  sleep 1
  local listen_pid
  listen_pid="$(port_pid 3000)"
  if [[ -n "$listen_pid" ]]; then
    echo "$listen_pid" >"$FRONTEND_PID_FILE"
  else
    echo $! >"$FRONTEND_PID_FILE"
  fi
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
  local bpid="" fpid="" bport_pid="" fport_pid=""
  [[ -f "$BACKEND_PID_FILE" ]] && bpid="$(cat "$BACKEND_PID_FILE")"
  [[ -f "$FRONTEND_PID_FILE" ]] && fpid="$(cat "$FRONTEND_PID_FILE")"
  bport_pid="$(port_pid 8000)"
  fport_pid="$(port_pid 3000)"

  if [[ -n "$bport_pid" ]]; then
    echo "Backend: running (PID $bport_pid) http://127.0.0.1:8000"
    if [[ "$bpid" != "$bport_pid" ]]; then
      echo "$bport_pid" >"$BACKEND_PID_FILE"
    fi
  elif [[ -n "$bpid" ]] && is_running "$bpid"; then
    echo "Backend: running (PID $bpid) http://127.0.0.1:8000"
  else
    echo "Backend: stopped"
  fi

  if [[ -n "$fport_pid" ]]; then
    echo "Frontend: running (PID $fport_pid) http://localhost:3000"
    if [[ "$fpid" != "$fport_pid" ]]; then
      echo "$fport_pid" >"$FRONTEND_PID_FILE"
    fi
  elif [[ -n "$fpid" ]] && is_running "$fpid"; then
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
