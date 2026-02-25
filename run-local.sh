#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

BACKEND_PID_FILE="$RUN_DIR/backend.pid"
FRONTEND_PID_FILE="$RUN_DIR/frontend.pid"
BACKEND_LOG="$RUN_DIR/backend.log"
FRONTEND_LOG="$RUN_DIR/frontend.log"

BACKEND_VENV_DIR="$ROOT_DIR/backend/.venv"
FRONTEND_NODE_BIN=""
FRONTEND_NPM_CMD="npm"

is_windows() {
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) return 0 ;;
    *) return 1 ;;
  esac
}

venv_python_path() {
  if is_windows; then
    echo "$BACKEND_VENV_DIR/Scripts/python.exe"
  else
    echo "$BACKEND_VENV_DIR/bin/python"
  fi
}

venv_activate_path() {
  if is_windows; then
    echo "$BACKEND_VENV_DIR/Scripts/activate"
  else
    echo "$BACKEND_VENV_DIR/bin/activate"
  fi
}

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

kill_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return
  fi

  if is_windows && command -v taskkill >/dev/null 2>&1; then
    taskkill //PID "$pid" //T //F >/dev/null 2>&1 || true
  else
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    if is_running "$pid"; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi
}

port_pid() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
    return
  fi

  if is_windows && command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess)" 2>/dev/null | tr -d '\r' || true
    return
  fi

  echo ""
}

ensure_backend_venv() {
  local vpy
  vpy="$(venv_python_path)"
  if [[ -x "$vpy" ]]; then
    return
  fi

  local base_python=""
  if command -v python3 >/dev/null 2>&1; then
    base_python="python3"
  elif command -v python >/dev/null 2>&1; then
    base_python="python"
  else
    echo "python3/python is required to create backend/.venv but was not found."
    exit 1
  fi

  echo "Creating backend virtual environment at $BACKEND_VENV_DIR ..."
  "$base_python" -m venv "$BACKEND_VENV_DIR"
}

backend_deps_ready() {
  local vpy
  vpy="$(venv_python_path)"
  "$vpy" -c "import fastapi, uvicorn" >/dev/null 2>&1
}

venv_pip_path() {
  if is_windows; then
    echo "$BACKEND_VENV_DIR/Scripts/pip.exe"
  else
    echo "$BACKEND_VENV_DIR/bin/pip"
  fi
}

ensure_backend_deps() {
  if backend_deps_ready; then
    return 0
  fi

  local vpip
  vpip="$(venv_pip_path)"
  if [[ ! -x "$vpip" ]] && [[ ! -f "$vpip" ]]; then
    echo "pip was not found in backend/.venv."
    return 1
  fi

  echo "Installing backend dependencies into backend/.venv ..."
  "$vpip" install -r "$ROOT_DIR/backend/requirements.txt"
  backend_deps_ready
}

node_tools_in_dir() {
  local dir="$1"

  if [[ -x "$dir/node" ]] && ([[ -x "$dir/npm" ]] || [[ -f "$dir/npm.cmd" ]]); then
    return 0
  fi

  if [[ -f "$dir/node.exe" ]] && ([[ -f "$dir/npm.cmd" ]] || [[ -f "$dir/npm" ]]); then
    return 0
  fi

  return 1
}

resolve_frontend_runtime() {
  local override="${NODE_BIN:-}"
  if [[ -n "$override" ]] && node_tools_in_dir "$override"; then
    FRONTEND_NODE_BIN="$override"
    if [[ -f "$override/npm.cmd" ]]; then
      FRONTEND_NPM_CMD="npm.cmd"
    else
      FRONTEND_NPM_CMD="npm"
    fi
    return 0
  fi

  local candidate
  for candidate in "$ROOT_DIR"/.tools/node-*/bin "$ROOT_DIR"/.tools/node-*; do
    [[ -d "$candidate" ]] || continue
    if node_tools_in_dir "$candidate"; then
      FRONTEND_NODE_BIN="$candidate"
      if [[ -f "$candidate/npm.cmd" ]]; then
        FRONTEND_NPM_CMD="npm.cmd"
      else
        FRONTEND_NPM_CMD="npm"
      fi
      return 0
    fi
  done

  if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
    FRONTEND_NODE_BIN=""
    FRONTEND_NPM_CMD="npm"
    return 0
  fi

  if [[ -n "${NVM_DIR:-}" ]] && [[ -s "${NVM_DIR}/nvm.sh" ]]; then
    # shellcheck disable=SC1090
    source "${NVM_DIR}/nvm.sh"
  elif [[ -s "$HOME/.nvm/nvm.sh" ]]; then
    # shellcheck disable=SC1090
    source "$HOME/.nvm/nvm.sh"
  fi

  if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
    FRONTEND_NODE_BIN=""
    FRONTEND_NPM_CMD="npm"
    return 0
  fi

  return 1
}

run_frontend_cmd() {
  local cmd="$1"
  if [[ -n "$FRONTEND_NODE_BIN" ]]; then
    bash -lc "cd '$ROOT_DIR/frontend' && export PATH='$FRONTEND_NODE_BIN':\$PATH && $cmd"
  else
    bash -lc "cd '$ROOT_DIR/frontend' && $cmd"
  fi
}

frontend_deps_ready() {
  [[ -d "$ROOT_DIR/frontend/node_modules" ]] || return 1
  [[ -x "$ROOT_DIR/frontend/node_modules/.bin/craco" ]] || [[ -f "$ROOT_DIR/frontend/node_modules/.bin/craco.cmd" ]]
}

ensure_frontend_deps() {
  if frontend_deps_ready; then
    return 0
  fi

  echo "Installing frontend dependencies in frontend/node_modules ..."
  local install_cmd=""
  if [[ -f "$ROOT_DIR/frontend/package-lock.json" ]]; then
    install_cmd="npm ci"
  else
    install_cmd="npm install"
  fi

  if ! run_frontend_cmd "$install_cmd"; then
    echo "npm install failed. Retrying with --legacy-peer-deps ..."
    run_frontend_cmd "$install_cmd --legacy-peer-deps"
  fi
  frontend_deps_ready
}

start_backend() {
  if [[ -f "$BACKEND_PID_FILE" ]] && is_running "$(cat "$BACKEND_PID_FILE")"; then
    echo "Backend already running (PID $(cat "$BACKEND_PID_FILE"))."
    return 0
  fi

  ensure_backend_venv

  if ! ensure_backend_deps; then
    echo "Backend dependencies are missing in backend/.venv."
    echo "Install them into the project venv:"
    echo "  $BACKEND_VENV_DIR/bin/pip install -r backend/requirements.txt"
    if is_windows; then
      echo "  $BACKEND_VENV_DIR/Scripts/pip.exe install -r backend/requirements.txt"
    fi
    echo "Skipping backend startup."
    return 1
  fi

  echo "Starting backend on http://127.0.0.1:8000 ..."
  local activate
  activate="$(venv_activate_path)"
  nohup bash -lc "cd '$ROOT_DIR/backend' && source '$activate' && uvicorn server:app --host 127.0.0.1 --port 8000" >"$BACKEND_LOG" 2>&1 &

  sleep 1
  local listen_pid
  listen_pid="$(port_pid 8000)"
  if [[ -n "$listen_pid" ]]; then
    echo "$listen_pid" >"$BACKEND_PID_FILE"
  else
    echo $! >"$BACKEND_PID_FILE"
  fi
  return 0
}

start_frontend() {
  if [[ -f "$FRONTEND_PID_FILE" ]] && is_running "$(cat "$FRONTEND_PID_FILE")"; then
    echo "Frontend already running (PID $(cat "$FRONTEND_PID_FILE"))."
    return 0
  fi

  if ! resolve_frontend_runtime; then
    echo "Node/npm not found. Install Node (or load nvm), or place a portable runtime in .tools/node-*/bin."
    echo "Skipping frontend startup."
    return 1
  fi

  if ! ensure_frontend_deps; then
    echo "Frontend dependencies install failed. Check npm output above."
    echo "Skipping frontend startup."
    return 1
  fi

  echo "Starting frontend on http://localhost:3000 ..."
  if [[ -n "$FRONTEND_NODE_BIN" ]]; then
    nohup bash -lc "cd '$ROOT_DIR/frontend' && export PATH='$FRONTEND_NODE_BIN':\$PATH && $FRONTEND_NPM_CMD start" >"$FRONTEND_LOG" 2>&1 &
  else
    nohup bash -lc "cd '$ROOT_DIR/frontend' && $FRONTEND_NPM_CMD start" >"$FRONTEND_LOG" 2>&1 &
  fi

  sleep 1
  local listen_pid
  listen_pid="$(port_pid 3000)"
  if [[ -n "$listen_pid" ]]; then
    echo "$listen_pid" >"$FRONTEND_PID_FILE"
  else
    echo $! >"$FRONTEND_PID_FILE"
  fi
  return 0
}

wait_for_backend() {
  for _ in {1..25}; do
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
  for _ in {1..40}; do
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
      kill_pid "$pid"
    fi
    rm -f "$pid_file"
  fi
}

stop_ports_fallback() {
  local pid

  pid="$(port_pid 8000)"
  if [[ -n "$pid" ]]; then
    echo "Stopping process on port 8000 (PID $pid) ..."
    kill_pid "$pid"
  fi

  pid="$(port_pid 3000)"
  if [[ -n "$pid" ]]; then
    echo "Stopping process on port 3000 (PID $pid) ..."
    kill_pid "$pid"
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
    backend_started=0
    frontend_started=0

    if start_backend; then
      backend_started=1
    fi

    if start_frontend; then
      frontend_started=1
    fi

    if [[ "$backend_started" -eq 1 ]]; then
      wait_for_backend || true
    fi

    if [[ "$frontend_started" -eq 1 ]]; then
      wait_for_frontend || true
    fi

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
