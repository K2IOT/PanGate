#!/usr/bin/env bash
# Local start check (spec §9.1 R1, R3, R4): start the gateway jar with profile local, wait for /ready,
# connect once with the devtools client, stop with SIGTERM (<= 15 s), start again and wait for /ready.
# Usage: local-start-check.sh <gateway-jar> <devtools-exec-jar>
set -euo pipefail

GATEWAY_JAR=${1:?gateway jar}
DEVTOOLS_JAR=${2:?devtools exec jar}
export GATEWAY_DEV_DIR=${GATEWAY_DEV_DIR:-$(mktemp -d)}
LOG=$(mktemp)
PID=""

fail() {
  echo "LOCAL START FAILED: $*"
  echo "---- gateway log (tail) ----"
  tail -n 80 "$LOG" || true
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  exit 1
}

start() {
  java -jar "$GATEWAY_JAR" --spring.profiles.active=local >>"$LOG" 2>&1 &
  PID=$!
  for _ in $(seq 1 30); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/ready)" = "200" ]; then
      return 0
    fi
    kill -0 "$PID" 2>/dev/null || fail "gateway exited during startup"
    sleep 1
  done
  fail "/ready not 200 within 30 s"
}

stop() {
  kill -TERM "$PID"
  for _ in $(seq 1 75); do
    kill -0 "$PID" 2>/dev/null || { PID=""; return 0; }
    sleep 0.2
  done
  fail "gateway did not exit within 15 s after SIGTERM"
}

start
TOKEN=$(java -jar "$DEVTOOLS_JAR" token --key "$GATEWAY_DEV_DIR/es256.pem" --tenant t1 --principal t1:check --device check-1 2>/dev/null)
java -jar "$DEVTOOLS_JAR" client --url wss://localhost:8443/v1/ws --ca "$GATEWAY_DEV_DIR/ca.crt" \
  --token "$TOKEN" --device check-1 --once 2>/dev/null | grep -q '^PONG$' || fail "client HELLO/PING failed"
stop
start
stop
echo "LOCAL START OK"
