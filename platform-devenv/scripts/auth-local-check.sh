#!/usr/bin/env bash
# auth-service local check (auth spec §12 item 2): start the jar with profile local against `make infra-up`, wait for
# /ready on the management port, mint a connect token through it with devtools (client credentials from the seed
# file), check the JWKS, stop with SIGTERM.
# Usage: auth-local-check.sh <auth-exec-jar> <devtools-exec-jar>
set -euo pipefail

AUTH_JAR=${1:?auth jar}
DEVTOOLS_JAR=${2:?devtools exec jar}
SEED_DIR=${AUTH_SEED_DIR:-$(mktemp -d)}
LOG=$(mktemp)
PID=""

fail() {
  echo "AUTH LOCAL CHECK FAILED: $*"
  echo "---- auth log (tail) ----"
  tail -n 80 "$LOG" || true
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  exit 1
}

java -jar "$AUTH_JAR" --spring.profiles.active=local --auth.seed.dir="$SEED_DIR" >>"$LOG" 2>&1 &
PID=$!
ready=""
for _ in $(seq 1 60); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8082/ready)" = "200" ]; then
    ready=1
    break
  fi
  kill -0 "$PID" 2>/dev/null || fail "auth-service exited during startup"
  sleep 1
done
[ -n "$ready" ] || fail "/ready not 200 within 60 s"

[ -s "$SEED_DIR/clients.json" ] || fail "seed file $SEED_DIR/clients.json missing"
curl -sf http://127.0.0.1:8090/.well-known/jwks.json | grep -q '"kid"' || fail "JWKS has no key"

TOKEN=$(java -jar "$DEVTOOLS_JAR" token --auth-url http://127.0.0.1:8090 --client-file "$SEED_DIR/clients.json" \
  --principal t1:check --device check-1 2>>"$LOG") || fail "devtools token --auth-url failed"
[[ "$TOKEN" =~ ^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$ ]] || fail "not a JWT: $TOKEN"
echo "connect token minted through auth-service (${#TOKEN} chars)"

kill -TERM "$PID"
for _ in $(seq 1 75); do
  kill -0 "$PID" 2>/dev/null || { PID=""; break; }
  sleep 0.2
done
[ -z "$PID" ] || fail "auth-service did not exit within 15 s of SIGTERM"
echo "AUTH LOCAL CHECK OK"
