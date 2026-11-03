#!/usr/bin/env bash
# Idle-session load check (spec §12 M1 exit): open N sessions from a separate devtools process, wait, then require
# every event loop's p99 lag < 20 ms. Usage: idle-load.sh <gateway-jar> <devtools-exec-jar> [N=18000]
# N > ~19k needs `ulimit -n` well above 2*N (gateway and client each hold N sockets); >28k needs extra --bind IPs.
set -euo pipefail

GATEWAY_JAR=${1:?gateway jar}
DEVTOOLS_JAR=${2:?devtools exec jar}
N=${3:-18000}
MAX_LAG=0.020
export GATEWAY_DEV_DIR=${GATEWAY_DEV_DIR:-$(mktemp -d)}
GLOG=$(mktemp)
CLOG=$(mktemp)
CPID=""

cleanup() {
  [ -n "$CPID" ] && kill "$CPID" 2>/dev/null || true
  [ -n "${GPID:-}" ] && kill "$GPID" 2>/dev/null || true
}
trap cleanup EXIT

java -jar "$GATEWAY_JAR" --spring.profiles.active=local >"$GLOG" 2>&1 &
GPID=$!
for _ in $(seq 1 30); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/ready)" = "200" ] && break
  sleep 1
done

BINDS=127.0.0.1
[ "$N" -gt 25000 ] && BINDS=127.0.0.1,127.0.0.2,127.0.0.3
java -jar "$DEVTOOLS_JAR" client --url wss://localhost:8443/v1/ws --ca "$GATEWAY_DEV_DIR/ca.crt" \
  --key "$GATEWAY_DEV_DIR/es256.pem" --load "$N" --ramp 2000 --bind "$BINDS" >"$CLOG" 2>&1 &
CPID=$!
for _ in $(seq 1 $((N / 1000 * 2 + 60))); do
  grep -q "^connected=$N " "$CLOG" && break
  sleep 1
done
grep -q "^connected=$N " "$CLOG" || { echo "only $(tail -1 "$CLOG")"; exit 1; }
sleep 15

LAG=$(curl -s http://127.0.0.1:8081/actuator/prometheus | awk '/^gw_eventloop_lag_seconds/ {print $2}' | sort -g | tail -1)
echo "sessions=$N max_loop_lag_p99_seconds=$LAG"
awk -v l="$LAG" -v m="$MAX_LAG" 'BEGIN { exit !(l < m) }' || { echo "loop lag $LAG >= $MAX_LAG"; exit 1; }
