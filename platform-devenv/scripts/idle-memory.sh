#!/usr/bin/env bash
# Heap per idle session (spec §11, M1 exit: < 5 KB): baseline heap after GC, open N sessions from a separate
# devtools process, GC again, divide. Usage: idle-memory.sh <gateway-jar> <devtools-exec-jar> [N=10000]
set -euo pipefail

GATEWAY_JAR=${1:?gateway jar}
DEVTOOLS_JAR=${2:?devtools exec jar}
N=${3:-10000}
LIMIT=5120
export GATEWAY_DEV_DIR=${GATEWAY_DEV_DIR:-$(mktemp -d)}
GLOG=$(mktemp)
CLOG=$(mktemp)
CPID=""

cleanup() {
  [ -n "$CPID" ] && kill "$CPID" 2>/dev/null || true
  [ -n "${GPID:-}" ] && kill "$GPID" 2>/dev/null || true
}
trap cleanup EXIT

heap() {
  jcmd "$GPID" GC.run >/dev/null
  sleep 2
  curl -s http://127.0.0.1:8081/actuator/prometheus \
    | awk '/^jvm_memory_used_bytes\{.*area="heap"/ {s += $NF} END {printf "%.0f", s}'
}

java -jar "$GATEWAY_JAR" --spring.profiles.active=local >"$GLOG" 2>&1 &
GPID=$!
for _ in $(seq 1 30); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/ready)" = "200" ] && break
  sleep 1
done
BASE=$(heap)

java -jar "$DEVTOOLS_JAR" client --url wss://localhost:8443/v1/ws --ca "$GATEWAY_DEV_DIR/ca.crt" \
  --key "$GATEWAY_DEV_DIR/es256.pem" --load "$N" --ramp 2000 >"$CLOG" 2>&1 &
CPID=$!
for _ in $(seq 1 120); do
  grep -q "^connected=$N " "$CLOG" && break
  sleep 1
done
grep -q "^connected=$N " "$CLOG" || { echo "only $(tail -1 "$CLOG")"; exit 1; }

LOADED=$(heap)
PER=$(( (LOADED - BASE) / N ))
echo "heap_base_bytes=$BASE heap_loaded_bytes=$LOADED sessions=$N heap_per_session_bytes=$PER"
[ "$PER" -lt "$LIMIT" ] || { echo "heap per session $PER >= $LIMIT"; exit 1; }
