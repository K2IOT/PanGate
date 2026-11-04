#!/usr/bin/env bash
# Idle-session load check (spec §12 M1 exit): open N sessions from a separate devtools process, wait, then require
# every event loop's p99 lag < 20 ms and shed level 0 (M3a exit). Usage: idle-load.sh <gateway-jar> <devtools-exec-jar> [N=18000]
# N > ~19k needs `ulimit -n` well above 2*N (gateway and client each hold N sockets); >28k needs extra --bind IPs.
# Back-to-back runs: the previous run's client sockets sit in TIME_WAIT for 60 s and use the same ephemeral ports.
set -euo pipefail

GATEWAY_JAR=${1:?gateway jar}
DEVTOOLS_JAR=${2:?devtools exec jar}
N=${3:-18000}
RAMP=${RAMP:-2000}  # new sessions per second; lower it on a small or shared box (client and gateway share CPU)
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
  --key "$GATEWAY_DEV_DIR/es256.pem" --load "$N" --ramp "$RAMP" --bind "$BINDS" >"$CLOG" 2>&1 &
CPID=$!
for _ in $(seq 1 $((N / 1000 * 2 + 60))); do
  grep -q "^connected=$N " "$CLOG" && break
  sleep 1
done
grep -q "^connected=$N " "$CLOG" || { echo "only $(tail -1 "$CLOG")"; exit 1; }
sleep 15
# The ramp (2000 TLS handshakes/s, client on the same box) can raise lag p99 past 50 ms and enter L1, which exits
# only after 30 s of calm (spec §7.3). The exit criterion is the steady state: wait up to 180 s for level 0.
shed() { curl -s http://127.0.0.1:8081/actuator/prometheus | awk '/^gw_shed_level/ {print int($2); exit}'; }
for i in $(seq 1 180); do
  [ "$(shed)" = "0" ] && break
  sleep 1
done
echo "shed level transitions during the run: $(grep -c 'shed level' "$GLOG"); level 0 after $((i - 1)) s more"

PROM=$(curl -s http://127.0.0.1:8081/actuator/prometheus)
LAG=$(awk '/^gw_eventloop_lag_seconds/ {print $2}' <<<"$PROM" | sort -g | tail -1)
SHED=$(awk '/^gw_shed_level/ {print $2}' <<<"$PROM" | head -1)
echo "sessions=$N max_loop_lag_p99_seconds=$LAG shed_level=$SHED"
[ "${SHED%.*}" = "0" ] || { echo "shed level ${SHED:-missing} != 0 under idle load"; exit 1; }
awk -v l="$LAG" -v m="$MAX_LAG" 'BEGIN { exit !(l < m) }' || { echo "loop lag $LAG >= $MAX_LAG"; exit 1; }
