#!/usr/bin/env bash
# Demo MVP catálogo 2.1: 3 nós, eleição, Put replicado, crash de 1 follower sem perder commits.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN="${BIN:-target/debug/raft-node}"
DATA="${DATA_DIR:-/tmp/raft-mvp-demo-$$}"
P1=7101; P2=7102; P3=7103
LOGDIR="$DATA/logs"
mkdir -p "$DATA"/{n1,n2,n3} "$LOGDIR"

cleanup() {
  echo ""
  echo "==> cleanup"
  for pid in ${PID1:-} ${PID2:-} ${PID3:-}; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "==> building raft-node"
cargo build -q --bin raft-node

echo "==> starting 3 nodes in $DATA"
"$BIN" serve --id 1 --bind 127.0.0.1:$P1 \
  --peers "2=127.0.0.1:$P2,3=127.0.0.1:$P3" --data-dir "$DATA/n1" \
  >"$LOGDIR/n1.log" 2>&1 &
PID1=$!
"$BIN" serve --id 2 --bind 127.0.0.1:$P2 \
  --peers "1=127.0.0.1:$P1,3=127.0.0.1:$P3" --data-dir "$DATA/n2" \
  >"$LOGDIR/n2.log" 2>&1 &
PID2=$!
"$BIN" serve --id 3 --bind 127.0.0.1:$P3 \
  --peers "1=127.0.0.1:$P1,2=127.0.0.1:$P2" --data-dir "$DATA/n3" \
  >"$LOGDIR/n3.log" 2>&1 &
PID3=$!

sleep 0.4

echo "==> waiting for leader (status)"
LEADER_ADDR=""
for _ in $(seq 1 40); do
  for addr in 127.0.0.1:$P1 127.0.0.1:$P2 127.0.0.1:$P3; do
    out="$("$BIN" status --addr "$addr" 2>/dev/null || true)"
    if echo "$out" | grep -q "role=Leader"; then
      LEADER_ADDR="$addr"
      echo "    leader at $addr :: $out"
      break 2
    fi
  done
  sleep 0.15
done
if [[ -z "$LEADER_ADDR" ]]; then
  echo "FAIL: no leader elected" >&2
  echo "---- logs ----"
  tail -n 30 "$LOGDIR"/*.log || true
  exit 1
fi

echo "==> Put hello=raft via $LEADER_ADDR"
"$BIN" put --addr "$LEADER_ADDR" hello raft
echo "==> Get hello"
got="$("$BIN" get --addr "$LEADER_ADDR" hello)"
echo "    => $got"
[[ "$got" == "raft" ]] || { echo "FAIL: expected raft got $got" >&2; exit 1; }

echo "==> metrics (valores reais do processo, sem taxa)"
metrics="$("$BIN" metrics --addr "$LEADER_ADDR")"
echo "    $metrics"
echo "$metrics" | grep -q "commits=" || { echo "FAIL: metrics sem commits" >&2; exit 1; }
echo "$metrics" | grep -q "elections=" || { echo "FAIL: metrics sem elections" >&2; exit 1; }
echo "$metrics" | grep -q "snapshot_bytes=" || { echo "FAIL: metrics sem snapshot_bytes" >&2; exit 1; }

# Identify a follower pid to kill
FOLLOWER_PID=""; FOLLOWER_PORT=""; FOLLOWER_ID=""
for pair in "1:$P1:$PID1" "2:$P2:$PID2" "3:$P3:$PID3"; do
  id="${pair%%:*}"; rest="${pair#*:}"; port="${rest%%:*}"; pid="${rest##*:}"
  out="$("$BIN" status --addr "127.0.0.1:$port" 2>/dev/null || true)"
  if echo "$out" | grep -q "role=Follower"; then
    FOLLOWER_PID=$pid; FOLLOWER_PORT=$port; FOLLOWER_ID=$id
    break
  fi
done
if [[ -z "$FOLLOWER_PID" ]]; then
  echo "FAIL: no follower found to crash" >&2
  exit 1
fi

echo "==> crashing follower id=$FOLLOWER_ID port=$FOLLOWER_PORT pid=$FOLLOWER_PID"
kill "$FOLLOWER_PID"
sleep 0.3
# clear pid so cleanup doesn't double-kill
case $FOLLOWER_ID in
  1) PID1="" ;;
  2) PID2="" ;;
  3) PID3="" ;;
esac

echo "==> Put during outage (majority 2/3 must still commit)"
"$BIN" put --addr "$LEADER_ADDR" during ok
got2="$("$BIN" get --addr "$LEADER_ADDR" during)"
echo "    => $got2"
[[ "$got2" == "ok" ]] || { echo "FAIL: put during outage" >&2; exit 1; }

echo "==> restarting follower $FOLLOWER_ID"
case $FOLLOWER_ID in
  1) peers="2=127.0.0.1:$P2,3=127.0.0.1:$P3"; d="$DATA/n1" ;;
  2) peers="1=127.0.0.1:$P1,3=127.0.0.1:$P3"; d="$DATA/n2" ;;
  3) peers="1=127.0.0.1:$P1,2=127.0.0.1:$P2"; d="$DATA/n3" ;;
esac
"$BIN" serve --id "$FOLLOWER_ID" --bind "127.0.0.1:$FOLLOWER_PORT" \
  --peers "$peers" --data-dir "$d" >"$LOGDIR/n${FOLLOWER_ID}-restart.log" 2>&1 &
case $FOLLOWER_ID in
  1) PID1=$! ;;
  2) PID2=$! ;;
  3) PID3=$! ;;
esac
sleep 0.8

echo "==> Put after recovery"
"$BIN" put --addr "$LEADER_ADDR" after recovered
got3="$("$BIN" get --addr "$LEADER_ADDR" after)"
echo "    => $got3"
[[ "$got3" == "recovered" ]] || { echo "FAIL: after recovery" >&2; exit 1; }

echo ""
echo "OK — demo MVP passou: eleição, replicate, crash follower, recover."
