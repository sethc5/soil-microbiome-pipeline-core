#!/usr/bin/env bash
# scripts/mgnify_tunnel.sh — MGnify ingest via local-machine JSONL transfer
#
# EBI's metagenomics API backend silently drops connections from Hetzner's
# ASN (AS24940).  This script runs on YOUR LOCAL MACHINE, fetches from MGnify
# directly (which works fine locally), writes a JSONL dump, scp's it to the
# server, then loads it into the pipeline DB.
#
# Usage (run from LOCAL machine, not the server):
#   bash scripts/mgnify_tunnel.sh
#   bash scripts/mgnify_tunnel.sh --max-results 5000  # quick test
#   bash scripts/mgnify_tunnel.sh --dry-run           # API test only
#
# Requires:
#   - 'pipeline' SSH alias in ~/.ssh/config  (already configured)
#   - Python venv active locally with requests or httpx installed
#   - scripts/ingest_mgnify.py (this repo)

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────
SERVER_ALIAS=${SERVER_ALIAS:-pipeline}
REMOTE_DB=${REMOTE_DB:-/data/pipeline/db/soil_microbiome.db}
REMOTE_PYTHON=${REMOTE_PYTHON:-/opt/pipeline/.venv/bin/python}
REMOTE_SCRIPT=${REMOTE_SCRIPT:-/opt/pipeline/scripts/ingest_mgnify.py}
MAX_RESULTS=${MAX_RESULTS:-50000}
LOCAL_JSONL=${LOCAL_JSONL:-/tmp/mgnify_soil_$(date +%Y%m%d_%H%M).jsonl}
DRY_RUN=""

# ── Parse args ────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-results=*)  MAX_RESULTS="${1#*=}"; shift ;;
        --max-results)    MAX_RESULTS="$2"; shift 2 ;;
        --dry-run)        DRY_RUN="--dry-run"; shift ;;
        --jsonl=*)        LOCAL_JSONL="${1#*=}"; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "===================================================="
echo " MGnify JSONL Transfer Ingest"
echo " Max results : $MAX_RESULTS"
echo " Local JSONL : $LOCAL_JSONL"
echo " Server      : $SERVER_ALIAS"
echo "===================================================="

# ── Step 1: Fetch from MGnify locally (has EBI access) ───────────────────
if [[ -n "$DRY_RUN" ]]; then
    echo "[1/3] DRY RUN — testing API access (5 results)..."
    python "$SCRIPT_DIR/ingest_mgnify.py" \
        --db /dev/null \
        --max-results 5 \
        --dry-run
    echo "[dry-run] API reachable. Remove --dry-run to do a real run."
    exit 0
fi

echo "[1/3] Fetching from MGnify API locally → $LOCAL_JSONL ..."
python "$SCRIPT_DIR/ingest_mgnify.py" \
    --db /dev/null \
    --max-results "$MAX_RESULTS" \
    --output-jsonl "$LOCAL_JSONL" \
    --resume \
    --checkpoint "${LOCAL_JSONL%.jsonl}_checkpoint.json"

RECORD_COUNT=$(wc -l < "$LOCAL_JSONL" || echo 0)
echo "[1/3] Done. $RECORD_COUNT records written to $LOCAL_JSONL"

if [[ "$RECORD_COUNT" -eq 0 ]]; then
    echo "ERROR: no records fetched — check API access and biome names."
    exit 1
fi

# ── Step 2: Transfer JSONL to server ─────────────────────────────────────
REMOTE_TMP="/tmp/$(basename "$LOCAL_JSONL")"
echo "[2/3] Transferring $(du -sh "$LOCAL_JSONL" | cut -f1) to ${SERVER_ALIAS}:${REMOTE_TMP} ..."
scp "$LOCAL_JSONL" "${SERVER_ALIAS}:${REMOTE_TMP}"

# ── Step 3: Load into server DB ──────────────────────────────────────────
echo "[3/3] Loading into server DB: $REMOTE_DB ..."
ssh "$SERVER_ALIAS" \
    "${REMOTE_PYTHON} ${REMOTE_SCRIPT} --db ${REMOTE_DB} --from-jsonl ${REMOTE_TMP}"

# Cleanup remote temp
ssh "$SERVER_ALIAS" "rm -f ${REMOTE_TMP}"

echo ""
echo "==== MGnify ingest complete ===="
echo "  Records transferred : $RECORD_COUNT"
echo "  Local JSONL kept at : $LOCAL_JSONL"
    "$SERVER_ALIAS"

echo "[tunnel] Reverse tunnel established. Running ingest on server..."

# ── Step 3: Run ingest remotely with proxy env var ───────────────────────
DRY_FLAG=${DRY_RUN:+--dry-run}
ssh -S "$CONTROL_SOCKET" "$SERVER_ALIAS" \
    "cd /opt/pipeline && \
     MGNIFY_PROXY=socks5://localhost:${SOCKS_PORT} \
     ${REMOTE_PYTHON} ${REMOTE_SCRIPT} \
       --db ${REMOTE_DB} \
       --max-results ${MAX_RESULTS} \
       --resume \
       ${DRY_FLAG:-}"

EXIT_CODE=$?

# ── Step 4: Tear down tunnel ──────────────────────────────────────────────
echo "[tunnel] Ingest finished (exit $EXIT_CODE). Closing tunnel..."
ssh -S "$CONTROL_SOCKET" -O exit "$SERVER_ALIAS" 2>/dev/null || true
# Also kill local SOCKS if we started it
if [[ -n "${LOCAL_SOCKS_PID:-}" ]]; then
    kill "$LOCAL_SOCKS_PID" 2>/dev/null || true
fi

echo "[tunnel] Done."
exit $EXIT_CODE
