#!/usr/bin/env bash
# scripts/install_sra_tools.sh — Install NCBI SRA Tools into conda env on server.
#
# Installs: prefetch, fasterq-dump (required by adapters/ncbi_sra_adapter.py)
# Uses the same miniforge3 at ~/miniforge3.
#
# Usage:
#   bash scripts/install_sra_tools.sh
set -euo pipefail

CONDA="${HOME}/miniforge3/bin/conda"
ENV_NAME="sra-tools"

echo "=== SRA Tools installer ==="
echo "Conda : $CONDA"
echo "Env   : $ENV_NAME"
echo ""

if ! [[ -x "$CONDA" ]]; then
  echo "ERROR: conda not found at $CONDA"
  exit 1
fi

if "$CONDA" env list | grep -q "^${ENV_NAME} "; then
  echo "Conda env '${ENV_NAME}' already exists."
else
  echo "Creating conda env '${ENV_NAME}' with sra-tools..."
  "$CONDA" create -y -n "$ENV_NAME" \
    -c bioconda -c conda-forge \
    sra-tools \
    2>&1 | tail -20
fi

PREFETCH="${HOME}/miniforge3/envs/${ENV_NAME}/bin/prefetch"
FASTERQ="${HOME}/miniforge3/envs/${ENV_NAME}/bin/fasterq-dump"

for BIN in "$PREFETCH" "$FASTERQ"; do
  if ! [[ -x "$BIN" ]]; then
    echo "ERROR: $BIN not found after install"
    exit 1
  fi
done

echo ""
echo "=== SRA Tools install complete ==="
echo "  prefetch     : $PREFETCH"
echo "  fasterq-dump : $FASTERQ"
echo ""
echo "Test with:"
echo "  $PREFETCH --version"
echo ""
echo "The SRA adapter (adapters/ncbi_sra_adapter.py) will auto-discover"
echo "these tools via compute/_tool_resolver.py."
echo ""
echo "Configure SRA cache location (recommended, avoids filling /tmp):"
echo "  ${HOME}/miniforge3/envs/${ENV_NAME}/bin/vdb-config --interactive"
echo "  # Set cache to: /data/pipeline/sra_cache"
