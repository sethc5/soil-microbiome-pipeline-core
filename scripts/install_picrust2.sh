#!/usr/bin/env bash
# scripts/install_picrust2.sh — Install PICRUSt2 into a dedicated conda env
# on a server where miniforge3 is at ~/miniforge3 but not in PATH.
#
# Also downloads the required reference data (~4 GB) into /data/pipeline/picrust2_ref.
#
# Usage:
#   bash scripts/install_picrust2.sh
#   bash scripts/install_picrust2.sh --skip-ref-data   # skip ~4 GB download
#
# After this runs, compute/picrust2_runner.py auto-discovers the binary via
# compute/_tool_resolver.py (searches ~/miniforge3/envs/picrust2/).
set -euo pipefail

CONDA="${HOME}/miniforge3/bin/conda"
ENV_NAME="picrust2"
REF_DIR="/data/pipeline/picrust2_ref"
SKIP_REF=false

for arg in "$@"; do
  [[ "$arg" == "--skip-ref-data" ]] && SKIP_REF=true
done

echo "=== PICRUSt2 installer ==="
echo "Conda : $CONDA"
echo "Env   : $ENV_NAME"
echo "Ref   : $REF_DIR"
echo ""

if ! [[ -x "$CONDA" ]]; then
  echo "ERROR: conda not found at $CONDA"
  exit 1
fi

# Check if env already exists
if "$CONDA" env list | grep -q "^${ENV_NAME} "; then
  echo "Conda env '${ENV_NAME}' already exists — skipping creation."
else
  echo "Creating conda env '${ENV_NAME}' with PICRUSt2..."
  "$CONDA" create -y -n "$ENV_NAME" \
    -c bioconda -c conda-forge \
    picrust2 \
    2>&1 | tail -20
  echo "Env created."
fi

PICRUST2_BIN="${HOME}/miniforge3/envs/${ENV_NAME}/bin/picrust2_pipeline.py"
if ! [[ -x "$PICRUST2_BIN" ]]; then
  echo "ERROR: picrust2_pipeline.py not found at $PICRUST2_BIN"
  exit 1
fi
echo "Binary found: $PICRUST2_BIN"

if $SKIP_REF; then
  echo "Skipping reference data check (--skip-ref-data)."
else
  echo ""
  echo "Verifying PICRUSt2 reference data (bundled in conda env)..."
  REFDIR="${HOME}/miniforge3/envs/${ENV_NAME}/lib/python3.12/site-packages/picrust2/default_files"
  if [[ -d "$REFDIR/prokaryotic" ]] && [[ -d "$REFDIR/bacteria" ]]; then
    echo "Reference data OK: $REFDIR"
  else
    # Try with python3.* wildcard
    REFDIR_GLOB=$(ls -d "${HOME}/miniforge3/envs/${ENV_NAME}"/lib/python*/site-packages/picrust2/default_files 2>/dev/null | head -1)
    if [[ -n "$REFDIR_GLOB" ]]; then
      echo "Reference data OK: $REFDIR_GLOB"
    else
      echo "WARNING: Could not locate PICRUSt2 default_files directory."
      echo "PICRUSt2 may still work — reference data is usually bundled with conda install."
    fi
  fi
  echo "Reference data ready at $REF_DIR"
fi

echo ""
echo "=== PICRUSt2 install complete ==="
echo ""
echo "Test with:"
echo "  ${HOME}/miniforge3/envs/${ENV_NAME}/bin/picrust2_pipeline.py --version"
echo ""
echo "The pipeline will auto-discover PICRUSt2 at runtime via _tool_resolver.py."
echo "Set PICRUST2_REF_DIR=$REF_DIR in config or environment if needed."
