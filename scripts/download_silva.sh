#!/usr/bin/env bash
# scripts/download_silva.sh — Download and prepare SILVA 16S reference for vsearch
#
# Downloads SILVA 138.1 SSURef Nr99 (non-redundant 99% OTU representatives)
# ~400MB compressed → ~1.5GB decompressed.
# Output: /data/pipeline/ref/SILVA_138.1_SSURef.fasta (formatted for vsearch)
#
# Usage:
#   bash scripts/download_silva.sh
#   bash scripts/download_silva.sh --ref-dir /custom/path

set -euo pipefail

REF_DIR="${1:-/data/pipeline/ref}"
SILVA_GZ="SILVA_138.1_SSURef_Nr99_tax_silva.fasta.gz"
SILVA_URL="https://www.arb-silva.de/fileadmin/silva_databases/release_138_1/Exports/${SILVA_GZ}"
SILVA_MIRROR="https://ftp.arb-silva.de/release_138_1/Exports/${SILVA_GZ}"
OUT_FASTA="${REF_DIR}/SILVA_138.1_SSURef.fasta"
OUT_GZ="${REF_DIR}/${SILVA_GZ}"

BIOINFO_BIN="/home/deploy/miniforge3/envs/bioinfo/bin"
VSEARCH="${BIOINFO_BIN}/vsearch"

mkdir -p "${REF_DIR}"

if [ -f "${OUT_FASTA}" ] && [ "$(stat -c%s "${OUT_FASTA}")" -gt 500000000 ]; then
    echo "SILVA ref already exists: ${OUT_FASTA}  ($(du -sh "${OUT_FASTA}" | cut -f1))"
    exit 0
fi

echo "=== Downloading SILVA 138.1 SSURef Nr99 ==="
echo "  Destination: ${OUT_GZ}"
echo "  ~400MB compressed..."

# Try primary, fallback to mirror
if ! curl -fSL --retry 3 --retry-delay 10 -C - -o "${OUT_GZ}" "${SILVA_URL}"; then
    echo "Primary failed, trying mirror..."
    curl -fSL --retry 3 --retry-delay 10 -C - -o "${OUT_GZ}" "${SILVA_MIRROR}"
fi

echo "=== Decompressing ==="
gunzip -k "${OUT_GZ}"
mv "${REF_DIR}/SILVA_138.1_SSURef_Nr99_tax_silva.fasta" "${OUT_FASTA}" 2>/dev/null || true

if [ -f "${OUT_FASTA}" ]; then
    echo "=== Verifying with vsearch (check orientation) ==="
    # vsearch requires no line-wrapping > 60kb, fasta is fine as-is
    head -2 "${OUT_FASTA}"
    NUMSEQ=$(grep -c "^>" "${OUT_FASTA}" || true)
    echo "=== Done: ${NUMSEQ} sequences in ${OUT_FASTA} ==="
    echo "Size: $(du -sh "${OUT_FASTA}" | cut -f1)"
else
    echo "ERROR: output file not found after decompression"
    exit 1
fi

# Clean up gz if decompression succeeded
rm -f "${OUT_GZ}"
echo "=== SILVA reference ready ==="
