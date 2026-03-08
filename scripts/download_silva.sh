#!/usr/bin/env bash
# scripts/download_silva.sh — Download and prepare 16S reference for vsearch
#
# Uses NCBI 16S_ribosomal_RNA BLAST database (67MB, public, updated monthly).
# Extracts FASTA + taxonomy for vsearch classification.
#
# Output: /data/pipeline/ref/16S_ref.fasta
#
# Usage:
#   bash scripts/download_silva.sh [ref_dir]

set -euo pipefail

REF_DIR="${1:-/data/pipeline/ref}"
BIOINFO_BIN="/home/deploy/miniforge3/envs/bioinfo/bin"
BLASTDBCMD="${BIOINFO_BIN}/blastdbcmd"
OUT_FASTA="${REF_DIR}/16S_ref.fasta"
NCBI_URL="https://ftp.ncbi.nlm.nih.gov/blast/db/16S_ribosomal_RNA.tar.gz"
BLAST_DB_DIR="${REF_DIR}/16S_blast_db"

mkdir -p "${REF_DIR}" "${BLAST_DB_DIR}"

# ── Check if already built ──────────────────────────────────────────────────
if [ -f "${OUT_FASTA}" ] && [ "$(stat -c%s "${OUT_FASTA}" 2>/dev/null || echo 0)" -gt 10000000 ]; then
    echo "16S reference already exists: ${OUT_FASTA}  ($(du -sh "${OUT_FASTA}" | cut -f1))"
    exit 0
fi

echo "=== Downloading NCBI 16S ribosomal RNA BLAST db (~67MB) ==="
TARBALL="${BLAST_DB_DIR}/16S_ribosomal_RNA.tar.gz"
curl -fSL --retry 3 --retry-delay 10 -C - -o "${TARBALL}" "${NCBI_URL}"
echo "  Download complete: $(du -sh "${TARBALL}" | cut -f1)"

echo "=== Extracting BLAST database ==="
cd "${BLAST_DB_DIR}"
tar -xzf "${TARBALL}"

echo "=== Extracting sequences with taxonomy labels ==="
# Dump all sequences; header includes accession then lineage when -outfmt uses %l
"${BLASTDBCMD}" \
    -db "${BLAST_DB_DIR}/16S_ribosomal_RNA" \
    -entry all \
    -outfmt "%a %l\n%s\n" \
    > "${OUT_FASTA}.tmp" 2>/dev/null

# Convert to FASTA with taxonomy in header
python3 - "${OUT_FASTA}.tmp" "${OUT_FASTA}" << 'PYEOF'
import sys, re
src, dst = sys.argv[1], sys.argv[2]
written = 0
with open(src) as fin, open(dst, "w") as fout:
    for line in fin:
        line = line.rstrip()
        if not line:
            continue
        # First line of each pair: "accession   lineage"
        # Second line: nucleotide sequence
        if re.match(r'^[A-Z]{1,3}[_\d]', line) or '\t' in line or ';' in line[:5] is False:
            parts = line.split(None, 1)
            acc = parts[0]
            tax = parts[1] if len(parts) > 1 else "Unknown"
            fout.write(f">{acc} {tax}\n")
            written += 1
        else:
            fout.write(f"{line}\n")
print(f"Written ~{written} entries")
PYEOF

# Fallback: if conversion produced empty file, just use raw blastdbcmd FASTA dump
if [ ! -s "${OUT_FASTA}" ] || [ "$(stat -c%s "${OUT_FASTA}")" -lt 10000 ]; then
    echo "  Raw dump fallback..."
    "${BLASTDBCMD}" \
        -db "${BLAST_DB_DIR}/16S_ribosomal_RNA" \
        -entry all \
        -outfmt "%f" \
        -out "${OUT_FASTA}"
fi

NUMSEQ=$(grep -c "^>" "${OUT_FASTA}" 2>/dev/null || echo 0)
echo "=== Done: ${NUMSEQ} sequences | $(du -sh "${OUT_FASTA}" | cut -f1) | ${OUT_FASTA} ==="
head -3 "${OUT_FASTA}"
