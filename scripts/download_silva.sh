#!/usr/bin/env bash
# scripts/download_silva.sh — Download and prepare 16S reference for vsearch
#
# Builds 16S_ref.fasta with full taxonomy lineage in FASTA headers so vsearch
# can produce taxonomically-labelled hits.
#
# Strategy:
#   1. Download NCBI 16S_ribosomal_RNA BLAST db (~67MB)
#   2. Extract sequences (standard FASTA) + per-sequence taxids
#   3. Download NCBI rankedlineage.dmp (~60MB) to map taxid → phylum;genus
#   4. Write final FASTA: >acc Superkingdom; Phylum; Class; Genus
#
# Output: /data/pipeline/ref/16S_ref.fasta
# Usage:  bash scripts/download_silva.sh [ref_dir]

set -euo pipefail

REF_DIR="${1:-/data/pipeline/ref}"
BIOINFO_BIN="/home/deploy/miniforge3/envs/bioinfo/bin"
BLASTDBCMD="${BIOINFO_BIN}/blastdbcmd"
OUT_FASTA="${REF_DIR}/16S_ref.fasta"
NCBI_16S_URL="https://ftp.ncbi.nlm.nih.gov/blast/db/16S_ribosomal_RNA.tar.gz"
TAXDUMP_URL="https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"
BLAST_DB_DIR="${REF_DIR}/16S_blast_db"
TAX_DIR="${REF_DIR}/taxdump"

mkdir -p "${REF_DIR}" "${BLAST_DB_DIR}" "${TAX_DIR}"

# ── Skip if already built with taxonomy ──────────────────────────────────────
if [ -f "${OUT_FASTA}" ] && [ "$(stat -c%s "${OUT_FASTA}" 2>/dev/null || echo 0)" -gt 5000000 ]; then
    if head -2 "${OUT_FASTA}" | grep -q ";"; then
        NSEQ=$(grep -c "^>" "${OUT_FASTA}" 2>/dev/null || echo 0)
        echo "16S reference already built: ${OUT_FASTA} (${NSEQ} seqs, $(du -sh "${OUT_FASTA}" | cut -f1))"
        exit 0
    fi
    echo "Existing FASTA lacks taxonomy — rebuilding..."
fi

# ── Step 1: download BLAST db ─────────────────────────────────────────────
TARBALL="${BLAST_DB_DIR}/16S_ribosomal_RNA.tar.gz"
if [ ! -f "${TARBALL}" ] || [ "$(stat -c%s "${TARBALL}" 2>/dev/null || echo 0)" -lt 1000000 ]; then
    echo "=== Downloading NCBI 16S ribosomal RNA BLAST db (~67MB) ==="
    curl -fSL --retry 3 --retry-delay 10 -C - -o "${TARBALL}" "${NCBI_16S_URL}"
    echo "  Downloaded: $(du -sh "${TARBALL}" | cut -f1)"
fi

# ── Step 2: extract BLAST db ──────────────────────────────────────────────
if [ ! -f "${BLAST_DB_DIR}/16S_ribosomal_RNA.nin" ] && \
   [ ! -f "${BLAST_DB_DIR}/16S_ribosomal_RNA.nsq" ]; then
    echo "=== Extracting BLAST database ==="
    cd "${BLAST_DB_DIR}" && tar -xzf "${TARBALL}" && cd -
fi

DB="${BLAST_DB_DIR}/16S_ribosomal_RNA"

# ── Step 3: dump sequences + taxids from BLAST db ────────────────────────
echo "=== Dumping sequences and taxids from BLAST db ==="
RAW_FASTA="${REF_DIR}/16S_raw.fasta"
TAXID_MAP="${REF_DIR}/16S_taxids.tsv"

"${BLASTDBCMD}" -db "${DB}" -entry all -outfmt "%f" -out "${RAW_FASTA}" 2>/dev/null
echo "  Sequences: $(grep -c "^>" "${RAW_FASTA}")"

"${BLASTDBCMD}" -db "${DB}" -entry all -outfmt "%a %T" -out "${TAXID_MAP}" 2>/dev/null
echo "  Taxid map: $(wc -l < "${TAXID_MAP}") entries"

# ── Step 4: download NCBI rankedlineage ──────────────────────────────────
TAXDUMP_TGZ="${TAX_DIR}/taxdump.tar.gz"
LINEAGE_FILE="${TAX_DIR}/rankedlineage.dmp"

if [ ! -f "${LINEAGE_FILE}" ]; then
    echo "=== Downloading NCBI taxonomy (~60MB) ==="
    curl -fSL --retry 3 --retry-delay 10 -C - -o "${TAXDUMP_TGZ}" "${TAXDUMP_URL}"
    echo "  Taxdump: $(du -sh "${TAXDUMP_TGZ}" | cut -f1)"
    cd "${TAX_DIR}"
    tar -xzf "${TAXDUMP_TGZ}" rankedlineage.dmp 2>/dev/null || tar -xzf "${TAXDUMP_TGZ}"
    cd -
fi

# ── Step 5: annotate FASTA headers with taxonomy ─────────────────────────
echo "=== Building taxonomy-annotated FASTA ==="
python3 - "${RAW_FASTA}" "${TAXID_MAP}" "${LINEAGE_FILE}" "${OUT_FASTA}" << 'PYEOF'
import sys

raw_fasta, taxid_map_f, lineage_f, out_fasta = sys.argv[1:5]

# accession → taxid
acc2taxid = {}
with open(taxid_map_f) as fh:
    for line in fh:
        parts = line.strip().split()
        if len(parts) >= 2:
            acc2taxid[parts[0]] = parts[1]           # full accession.version
            acc2taxid[parts[0].split(".")[0]] = parts[1]  # bare accession

# rankedlineage.dmp columns (pipe-separated):
# taxid | name | species | genus | family | order | class | phylum | kingdom | superkingdom |
print("  Loading rankedlineage.dmp ...", flush=True)
taxid2lin = {}
with open(lineage_f) as fh:
    for line in fh:
        p = [x.strip() for x in line.split("|")]
        if len(p) < 10:
            continue
        taxid   = p[0]
        genus   = p[3]  or ""
        clss    = p[6]  or ""
        phylum  = p[7]  or ""
        kingdom = p[8]  or ""
        superk  = p[9]  or ""
        parts   = [x for x in [superk, kingdom, phylum, clss, genus] if x]
        taxid2lin[taxid] = "; ".join(parts) if parts else "Unknown"
print(f"  {len(taxid2lin):,} taxonomy entries loaded", flush=True)

written = no_tax = 0
with open(raw_fasta) as fin, open(out_fasta, "w") as fout:
    for line in fin:
        line = line.rstrip()
        if line.startswith(">"):
            parts  = line[1:].split(None, 1)
            full_a = parts[0]
            bare_a = full_a.split(".")[0]
            taxid  = acc2taxid.get(full_a) or acc2taxid.get(bare_a)
            if taxid and taxid in taxid2lin:
                lineage = taxid2lin[taxid]
            else:
                title   = parts[1] if len(parts) > 1 else "Unknown"
                lineage = title.split(",")[0].split(" 16S")[0].split(" strain")[0]
                no_tax += 1
            fout.write(f">{full_a} {lineage}\n")
            written += 1
        else:
            fout.write(f"{line}\n")

print(f"  {written} sequences written ({no_tax} fell back to organism name)")
PYEOF

NSEQ=$(grep -c "^>" "${OUT_FASTA}" 2>/dev/null || echo 0)
echo "=== Done: ${NSEQ} sequences | $(du -sh "${OUT_FASTA}" | cut -f1) | ${OUT_FASTA} ==="
echo "  Sample: $(head -1 "${OUT_FASTA}")"
