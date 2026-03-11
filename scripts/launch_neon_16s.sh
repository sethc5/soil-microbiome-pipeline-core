#!/bin/bash
pkill -f process_neon_16s 2>/dev/null
sleep 1
cd /opt/pipeline
nohup /home/deploy/miniforge3/envs/bioinfo/bin/python scripts/process_neon_16s.py \
    --db /data/pipeline/db/soil_microbiome.db \
    --staging /data/pipeline/staging/neon_16s \
    --silva /data/pipeline/ref/16S_ref.fasta \
    --all-sites --workers 6 \
    >> logs/neon_16s_20260309.log 2>&1 &
echo "Started PID=$!"
