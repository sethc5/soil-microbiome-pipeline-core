#!/usr/bin/env bash
# =============================================================================
# provision_server.sh — Set up a fresh Hetzner server for the pipeline
# =============================================================================
# Run from the LOCAL machine (not on the server).
# Usage:
#   bash scripts/provision_server.sh [--host HOST] [--key KEYPATH]
#
# Installs Python 3.12, creates deploy user, clones repo, creates venv,
# installs all dependencies including CarveMe/COBRApy/SCIP/GLPK/DIAMOND.
# =============================================================================
set -euo pipefail

HOST="${1:-144.76.222.125}"
KEY="${2:-$HOME/.ssh/id_ed25519}"
SSH="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -i $KEY root@$HOST"
SCP="scp -o BatchMode=yes -o StrictHostKeyChecking=no -i $KEY"

echo "=== Provisioning $HOST ==="

# ── Phase 1: System packages ─────────────────────────────────────────────────
$SSH bash -s <<'REMOTE_SYS'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "--- Phase 1: System packages ---"
apt-get update -qq
apt-get install -y -qq \
  python3 python3-venv python3-dev python3-pip \
  build-essential git curl wget \
  libglpk-dev glpk-utils \
  libxml2-dev libxslt1-dev zlib1g-dev \
  sqlite3 libsqlite3-dev \
  libblas-dev liblapack-dev gfortran \
  cmake swig \
  diamond-aligner 2>/dev/null || true

# DIAMOND may not be in default repos on all Ubuntu versions
if ! command -v diamond &>/dev/null; then
  wget -qO /usr/local/bin/diamond https://github.com/bbuchfink/diamond/releases/download/v2.1.9/diamond-linux64.tar.gz
  cd /tmp && wget -qO- https://github.com/bbuchfink/diamond/releases/download/v2.1.9/diamond-linux64.tar.gz | tar xz
  mv diamond /usr/local/bin/diamond && chmod +x /usr/local/bin/diamond
fi

echo "System packages done. Python: $(python3 --version)"
REMOTE_SYS

# ── Phase 2: Deploy user + directory structure ───────────────────────────────
$SSH bash -s <<'REMOTE_USER'
set -euo pipefail

echo "--- Phase 2: Deploy user + directories ---"
if ! id deploy &>/dev/null; then
  useradd -m -s /bin/bash deploy
  mkdir -p /home/deploy/.ssh
  cp /root/.ssh/authorized_keys /home/deploy/.ssh/
  chown -R deploy:deploy /home/deploy/.ssh
  chmod 700 /home/deploy/.ssh
  echo "deploy ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/deploy
fi

# Pipeline directories
mkdir -p /opt/pipeline /data/pipeline/db /var/log/pipeline /data/pipeline/models /data/pipeline/proteomes
chown -R deploy:deploy /opt/pipeline /data/pipeline /var/log/pipeline
echo "Deploy user and directories ready"
REMOTE_USER

# ── Phase 3: Clone repo + venv ───────────────────────────────────────────────
$SSH bash -s <<'REMOTE_REPO'
set -euo pipefail

echo "--- Phase 3: Clone repo + venv ---"
sudo -u deploy bash -c '
cd /opt/pipeline

# Clone or update
if [ -d .git ]; then
  git pull origin main
else
  git clone https://github.com/sethc5/soil-microbiome-pipeline-core.git .
fi

# Create venv
python3 -m venv .venv
source .venv/bin/activate

# Core dependencies
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

# Metabolic modeling stack
pip install cobra swiglpk
pip install carveme
pip install pyscipopt 2>/dev/null || echo "pyscipopt not available — CarveMe will use SCIP if system-installed"

# Verify installs
python -c "import cobra; print(f\"COBRApy {cobra.__version__}\")"
python -c "import swiglpk; print(\"GLPK solver OK\")"
python -c "
try:
    import cobra
    m = cobra.Model(\"test\")
    m.solver = \"glpk\"
    print(f\"GLPK solver verified in COBRApy\")
except Exception as e:
    print(f\"GLPK test failed: {e}\")
"

echo "Venv ready at /opt/pipeline/.venv"
'
REMOTE_REPO

# ── Phase 4: Initialize database ─────────────────────────────────────────────
$SSH bash -s <<'REMOTE_DB'
set -euo pipefail

echo "--- Phase 4: Initialize database ---"
sudo -u deploy bash -c '
cd /opt/pipeline
source .venv/bin/activate
python -c "
from db_utils import SoilDB
with SoilDB(\"/data/pipeline/db/soil_microbiome.db\") as db:
    tables = [r[0] for r in db.conn.execute(\"SELECT name FROM sqlite_master WHERE type=\\\"table\\\" ORDER BY name\").fetchall()]
    print(f\"Schema initialized — {len(tables)} tables: {tables}\")
"
'
REMOTE_DB

# ── Phase 5: Verify ──────────────────────────────────────────────────────────
$SSH bash -s <<'REMOTE_VERIFY'
set -euo pipefail

echo "--- Phase 5: Verification ---"
sudo -u deploy bash -c '
cd /opt/pipeline
source .venv/bin/activate

echo "Python: $(python --version)"
echo "COBRApy: $(python -c "import cobra; print(cobra.__version__)")"
echo "GLPK: $(python -c "import swiglpk; print(\"OK\")")"
echo "CarveMe: $(which carve 2>/dev/null && carve --version 2>/dev/null || echo "not found")"
echo "DIAMOND: $(diamond version 2>/dev/null || echo "not found")"
echo "DB: $(ls -lh /data/pipeline/db/soil_microbiome.db)"
echo "Disk: $(df -h /data | tail -1)"
echo "CPUs: $(nproc)"
echo "RAM: $(free -h | grep Mem | awk "{print \$2}")"
echo ""
echo "=== Server provisioned and ready ==="
'
REMOTE_VERIFY

echo ""
echo "=== Provisioning complete ==="
echo "To launch pipeline:"
echo "  ssh -i $KEY deploy@$HOST"
echo "  cd /opt/pipeline && source .venv/bin/activate"
echo "  tmux new -s pipeline"
echo "  bash scripts/run_full_pipeline.sh --workers \$(nproc)"
