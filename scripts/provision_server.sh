#!/usr/bin/env bash
# =============================================================================
# provision_server.sh — Full Hetzner server provisioning for pipeline-core
# =============================================================================
# Run from the LOCAL machine (not on the server).  Requires root SSH access
# to the freshly-installed OS.  After the script completes, root SSH is
# disabled and all future access is through the deploy user.
#
# Usage:
#   bash scripts/provision_server.sh [HOST] [KEY]
#
# Defaults:
#   HOST = 144.76.222.125
#   KEY  = ~/.ssh/id_ed25519_personal
#
# What this script does (in order):
#   Phase 1  — Create deploy user, sudo NOPASSWD, copy SSH key
#   Phase 2  — SSH hardening (root disabled, password auth off)
#   Phase 3  — UFW firewall + fail2ban + unattended-upgrades
#   Phase 4  — System packages (Python, build tools, GLPK, CMake, SWIG, DIAMOND 2.1.9)
#   Phase 5  — Multi-project directory layout
#   Phase 6  — Per-project ed25519 deploy keys + SSH config
#   Phase 7  — Clone pipeline repo
#   Phase 8  — Python venv + all Python dependencies
#   Phase 9  — COBRApy / GLPK / CarveMe / DIAMOND verification
#   Phase 10 — Initialise SQLite database
#   Phase 11 — Symlinks (results → /data/pipeline/results, receipts → …)
#   Phase 12 — systemd pipeline-batch@.service template
#   Phase 13 — .bashrc aliases + MOTD
#   Phase 14 — Set git remote to SSH deploy key
#   Phase 15 — Final verification
# =============================================================================
set -euo pipefail

# Load .env if it exists (never committed — see .gitignore)
ENV_FILE="$(dirname "$0")/../.env"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
fi

HOST="${1:-${HETZNER2_HOST:-144.76.222.125}}"
KEY="${2:-${HETZNER2_KEY:-$HOME/.ssh/id_ed25519_personal}}"

SSH_ROOT="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -i $KEY root@$HOST"
SSH_DEP="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -i $KEY deploy@$HOST"

LOCAL_PUBKEY="$(cat "${KEY}.pub")"

echo "================================================================"
echo " Provisioning $HOST"
echo " Local pub key: ${KEY}.pub"
echo "================================================================"

# ── Phase 1: Deploy user ──────────────────────────────────────────────────────
echo ""
echo "--- Phase 1: Create deploy user ---"
$SSH_ROOT bash -s <<REMOTE
set -euo pipefail

if ! id deploy &>/dev/null; then
  useradd -m -s /bin/bash deploy
fi

mkdir -p /home/deploy/.ssh
chmod 700 /home/deploy/.ssh

if [ -f /root/.ssh/authorized_keys ]; then
  cp /root/.ssh/authorized_keys /home/deploy/.ssh/authorized_keys
fi
echo "$LOCAL_PUBKEY" >> /home/deploy/.ssh/authorized_keys
sort -u /home/deploy/.ssh/authorized_keys -o /home/deploy/.ssh/authorized_keys
chmod 600 /home/deploy/.ssh/authorized_keys
chown -R deploy:deploy /home/deploy/.ssh

echo "deploy ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/deploy
chmod 440 /etc/sudoers.d/deploy
echo "deploy user ready"
REMOTE

# ── Pre-flight: verify deploy key works BEFORE locking root ──────────────────
echo ""
echo "--- Pre-flight: verifying deploy key access ---"
if ! $SSH_DEP 'echo OK' 2>/dev/null | grep -q OK; then
  echo "ERROR: Cannot log in as deploy with $KEY" >&2
  echo "  Deploy user may not have the key yet, or sshd needs a moment." >&2
  echo "  Waiting 5s and retrying..." >&2
  sleep 5
  if ! $SSH_DEP 'echo OK' 2>/dev/null | grep -q OK; then
    echo "  Still failing — aborting before SSH hardening to avoid lockout." >&2
    exit 1
  fi
fi
echo "Deploy key login: OK — safe to harden root"

# ── Phase 2: SSH hardening ────────────────────────────────────────────────────
echo ""
echo "--- Phase 2: SSH hardening ---"
$SSH_ROOT bash -s <<'REMOTE'
set -euo pipefail
cat > /etc/ssh/sshd_config.d/99-hardened.conf <<'EOF'
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
AllowUsers deploy
MaxAuthTries 3
ClientAliveInterval 300
ClientAliveCountMax 2
X11Forwarding no
PrintLastLog yes
EOF
systemctl reload sshd
echo "SSH hardened"
REMOTE

# ── Phase 3: UFW + fail2ban + unattended-upgrades ─────────────────────────────
echo ""
echo "--- Phase 3: Firewall + fail2ban ---"
$SSH_DEP sudo bash -s <<'REMOTE'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get install -y -qq ufw fail2ban unattended-upgrades

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

cat > /etc/fail2ban/jail.d/sshd.local <<'EOF'
[sshd]
enabled  = true
port     = 22
filter   = sshd
maxretry = 5
findtime = 600
bantime  = 3600
EOF
systemctl enable fail2ban
systemctl restart fail2ban

cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
EOF
echo "Firewall + fail2ban done"
REMOTE

# ── Phase 4: System packages ──────────────────────────────────────────────────
echo ""
echo "--- Phase 4: System packages ---"
$SSH_DEP sudo bash -s <<'REMOTE'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  python3 python3-venv python3-dev python3-pip \
  build-essential git curl wget \
  libglpk-dev glpk-utils \
  libxml2-dev libxslt1-dev zlib1g-dev \
  sqlite3 libsqlite3-dev \
  libblas-dev liblapack-dev gfortran \
  cmake swig \
  tmux htop vim jq

# DIAMOND 2.1.9 from GitHub release (apt version is too old)
if ! diamond version 2>/dev/null | grep -q '2\.1'; then
  cd /tmp
  wget -qO diamond-linux64.tar.gz \
    https://github.com/bbuchfink/diamond/releases/download/v2.1.9/diamond-linux64.tar.gz
  tar xzf diamond-linux64.tar.gz
  mv diamond /usr/local/bin/diamond
  chmod +x /usr/local/bin/diamond
fi
diamond version
echo "System packages done"
REMOTE

# ── Phase 5: Directory layout ─────────────────────────────────────────────────
echo ""
echo "--- Phase 5: Directory layout ---"
$SSH_DEP sudo bash -s <<'REMOTE'
set -euo pipefail
for proj in pipeline cytools meridian; do
  mkdir -p \
    /opt/$proj \
    /data/$proj/db \
    /data/$proj/results \
    /data/$proj/receipts \
    /data/$proj/models \
    /data/$proj/proteomes \
    /var/log/$proj
  chown -R deploy:deploy /opt/$proj /data/$proj /var/log/$proj
done
echo "Directory layout ready"
REMOTE

# ── Phase 6: Deploy keys ──────────────────────────────────────────────────────
echo ""
echo "--- Phase 6: Per-project deploy keys ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
mkdir -p ~/.ssh/deploy_keys
chmod 700 ~/.ssh/deploy_keys

for proj in pipeline cytools meridian; do
  KEY=~/.ssh/deploy_keys/${proj}_deploy
  if [ ! -f "$KEY" ]; then
    ssh-keygen -t ed25519 -C "deploy@pipeline-core/${proj}" -f "$KEY" -N ""
  fi
  echo "=== ${proj} deploy public key ==="
  cat "${KEY}.pub"
done

CONF=~/.ssh/config
touch "$CONF"
for proj in pipeline cytools meridian; do
  if ! grep -q "Host github-${proj}" "$CONF" 2>/dev/null; then
    cat >> "$CONF" <<EOF

Host github-${proj}
  HostName github.com
  User git
  IdentityFile ~/.ssh/deploy_keys/${proj}_deploy
  IdentitiesOnly yes
  StrictHostKeyChecking no
EOF
  fi
done
chmod 600 "$CONF"
echo "Deploy keys and SSH config ready"
REMOTE

# ── Phase 7: Clone repo ───────────────────────────────────────────────────────
echo ""
echo "--- Phase 7: Clone pipeline repo ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
if [ -d /opt/pipeline/.git ]; then
  echo "Repo already cloned — pulling latest"
  cd /opt/pipeline && git pull origin main
else
  git clone https://github.com/sethc5/soil-microbiome-pipeline-core.git /opt/pipeline
fi
echo "Repo at $(cd /opt/pipeline && git rev-parse --short HEAD)"
REMOTE

# ── Phase 8: Python venv + dependencies ──────────────────────────────────────
echo ""
echo "--- Phase 8: Python venv + dependencies ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
cd /opt/pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
pip install cobra swiglpk carveme
echo "Python venv ready. $(python --version)"
REMOTE

# ── Phase 9: Tool verification ────────────────────────────────────────────────
echo ""
echo "--- Phase 9: Tool verification ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
cd /opt/pipeline && source .venv/bin/activate
python - <<'PYEOF'
import cobra, swiglpk
m = cobra.Model("t")
print(f"COBRApy {cobra.__version__} — GLPK OK")
PYEOF
carve --version 2>/dev/null || true
diamond version || true
REMOTE

# ── Phase 10: Database ────────────────────────────────────────────────────────
echo ""
echo "--- Phase 10: Initialise database ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
cd /opt/pipeline && source .venv/bin/activate
python - <<'PYEOF'
from db_utils import SoilDB
with SoilDB("/data/pipeline/db/soil_microbiome.db") as db:
    tables = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print(f"DB initialized — {len(tables)} tables: {tables}")
PYEOF
REMOTE

# ── Phase 11: Symlinks ────────────────────────────────────────────────────────
echo ""
echo "--- Phase 11: Symlinks ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
cd /opt/pipeline
for d in results receipts; do
  rm -rf "$d"
  ln -sfn /data/pipeline/$d $d
done
ls -la results receipts
REMOTE

# ── Phase 12: systemd service template ───────────────────────────────────────
echo ""
echo "--- Phase 12: systemd pipeline-batch@.service ---"
$SSH_DEP sudo bash -s <<'REMOTE'
set -euo pipefail
cat > /etc/systemd/system/pipeline-batch@.service <<'EOF'
[Unit]
Description=Pipeline batch job — %i
After=network.target

[Service]
Type=simple
User=deploy
WorkingDirectory=/opt/pipeline
Environment="PATH=/opt/pipeline/.venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/opt/pipeline/.venv/bin/python -m pipeline_core %i
StandardOutput=append:/var/log/pipeline/%i.log
StandardError=append:/var/log/pipeline/%i.err
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
echo "systemd template installed"
REMOTE

# ── Phase 13: .bashrc aliases + MOTD ─────────────────────────────────────────
echo ""
echo "--- Phase 13: Aliases + MOTD ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
if ! grep -q '# pipeline-core aliases' ~/.bashrc 2>/dev/null; then
  cat >> ~/.bashrc <<'BASHRC'

# pipeline-core aliases
alias pipe='cd /opt/pipeline && source .venv/bin/activate'
alias cyt='cd /opt/cytools && source .venv/bin/activate'
alias mer='cd /opt/meridian && source .venv/bin/activate'
alias logs-pipe='tail -f /var/log/pipeline/*.log'
alias disk='df -h /data /opt'
BASHRC
fi

sudo bash -c 'cat > /etc/motd <<EOF
╔═══════════════════════════════════════════════════════╗
║  pipeline-core  •  soil microbiome pipeline           ║
║  /opt/pipeline  •  /data/pipeline                     ║
║  alias: pipe   logs: /var/log/pipeline/               ║
╚═══════════════════════════════════════════════════════╝
EOF'
echo "Aliases and MOTD done"
REMOTE

# ── Phase 14: Git remote → SSH deploy key ─────────────────────────────────────
echo ""
echo "--- Phase 14: git remote via deploy key ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
ssh-keyscan -H github.com >> ~/.ssh/known_hosts 2>/dev/null || true
cd /opt/pipeline
git remote set-url origin github-pipeline:sethc5/soil-microbiome-pipeline-core.git
git fetch && echo "git SSH remote: OK"
REMOTE

# ── Phase 15: Final verification ──────────────────────────────────────────────
echo ""
echo "--- Phase 15: Final verification ---"
$SSH_DEP bash -s <<'REMOTE'
set -euo pipefail
echo "=== System ==="
echo "  OS:     $(lsb_release -ds)"
echo "  Kernel: $(uname -r)"
echo "  CPUs:   $(nproc)"
echo "  RAM:    $(free -h | awk '/Mem:/{print $2}')"
echo "  Disk:   $(df -h /data | tail -1 | awk '{print $4, "free"}')"
echo ""
echo "=== Security ==="
grep PermitRootLogin /etc/ssh/sshd_config.d/99-hardened.conf || true
sudo ufw status | head -3
sudo systemctl is-active fail2ban
echo ""
echo "=== Tools ==="
cd /opt/pipeline && source .venv/bin/activate
echo "  Python:  $(python --version)"
python -c "import cobra; print(f'  COBRApy: {cobra.__version__}')"
python -c "import swiglpk; print('  GLPK:    OK')"
(carve --version 2>&1 || true) | head -1
diamond version 2>&1 | head -1 || true
echo ""
echo "=== Repo ==="
cd /opt/pipeline
echo "  Commit: $(git rev-parse --short HEAD)"
echo "  Branch: $(git branch --show-current)"
echo ""
echo "=== Database ==="
python - <<'PYEOF'
from db_utils import SoilDB
with SoilDB("/data/pipeline/db/soil_microbiome.db") as db:
    tables = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print(f"  Tables ({len(tables)}): {tables}")
PYEOF
echo ""
echo "================================================================"
echo " Provisioning complete."
echo " Add deploy key to GitHub → Settings → Deploy Keys (read-only):"
cat ~/.ssh/deploy_keys/pipeline_deploy.pub
echo "================================================================"
REMOTE

echo ""
echo "Done. To start the pipeline:"
echo "  ssh -i $KEY deploy@$HOST"
echo "  pipe"
echo "  tmux new -s pipeline"
echo "  bash scripts/run_full_pipeline.sh --workers \$(nproc)"
