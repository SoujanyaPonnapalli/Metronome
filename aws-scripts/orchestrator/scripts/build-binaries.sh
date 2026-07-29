#!/usr/bin/env bash
# Build the three etcd binaries we need for the eval, in idempotent fashion.
# Intended to run on the orchestrator VM, but works anywhere with Go + git.
#
# Outputs (default /opt/metronome-eval/bin/):
#   etcd-vanilla    : etcd at the merge-base of metronome and main (clean baseline)
#   etcd-metronome  : etcd from the soujanyaponnapalli/etcd metronome branch tip
#                     (also serves --experimental-in-mem-only)
#   etcd-benchmark  : tools/benchmark from the metronome branch
#
# Env knobs:
#   ETCD_REPO          default https://github.com/SoujanyaPonnapalli/etcd.git
#   ETCD_VANILLA_REF   default 3401a41e5   (merge-base of metronome and main)
#   ETCD_METRONOME_REF default origin/metronome
#   BUILD_OUTPUT_DIR   default /opt/metronome-eval/bin
#   GO_VERSION         default the value parsed out of go.mod ("go 1.XX")
#   FORCE              if set, rebuild even if outputs exist
#
# Usage:
#   ./scripts/build-binaries.sh           # build whatever's missing
#   FORCE=1 ./scripts/build-binaries.sh   # always rebuild
set -euo pipefail

ETCD_REPO="${ETCD_REPO:-https://github.com/SoujanyaPonnapalli/etcd.git}"
# Default vanilla baseline is the fork's main tip (where the metronome branch
# diverges from). Override with ETCD_VANILLA_REF=<tag|sha|branch> if you want
# to pin to a specific upstream release for the baseline.
ETCD_VANILLA_REF="${ETCD_VANILLA_REF:-origin/main}"
# Pinned to commit 86ba1a8b7 ("Adding work stealing, configuration changes,
# and fixing bugs") — the canonical metronome drop with FLAG-based control
# (--metronome, --metronome-quorum-size, --experimental-in-mem-only).
# Later commits on origin/metronome (e.g. dc2af11e4) stripped the flag layer
# and hard-wired metronome always-on; that's not usable for this eval matrix.
# The commit lives in the orchestrator's repo as the `metronome-canonical`
# tag (shipped via git bundle since it isn't on origin anymore).
ETCD_METRONOME_REF="${ETCD_METRONOME_REF:-metronome-canonical}"
BUILD_OUTPUT_DIR="${BUILD_OUTPUT_DIR:-/opt/metronome-eval/bin}"
SRC_DIR="${SRC_DIR:-/opt/metronome-eval/src/etcd}"

log() { echo "[build-binaries] $*"; }

# ---- Prereqs ----------------------------------------------------------------
need_root_for() {
  # Returns 0 if we'd need sudo to write to $1's parent.
  local target_parent
  target_parent="$(dirname "$1")"
  [[ ! -w "$target_parent" && ! -w "$1" ]]
}

ensure_dir() {
  local d="$1"
  if [[ -d "$d" && -w "$d" ]]; then return; fi
  if [[ -d "$d" ]]; then return; fi
  if need_root_for "$d"; then
    sudo mkdir -p "$d"
    sudo chown "$USER" "$d"
  else
    mkdir -p "$d"
  fi
}

# Detect the Go version required by go.mod and verify the installed one is >=.
verify_or_install_go() {
  if command -v go >/dev/null 2>&1; then
    local have
    have="$(go env GOVERSION | sed 's/^go//')"
    log "found go ${have}"
    return
  fi

  log "go not found; installing latest stable from go.dev"
  local arch="amd64"
  case "$(uname -m)" in
    aarch64|arm64) arch="arm64" ;;
  esac
  local go_ver="${GO_VERSION:-1.26.0}"
  local tarball="go${go_ver}.linux-${arch}.tar.gz"
  local url="https://go.dev/dl/${tarball}"
  cd /tmp
  curl -fsSL -O "$url"
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf "$tarball"
  rm -f "$tarball"
  export PATH="/usr/local/go/bin:$PATH"
  # Make persistent for future shells.
  if ! grep -q '/usr/local/go/bin' /etc/profile.d/go.sh 2>/dev/null; then
    echo 'export PATH=/usr/local/go/bin:$PATH' | sudo tee /etc/profile.d/go.sh >/dev/null
  fi
  log "installed go $(go env GOVERSION)"
}

ensure_git() {
  command -v git >/dev/null 2>&1 || sudo apt-get update -qq && sudo apt-get install -y git
}

# ---- Source tree ------------------------------------------------------------
ensure_repo() {
  ensure_dir "$(dirname "$SRC_DIR")"
  if [[ -d "$SRC_DIR/.git" ]]; then
    log "fetching latest in $SRC_DIR"
    git -C "$SRC_DIR" fetch --quiet --tags --prune origin
  else
    log "cloning $ETCD_REPO into $SRC_DIR"
    git clone --quiet "$ETCD_REPO" "$SRC_DIR"
    git -C "$SRC_DIR" fetch --quiet --tags --prune origin
  fi
}

# ---- Build helpers ----------------------------------------------------------
checkout_clean() {
  local ref="$1"
  git -C "$SRC_DIR" -c advice.detachedHead=false checkout --quiet "$ref"
  # Wipe stale build artifacts; preserve vendored deps.
  git -C "$SRC_DIR" clean -xdf --quiet -- bin/ 2>/dev/null || rm -rf "$SRC_DIR/bin"
}

build_etcd() {
  # Produces $SRC_DIR/bin/etcd
  ( cd "$SRC_DIR" && BINDIR=bin ./scripts/build.sh )
}

build_benchmark() {
  # Produces $SRC_DIR/bin/benchmark
  ( cd "$SRC_DIR" && BINDIR=bin ./scripts/build_tools.sh )
}

install_as() {
  # install_as <src_in_bin> <dest_basename>
  local src="$SRC_DIR/bin/$1"
  local dst="$BUILD_OUTPUT_DIR/$2"
  [[ -x "$src" ]] || { log "ERROR: $src not built"; exit 4; }
  cp -f "$src" "$dst"
  chmod +x "$dst"
  log "installed $dst (size $(stat -c%s "$dst" 2>/dev/null || stat -f%z "$dst") bytes)"
}

already_built() {
  # Skip if FORCE is unset AND the binary exists.
  [[ -z "${FORCE:-}" && -x "$1" ]]
}

# ---- Main -------------------------------------------------------------------
main() {
  ensure_dir "$BUILD_OUTPUT_DIR"
  ensure_git
  verify_or_install_go
  ensure_repo

  local need_vanilla=1
  local need_metronome=1
  local need_benchmark=1
  already_built "$BUILD_OUTPUT_DIR/etcd-vanilla"   && need_vanilla=0
  already_built "$BUILD_OUTPUT_DIR/etcd-metronome" && need_metronome=0
  already_built "$BUILD_OUTPUT_DIR/etcd-benchmark" && need_benchmark=0

  if (( need_vanilla )); then
    log "==== building etcd-vanilla @ $ETCD_VANILLA_REF ===="
    checkout_clean "$ETCD_VANILLA_REF"
    build_etcd
    install_as etcd etcd-vanilla
  else
    log "etcd-vanilla up to date (set FORCE=1 to rebuild)"
  fi

  if (( need_metronome || need_benchmark )); then
    log "==== building etcd-metronome @ $ETCD_METRONOME_REF ===="
    checkout_clean "$ETCD_METRONOME_REF"
    if (( need_metronome )); then
      build_etcd
      install_as etcd etcd-metronome
    fi
    if (( need_benchmark )); then
      build_benchmark
      # build_tools.sh writes tools to bin/tools/<name> in 3.6+; older layouts
      # use bin/<name>. Resolve here.
      if   [[ -x "$SRC_DIR/bin/tools/benchmark" ]]; then install_as tools/benchmark etcd-benchmark
      elif [[ -x "$SRC_DIR/bin/benchmark"       ]]; then install_as benchmark       etcd-benchmark
      else log "ERROR: benchmark binary not found at expected paths"; exit 4
      fi
    fi
  else
    log "etcd-metronome + etcd-benchmark up to date (set FORCE=1 to rebuild)"
  fi

  log "summary:"
  ls -lh "$BUILD_OUTPUT_DIR"/{etcd-vanilla,etcd-metronome,etcd-benchmark} 2>/dev/null || true
  log "verifying --version smoke:"
  for b in etcd-vanilla etcd-metronome; do
    "$BUILD_OUTPUT_DIR/$b" --version 2>&1 | head -2 | sed "s|^|  $b: |"
  done
  "$BUILD_OUTPUT_DIR/etcd-benchmark" --help 2>&1 | head -1 | sed 's|^|  etcd-benchmark: |'
  log "done."
}

main "$@"
