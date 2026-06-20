#!/usr/bin/env sh
set -eu

REPO_URL="${AI_HIST_REPO_URL:-https://github.com/AgentWorkforce/relayhistory.git}"
REF="${AI_HIST_REF:-main}"
PREFIX="${AI_HIST_PREFIX:-$HOME/.local}"
BIN_DIR="${AI_HIST_BIN_DIR:-$PREFIX/bin}"
INSTALL_DIR="${AI_HIST_INSTALL_DIR:-$PREFIX/share/ai-hist}"
BUILD_PROFILE="${AI_HIST_BUILD_PROFILE:-release}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ai-hist installer: missing required command: $1" >&2
    return 1
  fi
}

script_dir() {
  case "$0" in
    */*) cd "$(dirname "$0")" && pwd ;;
    *) pwd ;;
  esac
}

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

if [ -n "${AI_HIST_SOURCE_DIR:-}" ]; then
  src_dir="$AI_HIST_SOURCE_DIR"
elif [ -f "$(script_dir)/Cargo.toml" ] && [ -f "$(script_dir)/ai-hist" ]; then
  src_dir="$(script_dir)"
else
  need git || {
    echo "Install git or run from a cloned ai-hist checkout." >&2
    exit 1
  }
  src_dir="$tmp_dir/ai-hist"
  git clone --depth 1 --branch "$REF" "$REPO_URL" "$src_dir"
fi

need cargo || {
  echo "Install Rust from https://rustup.rs/ and rerun this script." >&2
  exit 1
}
need python3 || {
  echo "Install Python 3 and rerun this script." >&2
  exit 1
}

if [ "$BUILD_PROFILE" = "release" ]; then
  (cd "$src_dir" && cargo build --release -q -p ai-hist-cli)
else
  (cd "$src_dir" && cargo build -q -p ai-hist-cli)
fi

rust_bin="$src_dir/target/$BUILD_PROFILE/ai-hist"
if [ ! -x "$rust_bin" ]; then
  echo "ai-hist installer: Rust binary was not built at $rust_bin" >&2
  exit 1
fi

mkdir -p "$BIN_DIR" "$INSTALL_DIR"
cp "$src_dir/ai-hist" "$INSTALL_DIR/ai-hist-wrapper"
cp "$src_dir/ai-hist-python" "$INSTALL_DIR/ai-hist-python"
cp "$rust_bin" "$INSTALL_DIR/ai-hist-rust-bin"
chmod 755 "$INSTALL_DIR/ai-hist-wrapper" "$INSTALL_DIR/ai-hist-python" "$INSTALL_DIR/ai-hist-rust-bin"

cat > "$BIN_DIR/ai-hist" <<EOF
#!/usr/bin/env sh
export AI_HIST_RUST_BIN="\${AI_HIST_RUST_BIN:-$INSTALL_DIR/ai-hist-rust-bin}"
exec "$INSTALL_DIR/ai-hist-wrapper" "\$@"
EOF

cat > "$BIN_DIR/ai-hist-python" <<EOF
#!/usr/bin/env sh
exec python3 "$INSTALL_DIR/ai-hist-python" "\$@"
EOF

cat > "$BIN_DIR/ai-hist-rust" <<EOF
#!/usr/bin/env sh
exec "$INSTALL_DIR/ai-hist-rust-bin" "\$@"
EOF

chmod 755 "$BIN_DIR/ai-hist" "$BIN_DIR/ai-hist-python" "$BIN_DIR/ai-hist-rust"

cat <<EOF
ai-hist installed.

Commands:
  $BIN_DIR/ai-hist
  $BIN_DIR/ai-hist-python
  $BIN_DIR/ai-hist-rust

Add this to your shell profile if needed:
  export PATH="$BIN_DIR:\$PATH"
EOF
