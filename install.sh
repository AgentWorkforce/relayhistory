#!/usr/bin/env sh
set -eu

REPO_SLUG="${AI_HIST_REPO_SLUG:-AgentWorkforce/relayhistory}"
REPO_URL="${AI_HIST_REPO_URL:-https://github.com/$REPO_SLUG.git}"
REF="${AI_HIST_REF:-main}"
VERSION="${AI_HIST_VERSION:-latest}"
PREFIX="${AI_HIST_PREFIX:-$HOME/.local}"
BIN_DIR="${AI_HIST_BIN_DIR:-$PREFIX/bin}"
INSTALL_DIR="${AI_HIST_INSTALL_DIR:-$PREFIX/share/ai-hist}"
BUILD_PROFILE="${AI_HIST_BUILD_PROFILE:-release}"
INSTALL_METHOD="${AI_HIST_INSTALL_METHOD:-auto}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ai-hist installer: missing required command: $1" >&2
    return 1
  fi
}

info() {
  echo "ai-hist installer: $*"
}

warn() {
  echo "ai-hist installer: $*" >&2
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

case "$INSTALL_METHOD" in
  auto | binary | source) ;;
  *)
    echo "ai-hist installer: AI_HIST_INSTALL_METHOD must be auto, binary, or source" >&2
    exit 2
    ;;
esac

platform_asset() {
  os="$(uname -s 2>/dev/null || echo unknown)"
  arch="$(uname -m 2>/dev/null || echo unknown)"

  case "$os" in
    Darwin) os_part="darwin" ;;
    Linux) os_part="linux" ;;
    *)
      warn "unsupported OS for prebuilt binary: $os"
      return 1
      ;;
  esac

  case "$arch" in
    arm64 | aarch64) arch_part="arm64" ;;
    x86_64 | amd64) arch_part="x64" ;;
    *)
      warn "unsupported architecture for prebuilt binary: $arch"
      return 1
      ;;
  esac

  echo "ai-hist-$os_part-$arch_part"
}

release_download_url() {
  asset="$1"
  if [ -n "${AI_HIST_BINARY_URL:-}" ]; then
    echo "$AI_HIST_BINARY_URL"
    return 0
  fi

  if [ "$VERSION" = "latest" ]; then
    echo "https://github.com/$REPO_SLUG/releases/latest/download/$asset"
    return 0
  fi

  case "$VERSION" in
    v*) tag="$VERSION" ;;
    *) tag="v$VERSION" ;;
  esac
  echo "https://github.com/$REPO_SLUG/releases/download/$tag/$asset"
}

raw_ref() {
  if [ -n "${AI_HIST_RAW_REF:-}" ]; then
    echo "$AI_HIST_RAW_REF"
    return 0
  fi

  if [ "$VERSION" != "latest" ]; then
    case "$VERSION" in
      v*) echo "$VERSION" ;;
      *) echo "v$VERSION" ;;
    esac
    return 0
  fi

  echo "$REF"
}

source_ref() {
  if [ -n "${AI_HIST_SOURCE_REF:-}" ]; then
    echo "$AI_HIST_SOURCE_REF"
    return 0
  fi

  raw_ref
}

install_binary_launchers() {
  rust_bin="$1"

  mkdir -p "$BIN_DIR" "$INSTALL_DIR"

  cat > "$BIN_DIR/ai-hist" <<EOF
#!/usr/bin/env sh
if [ "\${AI_HIST_CLI:-auto}" = "python" ]; then
  exec "$BIN_DIR/ai-hist-python" "\$@"
fi
exec "\${AI_HIST_RUST_BIN:-$rust_bin}" "\$@"
EOF

  cat > "$BIN_DIR/ai-hist-rust" <<EOF
#!/usr/bin/env sh
exec "\${AI_HIST_RUST_BIN:-$rust_bin}" "\$@"
EOF

  cat > "$BIN_DIR/ai-hist-python" <<EOF
#!/usr/bin/env sh
if [ ! -f "$INSTALL_DIR/ai-hist-python" ]; then
  echo "ai-hist-python was not installed with this ai-hist binary install." >&2
  exit 127
fi
exec python3 "$INSTALL_DIR/ai-hist-python" "\$@"
EOF

  chmod 755 "$BIN_DIR/ai-hist" "$BIN_DIR/ai-hist-rust" "$BIN_DIR/ai-hist-python"
}

install_python_fallback() {
  mkdir -p "$INSTALL_DIR"

  if [ -n "${AI_HIST_WRAPPER_SOURCE_DIR:-}" ] && [ -f "$AI_HIST_WRAPPER_SOURCE_DIR/ai-hist-python" ]; then
    cp "$AI_HIST_WRAPPER_SOURCE_DIR/ai-hist-python" "$INSTALL_DIR/ai-hist-python"
    chmod 755 "$INSTALL_DIR/ai-hist-python"
    return 0
  fi

  if [ -f "$(script_dir)/ai-hist-python" ]; then
    cp "$(script_dir)/ai-hist-python" "$INSTALL_DIR/ai-hist-python"
    chmod 755 "$INSTALL_DIR/ai-hist-python"
    return 0
  fi

  need curl || return 1
  ref="$(raw_ref)"
  url="https://raw.githubusercontent.com/$REPO_SLUG/$ref/ai-hist-python"
  if curl -fsSL "$url" -o "$INSTALL_DIR/ai-hist-python" 2>/dev/null; then
    chmod 755 "$INSTALL_DIR/ai-hist-python"
    return 0
  fi

  warn "could not install ai-hist-python fallback from $url"
  return 1
}

install_prebuilt() {
  need curl || return 1

  if [ -n "${AI_HIST_BINARY_URL:-}" ]; then
    asset="ai-hist-custom"
    url="$AI_HIST_BINARY_URL"
  else
    asset="$(platform_asset)" || return 1
    url="$(release_download_url "$asset")"
  fi
  rust_bin="$INSTALL_DIR/ai-hist-rust-bin"
  download="$tmp_dir/$asset"

  info "downloading prebuilt $asset"
  mkdir -p "$INSTALL_DIR"
  if ! curl -fsSL "$url" -o "$download"; then
    warn "prebuilt binary not available at $url"
    return 1
  fi

  cp "$download" "$rust_bin"
  chmod 755 "$rust_bin"

  if ! "$rust_bin" --version >/dev/null 2>&1; then
    warn "downloaded binary failed verification"
    rm -f "$rust_bin"
    return 1
  fi

  install_binary_launchers "$rust_bin"
  install_python_fallback || true
  info "installed prebuilt binary"
  return 0
}

resolve_source_dir() {
  if [ -n "${AI_HIST_SOURCE_DIR:-}" ]; then
    echo "$AI_HIST_SOURCE_DIR"
    return 0
  fi

  if [ -f "$(script_dir)/Cargo.toml" ] && [ -f "$(script_dir)/ai-hist" ]; then
    echo "$(script_dir)"
    return 0
  fi

  need git || {
    echo "Install git or run from a cloned ai-hist checkout." >&2
    return 1
  }
  src_dir="$tmp_dir/ai-hist"
  git clone --depth 1 --branch "$(source_ref)" "$REPO_URL" "$src_dir"
  echo "$src_dir"
}

install_from_source() {
  src_dir="$(resolve_source_dir)" || exit 1

  need cargo || {
    echo "Install Rust from https://rustup.rs/ and rerun this script, or use AI_HIST_INSTALL_METHOD=binary with a published prebuilt binary." >&2
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
  info "installed from source"
}

if [ "$INSTALL_METHOD" = "binary" ]; then
  install_prebuilt || exit 1
elif [ "$INSTALL_METHOD" = "source" ]; then
  install_from_source
elif [ -z "${AI_HIST_SOURCE_DIR:-}" ] && ! { [ -f "$(script_dir)/Cargo.toml" ] && [ -f "$(script_dir)/ai-hist" ]; }; then
  install_prebuilt || install_from_source
else
  install_from_source
fi

cat <<EOF
ai-hist installed.

Commands:
  $BIN_DIR/ai-hist
  $BIN_DIR/ai-hist-python
  $BIN_DIR/ai-hist-rust

Add this to your shell profile if needed:
  export PATH="$BIN_DIR:\$PATH"
EOF
