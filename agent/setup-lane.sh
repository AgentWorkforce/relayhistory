#!/bin/bash
# relayhistory's lane setup, run as root at the end of the kit's setup.sh
# (first boot, and again by every hot update). Idempotent.
#  - Rust stable (rustup, as the agent user, into ~/.cargo, which is on the
#    lead's PATH) with rustfmt and clippy, the toolchain relayhistory's CI
#    uses (rust-version 1.88 or newer).
#  - Both repos in ~/src and ~/build. The kit's load-secrets.sh clones them
#    whenever a GitHub token is available; this retries any that are missing.
set -euo pipefail
# shellcheck source=/dev/null
. /opt/agent-host/env

# A Rust install failure fails the lane (the lead's checks need cargo), unlike
# a clone failure, which load-secrets.sh retries. The installer is downloaded
# to a file first, so a failed download is an error rather than an empty
# script that sh runs successfully.
if ! sudo -H -u agent bash -c '[ -x ~/.cargo/bin/rustup ]'; then
  sudo -H -u agent bash -c 'set -euo pipefail
    installer=$(mktemp)
    trap "rm -f \"$installer\"" EXIT
    curl --proto =https --tlsv1.2 -fsSL https://sh.rustup.rs -o "$installer"
    sh "$installer" -y --profile minimal --default-toolchain stable --component rustfmt,clippy'
fi
# Also on a host where Rust was already installed without them.
sudo -H -u agent bash -c 'set -euo pipefail; ~/.cargo/bin/rustup component add rustfmt clippy'

for repo in ${AGENT_REPOS:-}; do
  name=${repo##*/}
  if [ -s /run/agent-host/gh-token ]; then
    sudo -H -u agent bash -lc "mkdir -p ~/src ~/build
      [ -d ~/src/$name/.git ] || gh repo clone '$repo' ~/src/$name
      [ -d ~/build/$name/.git ] || gh repo clone '$repo' ~/build/$name" \
      || echo "warning: could not clone $repo; load-secrets.sh retries every 30 minutes" >&2
  fi
done
echo "relayhistory lane: setup complete"
