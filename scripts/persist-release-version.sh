#!/usr/bin/env bash
# Replay the version commit onto the current tip of BRANCH.
#
# Usage: persist-release-version.sh BRANCH START_SHA
#
# HEAD is START_SHA plus the version commit, or START_SHA itself when the
# manifests were already at the release version. origin/BRANCH is fetched and
# the version commit is rebased onto it so a merge that landed during publish
# does not drop the persist. Each push retries from that original version
# commit: a second merge between fetch and push is another rebase, not a
# failed persist. The published tree is assumed to already be tagged; this
# script only updates the branch.
#
# When BRANCH's CHANGELOG.md moved since START_SHA, entries landed while the
# release ran. Whether git merges them cleanly (possibly into the released
# section) or conflicts only in CHANGELOG.md, the changelog is rebuilt from
# BRANCH's copy: the released section exactly as tagged, and the entries BRANCH
# gained since START_SHA kept pending (they are not in the release). Any other
# conflict fails closed.
set -euo pipefail

SCRIPTS=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ $# -ne 2 ]]; then
  echo "Usage: persist-release-version.sh BRANCH START_SHA" >&2
  exit 1
fi

BRANCH=$1
START_SHA=$2
ATTEMPTS="${PERSIST_PUSH_ATTEMPTS:-5}"
VERSION_SHA=$(git rev-parse HEAD)

git fetch origin "$BRANCH"
REMOTE_SHA=$(git rev-parse "origin/$BRANCH")

if [[ "$VERSION_SHA" == "$START_SHA" ]]; then
  echo "Version metadata is already on $START_SHA; not rewriting $BRANCH"
  if [[ "$REMOTE_SHA" != "$START_SHA" ]]; then
    echo "origin/$BRANCH advanced to $REMOTE_SHA; leaving it in place"
  fi
  exit 0
fi

# Rebuild CHANGELOG.md in the working tree from BRANCH's copy: the released
# section exactly as tagged, and the entries BRANCH gained since START_SHA kept
# pending. Called where set -e does not apply, so every step returns its own
# failure.
carry_changelog() {
  if [[ -z "${VERSION:-}" ]]; then
    echo "CHANGELOG.md changed on origin/$BRANCH during the release, and VERSION is unset; cannot carry the cut" >&2
    return 1
  fi
  echo "origin/$BRANCH changed CHANGELOG.md during the release; carrying the $VERSION cut onto it"
  git show "origin/$BRANCH:CHANGELOG.md" > CHANGELOG.md || return 1
  node "$SCRIPTS/cut-changelog.mjs" --version "$VERSION" \
    --released-from "$VERSION_SHA" --pending-since "$START_SHA" || return 1
  git add CHANGELOG.md
}

# A conflict only in CHANGELOG.md: resolve it with the carried changelog, so
# the replayed commit keeps the release cut (and is never empty).
resolve_changelog_conflict() {
  [[ "$(git diff --name-only --diff-filter=U)" == "CHANGELOG.md" ]] || return 1
  echo "CHANGELOG.md conflicts with origin/$BRANCH"
  carry_changelog || return 1
  GIT_EDITOR=true git rebase --continue
}

# After a clean rebase, git may have merged entries that landed during the
# release into the released section: rebuild whenever both sides changed it.
reconcile_changelog() {
  git diff --quiet "$START_SHA" "$VERSION_SHA" -- CHANGELOG.md && return 0
  git diff --quiet "$START_SHA" "origin/$BRANCH" -- CHANGELOG.md && return 0
  carry_changelog || return 1
  git diff --cached --quiet || git commit --quiet --amend --no-edit || return 1
}

conflict() {
  [[ "$(git diff --name-only --diff-filter=U)" == "CHANGELOG.md" ]] || return 1
  echo "CHANGELOG.md conflicts with origin/$BRANCH; taking its copy"
  git checkout --ours -- CHANGELOG.md
  git add CHANGELOG.md
  GIT_EDITOR=true git rebase --continue
}

# Called as `reconcile_changelog || conflict`, where set -e does not apply,
# so every step returns its own failure.
reconcile_changelog() {
  git diff --quiet "$START_SHA" "$VERSION_SHA" -- CHANGELOG.md && return 0
  git diff --quiet "$START_SHA" "origin/$BRANCH" -- CHANGELOG.md && return 0
  echo "origin/$BRANCH changed CHANGELOG.md during the release; carrying the $VERSION cut onto it"
  git show "origin/$BRANCH:CHANGELOG.md" > CHANGELOG.md || return 1
  node "$SCRIPTS/cut-changelog.mjs" --version "$VERSION" \
    --released-from "$VERSION_SHA" --pending-since "$START_SHA" || return 1
  git add CHANGELOG.md || return 1
  git diff --cached --quiet || git commit --quiet --amend --no-edit || return 1
}

conflict() {
  echo "Version commit does not apply cleanly onto origin/$BRANCH." >&2
  echo "The published tree remains at $VERSION_SHA." >&2
  if [[ -n "${VERSION:-}" ]]; then
    echo "It is tagged sdk-ts-v$VERSION. Cherry-pick that commit onto $BRANCH; skip_core=true custom_version=$VERSION finishes crate/plugins if they still need this version." >&2
  fi
  git rebase --abort || true
  git reset --hard "$VERSION_SHA"
  exit 1
}

attempt=1
while (( attempt <= ATTEMPTS )); do
  if (( attempt > 1 )); then
    git fetch origin "$BRANCH"
    REMOTE_SHA=$(git rev-parse "origin/$BRANCH")
    git reset --hard "$VERSION_SHA"
  fi
  if [[ "$REMOTE_SHA" != "$START_SHA" ]]; then
    echo "origin/$BRANCH advanced from $START_SHA to $REMOTE_SHA; rebasing the version commit"
    git rebase --onto "origin/$BRANCH" "$START_SHA" || resolve_changelog_conflict || conflict
    reconcile_changelog || conflict
  fi
  if git push origin "HEAD:refs/heads/$BRANCH"; then
    exit 0
  fi
  echo "Push of the version commit was rejected (attempt $attempt/$ATTEMPTS)"
  attempt=$((attempt + 1))
done

echo "Could not persist the version commit onto $BRANCH after $ATTEMPTS attempts." >&2
exit 1
