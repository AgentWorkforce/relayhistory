#!/usr/bin/env bash
# Replay a version-only commit onto the current tip of BRANCH.
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
set -euo pipefail

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
    git rebase --onto "origin/$BRANCH" "$START_SHA" || conflict
  fi
  if git push origin "HEAD:refs/heads/$BRANCH"; then
    exit 0
  fi
  echo "Push of the version commit was rejected (attempt $attempt/$ATTEMPTS)"
  attempt=$((attempt + 1))
done

echo "Could not persist the version commit onto $BRANCH after $ATTEMPTS attempts." >&2
exit 1
