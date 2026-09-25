#!/usr/bin/env bash
# Version checks shared by the image workflows. The fork's version is
# vX.Y.Z-hdx.N, where X.Y.Z is upstream's BASE_VERSION from the Makefile.
#
#   release-version.sh base             print BASE_VERSION, failing unless it is vX.Y.Z
#   release-version.sh check-main       base, plus no regression below the newest release tag
#   release-version.sh check-tag <tag>  a release tag that may ship: see check_tag
#
# check-main and check-tag need full history and tags (checkout fetch-depth: 0).
set -euo pipefail

# -hdx.N: fork build, not semver pre-release
readonly TAG_RE='^v([0-9]+\.[0-9]+\.[0-9]+)-hdx\.[0-9]+$'
readonly BASE_RE='^v[0-9]+\.[0-9]+\.[0-9]+$'

fail() {
  echo "::error::$*" >&2
  exit 1
}

base_version() {
  local base
  base="$(sed -nE 's/^BASE_VERSION[[:space:]]*:?=[[:space:]]*([^[:space:]#]+).*/\1/p' Makefile)"
  [[ -n "$base" ]] || fail "BASE_VERSION not found in Makefile"
  [[ "$base" =~ $BASE_RE ]] || fail "Makefile BASE_VERSION '$base' is not vX.Y.Z"
  echo "$base"
}

check_main() {
  local base newest
  base="$(base_version)"
  newest="$(git tag -l 'v*-hdx.*' |
    sed -nE "s/$TAG_RE/v\1/p" |
    sort -V | tail -n 1)"
  if [[ -n "$newest" && "$(printf '%s\n%s\n' "$newest" "$base" | sort -V | tail -n 1)" != "$base" ]]; then
    fail "Makefile BASE_VERSION $base is lower than the newest release base $newest"
  fi
  echo "$base"
}

# A tag may ship only if it is vX.Y.Z-hdx.N, annotated with a non-empty
# message, on main, and its X.Y.Z matches the Makefile at the tagged commit.
check_tag() {
  local tag="$1" tag_base base commit
  [[ "$tag" =~ $TAG_RE ]] || fail "tag '$tag' is not vX.Y.Z-hdx.N"
  tag_base="v${BASH_REMATCH[1]}"

  # actions/checkout can leave an annotated tag as a lightweight ref, so fetch
  # the tag object itself before inspecting it.
  git fetch --quiet --force origin "refs/tags/$tag:refs/tags/$tag"
  [[ "$(git cat-file -t "refs/tags/$tag")" == "tag" ]] ||
    fail "tag $tag is lightweight; release tags must be annotated (git tag -a)"
  [[ -n "$(git tag -l --format='%(contents)' "$tag" | tr -d '[:space:]')" ]] ||
    fail "tag $tag has an empty annotation; release tags need a message describing the release"

  commit="$(git rev-parse "refs/tags/$tag^{commit}")"
  git merge-base --is-ancestor "$commit" origin/main ||
    fail "tag $tag points to $commit, which is not on main"

  base="$(base_version)"
  [[ "$tag_base" == "$base" ]] ||
    fail "tag $tag does not match Makefile BASE_VERSION $base"
}

case "${1:-}" in
  base) base_version ;;
  check-main) check_main ;;
  check-tag)
    [[ $# -eq 2 ]] || fail "usage: $0 check-tag <tag>"
    check_tag "$2"
    ;;
  *) fail "usage: $0 base | check-main | check-tag <tag>" ;;
esac
