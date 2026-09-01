#!/usr/bin/env bash
# Generate a GitHub release body from Conventional Commits / commits.
#
#   gen-release-notes.sh <tag> <output-file>
#     tag    : v1.3.0 (release) or nightly (rolling)
#     output : path to write the markdown body to
#
# Requires git-cliff on PATH and a checkout with full history + tags
# (fetch-depth: 0, fetch-tags: true).
set -euo pipefail

tag="${1:?usage: gen-release-notes.sh <tag> <output-file>}"
out="${2:?missing output file}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
config="${repo_root}/cliff.toml"

# Rolling tag (e.g. nightly) is anything not shaped like vX.Y.Z.
rolling=false
if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  rolling=true
fi

if [[ "$rolling" == "true" ]]; then
  # Rolling: diff from the newest stable tag up to HEAD.
  prev="$(git tag -l "v*" --sort=-v:refname | head -n1)"
  end_ref="HEAD"
  compare_to="$(git rev-parse HEAD)"
else
  # Release: previous stable tag is the greatest v* strictly LESS than
  # the current one by semver (not just "the greatest other tag"). Insert the
  # tag into the sorted list and take the line before it. bash 3.2-safe: no
  # `mapfile` (macOS ships bash 3.2), plain awk instead.
  prev="$( { git tag -l "v*" | grep -vx "$tag"; printf '%s\n' "$tag"; } \
            | sort -V | awk -v t="$tag" '$0 == t { print p; exit } { p = $0 }')"
  end_ref="$tag"
  compare_to="$tag"
fi

if [[ -n "$prev" ]]; then
  range="${prev}..${end_ref}"
else
  # First release: git-cliff rejects a bare tag as a range, so
  # span from the repo's root commit reachable from end_ref up to it.
  root="$(git rev-list --max-parents=0 "$end_ref" 2>/dev/null | tail -1)"
  range="${root}..${end_ref}"
fi

# git-cliff exits non-zero when the range has no releasable commits; capture and
# fall through to the fallback body instead of failing the release.
body=""
if body="$(git cliff "$range" \
      --tag-pattern "^v[0-9]" \
      --tag "$tag" \
      --config "$config" \
      --strip all 2>/dev/null)"; then
  :
else
  body=""
fi

# Compose the compare-link footer from the exact prev/ref we computed.
server_url="${GITHUB_SERVER_URL:-https://github.com}"
repo="${GITHUB_REPOSITORY:-}"
if [[ -z "$repo" ]]; then
  origin="$(git -C "$repo_root" remote get-url origin 2>/dev/null || echo '')"
  repo="$(printf '%s' "$origin" | sed -E 's#^.*[:/]([^/]+/[^/]+)$#\1#; s#\.git$##')"
fi
footer=""
if [[ -n "$prev" && -n "$repo" ]]; then
  footer="**Full Changelog**: ${server_url}/${repo}/compare/${prev}...${compare_to}"
fi

# Fallback when nothing survived filtering. git-cliff still renders a bare
# "## What's Changed" header for an empty commit set (version comes from --tag),
# so detect emptiness by the absence of list items, not overall blankness.
if ! printf '%s' "$body" | grep -q '^- '; then
  body="## What's Changed in ${tag}

_No user-facing changes in this release; see the full changelog below._"
fi

{
  printf '%s\n' "$body"
  if [[ -n "$footer" ]]; then
    printf '\n%s\n' "$footer"
  fi
} > "$out"

echo "Wrote release notes to $out ($(wc -l < "$out") lines)"
