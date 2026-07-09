#!/usr/bin/env bash
set -euo pipefail

REPO="webzfs/webzfs"
OUTDIR="pr-diffs"
DEPENDABOT_ONLY=0
AUTHOR_FILTER=""
INCLUDE_DRAFTS=1
FORCE=0
CHECK_REPO=""
MAX_PAGES=20
API_BASE="${GITHUB_API_URL:-https://api.github.com}"

usage() {
    cat <<'USAGE'
download-open-pr-diffs.sh

Downloads .diff files for currently open GitHub pull requests.

Default target:
  webzfs/webzfs

Requirements:
  bash
  curl
  python3

Optional:
  git, only if using --check

Authentication:
  Public repos usually work without a token.
  To avoid rate limits or access private repos, set:

    export GITHUB_TOKEN=...

Usage:
  ./download-open-pr-diffs.sh
  ./download-open-pr-diffs.sh --repo webzfs/webzfs
  ./download-open-pr-diffs.sh --dependabot-only
  ./download-open-pr-diffs.sh --author "dependabot[bot]"
  ./download-open-pr-diffs.sh --out /tmp/webzfs-pr-diffs
  ./download-open-pr-diffs.sh --check /path/to/local/repo

Options:
  --repo OWNER/REPO       GitHub repository to query.
  --out DIR              Output directory for downloaded diffs.
  --dependabot-only      Only download PRs authored by dependabot[bot].
  --author USERNAME      Only download PRs from a specific GitHub login.
  --no-drafts            Skip draft PRs.
  --force                Re-download diffs even if files already exist.
  --check REPO_PATH      Run git apply --check for each diff in this local repo.
                         This checks against the repo's currently checked-out branch.
  --max-pages N          Maximum API pages to scan. Default: 20.
  -h, --help             Show this help.

Output:
  DIR/diffs/*.diff
  DIR/manifest.tsv
  DIR/manifest.md
  DIR/prs.jsonl

Notes:
  This script discovers open PRs at runtime. It is not tied to old PR numbers.
  It downloads whatever PRs are open when you run it.
USAGE
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

shell_quote() {
    python3 - "$1" <<'PY'
import shlex
import sys
print(shlex.quote(sys.argv[1]))
PY
}

abs_path() {
    python3 - "$1" <<'PY'
import os
import sys
print(os.path.abspath(sys.argv[1]))
PY
}

api_headers_json=(
    -H "Accept: application/vnd.github+json"
    -H "X-GitHub-Api-Version: 2022-11-28"
    -H "User-Agent: open-pr-diff-downloader"
)

api_headers_diff=(
    -H "Accept: application/vnd.github.v3.diff"
    -H "X-GitHub-Api-Version: 2022-11-28"
    -H "User-Agent: open-pr-diff-downloader"
)

add_auth_headers() {
    if [ -n "${GITHUB_TOKEN:-}" ]; then
        api_headers_json+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
        api_headers_diff+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --repo)
            [ "$#" -ge 2 ] || die "--repo requires OWNER/REPO"
            REPO="$2"
            shift 2
            ;;
        --out)
            [ "$#" -ge 2 ] || die "--out requires a directory"
            OUTDIR="$2"
            shift 2
            ;;
        --dependabot-only)
            DEPENDABOT_ONLY=1
            AUTHOR_FILTER="dependabot[bot]"
            shift
            ;;
        --author)
            [ "$#" -ge 2 ] || die "--author requires a GitHub login"
            AUTHOR_FILTER="$2"
            shift 2
            ;;
        --no-drafts)
            INCLUDE_DRAFTS=0
            shift
            ;;
        --force)
            FORCE=1
            shift
            ;;
        --check)
            [ "$#" -ge 2 ] || die "--check requires a local git repo path"
            CHECK_REPO="$2"
            shift 2
            ;;
        --max-pages)
            [ "$#" -ge 2 ] || die "--max-pages requires a number"
            MAX_PAGES="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "Unknown option: $1"
            ;;
    esac
done

need_cmd curl
need_cmd python3

if [ -n "$CHECK_REPO" ]; then
    need_cmd git
    [ -d "$CHECK_REPO/.git" ] || git -C "$CHECK_REPO" rev-parse --git-dir >/dev/null 2>&1 || die "--check path is not a git repo: $CHECK_REPO"
fi

case "$REPO" in
    */*) ;;
    *) die "--repo must be OWNER/REPO, example: webzfs/webzfs" ;;
esac

if ! printf '%s' "$MAX_PAGES" | grep -Eq '^[0-9]+$'; then
    die "--max-pages must be an integer"
fi

add_auth_headers

DIFF_DIR="$OUTDIR/diffs"
mkdir -p "$DIFF_DIR"

TMPDIR="$(mktemp -d)"
cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

PRS_JSONL="$OUTDIR/prs.jsonl"
MANIFEST_TSV="$OUTDIR/manifest.tsv"
MANIFEST_MD="$OUTDIR/manifest.md"

: > "$PRS_JSONL"
printf 'pr\ttitle\tauthor\thead\tbase\tdraft\tdiff_file\tapply_check\thtml_url\n' > "$MANIFEST_TSV"

downloaded_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

printf 'Querying open PRs for %s...\n' "$REPO"

page=1
total_seen=0
while [ "$page" -le "$MAX_PAGES" ]; do
    page_file="$TMPDIR/page-$page.json"
    api_url="${API_BASE}/repos/${REPO}/pulls?state=open&per_page=100&page=${page}"

    if ! curl -fsSL "${api_headers_json[@]}" "$api_url" -o "$page_file"; then
        die "Failed to query GitHub API: $api_url"
    fi

    count="$(
        python3 - "$page_file" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

if isinstance(data, dict):
    msg = data.get("message", "GitHub API returned an error object")
    print(f"ERROR:{msg}")
    sys.exit(0)

print(len(data))
PY
    )"

    case "$count" in
        ERROR:*)
            die "${count#ERROR:}"
            ;;
    esac

    if [ "$count" -eq 0 ]; then
        break
    fi

    total_seen=$((total_seen + count))

    python3 - "$page_file" "$PRS_JSONL" "$AUTHOR_FILTER" "$INCLUDE_DRAFTS" <<'PY'
import json
import re
import sys

page_path, out_path, author_filter, include_drafts_s = sys.argv[1:5]
include_drafts = include_drafts_s == "1"

def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value[:80] or "untitled"

with open(page_path, "r", encoding="utf-8") as f:
    prs = json.load(f)

with open(out_path, "a", encoding="utf-8") as out:
    for pr in prs:
        author = (pr.get("user") or {}).get("login") or ""
        if author_filter and author != author_filter:
            continue

        draft = bool(pr.get("draft", False))
        if draft and not include_drafts:
            continue

        number = int(pr["number"])
        title = (pr.get("title") or "").replace("\t", " ").replace("\n", " ")
        head = ((pr.get("head") or {}).get("ref") or "").replace("\t", " ")
        base = ((pr.get("base") or {}).get("ref") or "").replace("\t", " ")
        html_url = pr.get("html_url") or ""
        slug = slugify(title)
        diff_name = f"pr-{number}-{slug}.diff"

        record = {
            "number": number,
            "title": title,
            "author": author,
            "head": head,
            "base": base,
            "draft": draft,
            "html_url": html_url,
            "diff_name": diff_name,
        }
        out.write(json.dumps(record, sort_keys=True) + "\n")
PY

    page=$((page + 1))
done

selected_count="$(python3 - "$PRS_JSONL" <<'PY'
import sys
count = 0
with open(sys.argv[1], "r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            count += 1
print(count)
PY
)"

if [ "$selected_count" -eq 0 ]; then
    printf 'No matching open PRs found.\n'
    printf 'Saw %s total open PR(s), selected 0 after filters.\n' "$total_seen"
    exit 0
fi

printf 'Found %s matching open PR(s). Downloading diffs...\n' "$selected_count"

while IFS= read -r line; do
    [ -n "$line" ] || continue

    eval "$(
        python3 - "$line" <<'PY'
import json
import shlex
import sys

r = json.loads(sys.argv[1])
for key in ["number", "title", "author", "head", "base", "draft", "html_url", "diff_name"]:
    value = str(r[key])
    print(f"{key.upper()}={shlex.quote(value)}")
PY
    )"

    diff_file="$DIFF_DIR/$DIFF_NAME"
    diff_api_url="${API_BASE}/repos/${REPO}/pulls/${NUMBER}"

    if [ -f "$diff_file" ] && [ "$FORCE" -ne 1 ]; then
        printf 'Keeping existing diff: %s\n' "$diff_file"
    else
        printf 'Downloading PR #%s: %s\n' "$NUMBER" "$TITLE"
        if ! curl -fsSL "${api_headers_diff[@]}" "$diff_api_url" -o "$diff_file"; then
            rm -f "$diff_file"
            printf 'WARNING: failed to download diff for PR #%s\n' "$NUMBER" >&2
            continue
        fi
    fi

    apply_check="not_run"
    if [ -n "$CHECK_REPO" ]; then
        abs_diff="$(abs_path "$diff_file")"
        if git -C "$CHECK_REPO" apply --check "$abs_diff" >/dev/null 2>&1; then
            apply_check="applies"
            printf '  git apply --check: applies\n'
        else
            apply_check="failed"
            printf '  git apply --check: failed\n'
        fi
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$NUMBER" "$TITLE" "$AUTHOR" "$HEAD" "$BASE" "$DRAFT" "$diff_file" "$apply_check" "$HTML_URL" \
        >> "$MANIFEST_TSV"

done < "$PRS_JSONL"

python3 - "$MANIFEST_TSV" "$MANIFEST_MD" "$REPO" "$downloaded_at" "$AUTHOR_FILTER" "$INCLUDE_DRAFTS" <<'PY'
import csv
import sys

manifest_tsv, manifest_md, repo, downloaded_at, author_filter, include_drafts = sys.argv[1:7]

with open(manifest_tsv, "r", encoding="utf-8", newline="") as f:
    rows = list(csv.DictReader(f, delimiter="\t"))

def esc(value):
    return str(value).replace("|", "\\|")

filter_text = "none"
if author_filter:
    filter_text = f"author = {author_filter}"
if include_drafts != "1":
    filter_text += ", drafts skipped"

with open(manifest_md, "w", encoding="utf-8") as out:
    out.write(f"# Open PR diffs for `{repo}`\n\n")
    out.write(f"Downloaded: `{downloaded_at}`\n\n")
    out.write(f"Filter: `{filter_text}`\n\n")
    out.write("| PR | Title | Author | Head | Base | Draft | Apply check | Diff file |\n")
    out.write("|---:|---|---|---|---|---|---|---|\n")
    for r in rows:
        out.write(
            f"| #{esc(r['pr'])} "
            f"| {esc(r['title'])} "
            f"| {esc(r['author'])} "
            f"| `{esc(r['head'])}` "
            f"| `{esc(r['base'])}` "
            f"| {esc(r['draft'])} "
            f"| {esc(r['apply_check'])} "
            f"| `{esc(r['diff_file'])}` |\n"
        )
PY

printf '\nDone.\n'
printf 'Diffs:    %s\n' "$DIFF_DIR"
printf 'Manifest: %s\n' "$MANIFEST_MD"

if [ -z "$CHECK_REPO" ]; then
    printf '\nTo check one diff against your current branch:\n'
    printf '  git apply --check %s\n' "$(shell_quote "$DIFF_DIR/pr-NAME.diff")"
    printf '\nTo test one diff on a throwaway branch:\n'
    printf '  git checkout -b test/pr-number\n'
    printf '  git apply %s\n' "$(shell_quote "$DIFF_DIR/pr-NAME.diff")"
fi
