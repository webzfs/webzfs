#!/usr/bin/env bash
set -euo pipefail

THREE_WAY=0
ALLOW_EMPTY=0
COMMIT_EACH=0
COMMIT_ALL=0
KEEP_BRANCH_ON_FAIL=1

usage() {
    cat <<'USAGE'
apply-diffs.sh

Create a new git branch and apply every .diff/.patch file from a directory.

Usage forms:
  apply-diffs.sh [options] BRANCH_NAME DIFF_DIR [REPO_PATH]
  apply-diffs.sh [options] BRANCH_NAME DIFF_PARENT DIFF_SUBDIR [REPO_PATH]

Examples:
  apply-diffs.sh test/dependabot /mnt/scratch/diffs .
  apply-diffs.sh test/dependabot /mnt/scratch diffs .
  apply-diffs.sh test/dependabot /mnt/scratch/diffs
  apply-diffs.sh --3way test/dependabot /mnt/scratch diffs ./

Options:
  --3way              Use "git apply --3way" instead of plain "git apply".
                      This can help when a patch is slightly offset from your branch.
  --allow-empty       Do not fail if no .diff/.patch files are found.
  --commit-each       Commit after each successfully applied diff.
  --commit-all        Commit all applied diffs as one final commit.
  --delete-branch-on-fail
                      If applying a diff fails, switch back to the original branch
                      and delete the newly-created branch.
  -h, --help          Show this help.

Behavior:
  - REPO_PATH defaults to the current directory.
  - The repo must have a clean working tree before starting.
  - BRANCH_NAME must not already exist.
  - Diff files are applied in sorted filename order.
  - If a patch fails, the script stops immediately.
  - Without --commit-each or --commit-all, changes remain uncommitted on the new branch.
USAGE
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

abs_path() {
    python3 - "$1" <<'PY'
import os
import sys
print(os.path.abspath(sys.argv[1]))
PY
}

list_diffs() {
    python3 - "$1" <<'PY'
import os
import sys

root = sys.argv[1]
paths = []
for name in os.listdir(root):
    path = os.path.join(root, name)
    if os.path.isfile(path) and (name.endswith(".diff") or name.endswith(".patch")):
        paths.append(path)

for path in sorted(paths):
    print(path)
PY
}

slug_from_diff() {
    python3 - "$1" <<'PY'
import os
import re
import sys

base = os.path.basename(sys.argv[1])
base = re.sub(r"\.(diff|patch)$", "", base)
base = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")
print(base or "diff")
PY
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --3way)
            THREE_WAY=1
            shift
            ;;
        --allow-empty)
            ALLOW_EMPTY=1
            shift
            ;;
        --commit-each)
            COMMIT_EACH=1
            shift
            ;;
        --commit-all)
            COMMIT_ALL=1
            shift
            ;;
        --delete-branch-on-fail)
            KEEP_BRANCH_ON_FAIL=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            break
            ;;
        -*)
            die "Unknown option: $1"
            ;;
        *)
            break
            ;;
    esac
done

if [ "$COMMIT_EACH" -eq 1 ] && [ "$COMMIT_ALL" -eq 1 ]; then
    die "Use either --commit-each or --commit-all, not both"
fi

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
    usage
    exit 2
fi

BRANCH_NAME="$1"
ARG2="$2"
ARG3="${3:-}"
ARG4="${4:-}"

need_cmd git
need_cmd python3

REPO_PATH="."
DIFF_DIR=""

if [ "$#" -eq 2 ]; then
    DIFF_DIR="$ARG2"
    REPO_PATH="."
elif [ "$#" -eq 3 ]; then
    if [ -d "$ARG2/$ARG3" ]; then
        DIFF_DIR="$ARG2/$ARG3"
        REPO_PATH="."
    else
        DIFF_DIR="$ARG2"
        REPO_PATH="$ARG3"
    fi
else
    DIFF_DIR="$ARG2/$ARG3"
    REPO_PATH="$ARG4"
fi

[ -n "$BRANCH_NAME" ] || die "Branch name cannot be empty"
[ -d "$DIFF_DIR" ] || die "Diff directory not found: $DIFF_DIR"

REPO_PATH="$(abs_path "$REPO_PATH")"
DIFF_DIR="$(abs_path "$DIFF_DIR")"

git -C "$REPO_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Not a git repo: $REPO_PATH"

TOPLEVEL="$(git -C "$REPO_PATH" rev-parse --show-toplevel)"
ORIGINAL_BRANCH="$(git -C "$TOPLEVEL" branch --show-current || true)"
ORIGINAL_HEAD="$(git -C "$TOPLEVEL" rev-parse --verify HEAD)"

if [ -z "$ORIGINAL_BRANCH" ]; then
    ORIGINAL_BRANCH="$ORIGINAL_HEAD"
fi

if ! git -C "$TOPLEVEL" diff --quiet --ignore-submodules --; then
    die "Working tree has unstaged changes. Commit, stash, or discard them first."
fi

if ! git -C "$TOPLEVEL" diff --cached --quiet --ignore-submodules --; then
    die "Index has staged changes. Commit, stash, or unstage them first."
fi

if git -C "$TOPLEVEL" show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
    die "Branch already exists: $BRANCH_NAME"
fi

mapfile -t DIFF_FILES < <(list_diffs "$DIFF_DIR")

if [ "${#DIFF_FILES[@]}" -eq 0 ]; then
    if [ "$ALLOW_EMPTY" -eq 1 ]; then
        printf 'No .diff/.patch files found in %s\n' "$DIFF_DIR"
        exit 0
    fi
    die "No .diff/.patch files found in: $DIFF_DIR"
fi

printf 'Repository: %s\n' "$TOPLEVEL"
printf 'Starting from: %s\n' "$ORIGINAL_BRANCH"
printf 'New branch:  %s\n' "$BRANCH_NAME"
printf 'Diff dir:    %s\n' "$DIFF_DIR"
printf 'Diff count:  %s\n' "${#DIFF_FILES[@]}"
printf '\n'

git -C "$TOPLEVEL" checkout -b "$BRANCH_NAME"

created_branch=1

fail_cleanup() {
    status=$?
    if [ "$status" -ne 0 ] && [ "${created_branch:-0}" -eq 1 ] && [ "$KEEP_BRANCH_ON_FAIL" -eq 0 ]; then
        printf '\nFailure detected. Returning to original branch and deleting %s...\n' "$BRANCH_NAME" >&2

        git -C "$TOPLEVEL" reset --hard >/dev/null 2>&1 || true
        git -C "$TOPLEVEL" checkout "$ORIGINAL_BRANCH" >/dev/null 2>&1 || true
        git -C "$TOPLEVEL" branch -D "$BRANCH_NAME" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap fail_cleanup EXIT

APPLY_ARGS=()
if [ "$THREE_WAY" -eq 1 ]; then
    APPLY_ARGS+=(--3way)
fi

applied=0

for diff_file in "${DIFF_FILES[@]}"; do
    printf 'Applying: %s\n' "$diff_file"

    if ! git -C "$TOPLEVEL" apply "${APPLY_ARGS[@]}" "$diff_file"; then
        printf '\nFailed while applying:\n  %s\n\n' "$diff_file" >&2
        printf 'You are still on branch:\n  %s\n\n' "$BRANCH_NAME" >&2
        printf 'Useful inspection commands:\n' >&2
        printf '  git status\n' >&2
        printf '  git diff\n' >&2
        printf '  git apply --check %q\n' "$diff_file" >&2
        if [ "$THREE_WAY" -eq 0 ]; then
            printf '  git apply --3way %q\n' "$diff_file" >&2
        fi
        exit 1
    fi

    applied=$((applied + 1))

    if [ "$COMMIT_EACH" -eq 1 ]; then
        if ! git -C "$TOPLEVEL" diff --quiet --ignore-submodules --; then
            slug="$(slug_from_diff "$diff_file")"
            git -C "$TOPLEVEL" add -A
            git -C "$TOPLEVEL" commit -m "Apply $slug"
        else
            printf '  No working tree changes from this diff; skipping commit.\n'
        fi
    fi
done

if [ "$COMMIT_ALL" -eq 1 ]; then
    if ! git -C "$TOPLEVEL" diff --quiet --ignore-submodules --; then
        git -C "$TOPLEVEL" add -A
        git -C "$TOPLEVEL" commit -m "Apply PR diffs"
    else
        printf 'No working tree changes to commit.\n'
    fi
fi

trap - EXIT

printf '\nDone.\n'
printf 'Applied %s diff(s) to branch %s.\n' "$applied" "$BRANCH_NAME"

if [ "$COMMIT_EACH" -eq 0 ] && [ "$COMMIT_ALL" -eq 0 ]; then
    printf '\nChanges are currently uncommitted.\n'
    printf 'Next commands you probably want:\n'
    printf '  git status\n'
    printf '  git diff --stat\n'
    printf '  git add -A\n'
    printf '  git commit -m "Apply open PR diffs"\n'
fi
