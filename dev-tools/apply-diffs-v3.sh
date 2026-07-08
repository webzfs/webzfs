#!/usr/bin/env bash
set -euo pipefail

THREE_WAY=0
ALLOW_EMPTY=0
COMMIT_EACH=0
COMMIT_ALL=0
DELETE_BRANCH_ON_FAIL=0
START_FROM="HEAD"
START_AT=1
ONTO_CURRENT=0
RESUME=0
CLEAR_STATE=0

usage() {
    cat <<'USAGE'
apply-diffs-v3.sh

Create a new git branch and apply every .diff/.patch file from a directory.
If a patch fails with conflicts, the script saves resume state so you can fix,
commit, and continue with the remaining diffs.

Create a new branch:
  apply-diffs-v3.sh [options] BRANCH_NAME DIFF_DIR [REPO_PATH]
  apply-diffs-v3.sh [options] BRANCH_NAME DIFF_PARENT DIFF_SUBDIR [REPO_PATH]

Apply to the currently checked-out branch:
  apply-diffs-v3.sh --onto-current [options] DIFF_DIR [REPO_PATH]
  apply-diffs-v3.sh --onto-current [options] DIFF_PARENT DIFF_SUBDIR [REPO_PATH]

Resume after resolving and committing a failed/conflicted diff:
  apply-diffs-v3.sh --resume [REPO_PATH]

Examples:
  apply-diffs-v3.sh dependabot /mnt/scratch/diffs .
  apply-diffs-v3.sh dependabot /mnt/scratch diffs ./
  apply-diffs-v3.sh --start-from origin/main --3way dependabot /mnt/scratch/diffs .
  apply-diffs-v3.sh --onto-current --start-at 2 --3way /mnt/scratch/diffs .
  apply-diffs-v3.sh --resume .

Options:
  --start-from REF     Create the new branch from REF. Default: HEAD.
  --onto-current       Do not create a branch; apply to the current branch.
  --start-at N         Start at sorted diff number N, using 1-based indexing.
                       Example: --start-at 2 skips the first sorted diff.
  --resume             Resume from saved state after a previous conflict/failure.
  --clear-state        Remove saved resume state for this repo.
  --3way              Use "git apply --3way".
  --allow-empty        Do not fail if no .diff/.patch files are found.
  --commit-each        Commit after each successfully applied diff.
  --commit-all         Commit all applied diffs as one final commit.
  --delete-branch-on-fail
                       If creating a new branch and a failure occurs, delete it.
                       This is not used for --onto-current or --resume.
  -h, --help           Show this help.

Important:
  If a diff fails with conflicts, resolve the conflict, commit the result, then run:
    apply-diffs-v3.sh --resume .
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

is_clean_repo() {
    local repo="$1"

    git -C "$repo" diff --quiet --ignore-submodules -- &&
    git -C "$repo" diff --cached --quiet --ignore-submodules --
}

has_changes_or_conflicts() {
    local repo="$1"

    [ -n "$(git -C "$repo" status --porcelain)" ]
}

write_state() {
    local state_dir="$1"
    local branch="$2"
    local diff_dir="$3"
    local next_index="$4"
    local total="$5"
    local three_way="$6"
    local commit_each="$7"
    local commit_all="$8"

    mkdir -p "$state_dir"
    printf '%s\n' "$branch" > "$state_dir/branch"
    printf '%s\n' "$diff_dir" > "$state_dir/diff_dir"
    printf '%s\n' "$next_index" > "$state_dir/next_index"
    printf '%s\n' "$total" > "$state_dir/total"
    printf '%s\n' "$three_way" > "$state_dir/three_way"
    printf '%s\n' "$commit_each" > "$state_dir/commit_each"
    printf '%s\n' "$commit_all" > "$state_dir/commit_all"
}

clear_state_dir() {
    local state_dir="$1"
    rm -rf "$state_dir"
}

read_state_file() {
    local path="$1"
    [ -f "$path" ] || die "Missing resume state file: $path"
    sed -n '1p' "$path"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --start-from)
            [ "$#" -ge 2 ] || die "--start-from requires a git ref"
            START_FROM="$2"
            shift 2
            ;;
        --onto-current)
            ONTO_CURRENT=1
            shift
            ;;
        --start-at)
            [ "$#" -ge 2 ] || die "--start-at requires a number"
            START_AT="$2"
            shift 2
            ;;
        --resume)
            RESUME=1
            shift
            ;;
        --clear-state)
            CLEAR_STATE=1
            shift
            ;;
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
            DELETE_BRANCH_ON_FAIL=1
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

need_cmd git
need_cmd python3

if [ "$COMMIT_EACH" -eq 1 ] && [ "$COMMIT_ALL" -eq 1 ]; then
    die "Use either --commit-each or --commit-all, not both"
fi

if ! printf '%s' "$START_AT" | grep -Eq '^[0-9]+$'; then
    die "--start-at must be a positive integer"
fi

if [ "$START_AT" -lt 1 ]; then
    die "--start-at must be 1 or greater"
fi

REPO_PATH="."
BRANCH_NAME=""
DIFF_DIR=""

if [ "$RESUME" -eq 1 ] || [ "$CLEAR_STATE" -eq 1 ]; then
    if [ "$#" -gt 1 ]; then
        usage
        exit 2
    fi
    REPO_PATH="${1:-.}"
else
    if [ "$ONTO_CURRENT" -eq 1 ]; then
        if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
            usage
            exit 2
        fi

        ARG1="$1"
        ARG2="${2:-}"
        ARG3="${3:-}"

        if [ "$#" -eq 1 ]; then
            DIFF_DIR="$ARG1"
            REPO_PATH="."
        elif [ "$#" -eq 2 ]; then
            if [ -d "$ARG1/$ARG2" ]; then
                DIFF_DIR="$ARG1/$ARG2"
                REPO_PATH="."
            else
                DIFF_DIR="$ARG1"
                REPO_PATH="$ARG2"
            fi
        else
            DIFF_DIR="$ARG1/$ARG2"
            REPO_PATH="$ARG3"
        fi
    else
        if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
            usage
            exit 2
        fi

        BRANCH_NAME="$1"
        ARG2="$2"
        ARG3="${3:-}"
        ARG4="${4:-}"

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
    fi
fi

REPO_PATH="$(abs_path "$REPO_PATH")"
git -C "$REPO_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Not a git repo: $REPO_PATH"

TOPLEVEL="$(git -C "$REPO_PATH" rev-parse --show-toplevel)"
GIT_STATE_DIR="$(git -C "$TOPLEVEL" rev-parse --git-path apply-diffs-state)"

if [ "$CLEAR_STATE" -eq 1 ]; then
    clear_state_dir "$GIT_STATE_DIR"
    printf 'Cleared apply-diffs resume state for %s\n' "$TOPLEVEL"
    exit 0
fi

ORIGINAL_BRANCH="$(git -C "$TOPLEVEL" branch --show-current || true)"
ORIGINAL_HEAD="$(git -C "$TOPLEVEL" rev-parse --verify HEAD)"

if [ -z "$ORIGINAL_BRANCH" ]; then
    ORIGINAL_BRANCH="$ORIGINAL_HEAD"
fi

if [ "$RESUME" -eq 1 ]; then
    [ -d "$GIT_STATE_DIR" ] || die "No saved resume state found for this repo"

    SAVED_BRANCH="$(read_state_file "$GIT_STATE_DIR/branch")"
    DIFF_DIR="$(read_state_file "$GIT_STATE_DIR/diff_dir")"
    START_AT="$(read_state_file "$GIT_STATE_DIR/next_index")"
    THREE_WAY="$(read_state_file "$GIT_STATE_DIR/three_way")"
    COMMIT_EACH="$(read_state_file "$GIT_STATE_DIR/commit_each")"
    COMMIT_ALL="$(read_state_file "$GIT_STATE_DIR/commit_all")"

    CURRENT_BRANCH="$(git -C "$TOPLEVEL" branch --show-current || true)"
    [ "$CURRENT_BRANCH" = "$SAVED_BRANCH" ] || die "Resume state is for branch '$SAVED_BRANCH', but current branch is '$CURRENT_BRANCH'"

    ONTO_CURRENT=1
    BRANCH_NAME="$CURRENT_BRANCH"
fi

[ -d "$DIFF_DIR" ] || die "Diff directory not found: $DIFF_DIR"
DIFF_DIR="$(abs_path "$DIFF_DIR")"

if ! is_clean_repo "$TOPLEVEL"; then
    die "Working tree/index is not clean. Commit, stash, or discard changes before running this script."
fi

mapfile -t DIFF_FILES < <(list_diffs "$DIFF_DIR")

if [ "${#DIFF_FILES[@]}" -eq 0 ]; then
    if [ "$ALLOW_EMPTY" -eq 1 ]; then
        printf 'No .diff/.patch files found in %s\n' "$DIFF_DIR"
        exit 0
    fi
    die "No .diff/.patch files found in: $DIFF_DIR"
fi

if [ "$START_AT" -gt "${#DIFF_FILES[@]}" ]; then
    printf 'Nothing to do. --start-at %s is beyond diff count %s.\n' "$START_AT" "${#DIFF_FILES[@]}"
    clear_state_dir "$GIT_STATE_DIR"
    exit 0
fi

if [ "$ONTO_CURRENT" -eq 0 ]; then
    git -C "$TOPLEVEL" rev-parse --verify "$START_FROM^{commit}" >/dev/null 2>&1 || die "Start ref does not exist: $START_FROM"

    if git -C "$TOPLEVEL" show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
        die "Branch already exists: $BRANCH_NAME"
    fi

    printf 'Repository: %s\n' "$TOPLEVEL"
    printf 'Original ref: %s\n' "$ORIGINAL_BRANCH"
    printf 'Start from:  %s\n' "$START_FROM"
    printf 'New branch:  %s\n' "$BRANCH_NAME"
    printf 'Diff dir:    %s\n' "$DIFF_DIR"
    printf 'Diff count:  %s\n' "${#DIFF_FILES[@]}"
    printf 'Start at:    %s\n' "$START_AT"
    printf '\n'

    git -C "$TOPLEVEL" checkout -b "$BRANCH_NAME" "$START_FROM"
    CREATED_BRANCH=1
else
    CURRENT_BRANCH="$(git -C "$TOPLEVEL" branch --show-current || true)"
    [ -n "$CURRENT_BRANCH" ] || die "--onto-current/--resume requires a named current branch"

    BRANCH_NAME="$CURRENT_BRANCH"
    CREATED_BRANCH=0

    printf 'Repository: %s\n' "$TOPLEVEL"
    printf 'Current branch: %s\n' "$BRANCH_NAME"
    printf 'Diff dir:       %s\n' "$DIFF_DIR"
    printf 'Diff count:     %s\n' "${#DIFF_FILES[@]}"
    printf 'Start at:       %s\n' "$START_AT"
    printf '\n'
fi

fail_cleanup() {
    status=$?
    if [ "$status" -ne 0 ] && [ "${CREATED_BRANCH:-0}" -eq 1 ] && [ "$DELETE_BRANCH_ON_FAIL" -eq 1 ]; then
        printf '\nFailure detected. Returning to original ref and deleting %s...\n' "$BRANCH_NAME" >&2

        git -C "$TOPLEVEL" reset --hard >/dev/null 2>&1 || true
        git -C "$TOPLEVEL" checkout "$ORIGINAL_BRANCH" >/dev/null 2>&1 || git -C "$TOPLEVEL" checkout "$ORIGINAL_HEAD" >/dev/null 2>&1 || true
        git -C "$TOPLEVEL" branch -D "$BRANCH_NAME" >/dev/null 2>&1 || true
        clear_state_dir "$GIT_STATE_DIR"
    fi
    exit "$status"
}
trap fail_cleanup EXIT

APPLY_ARGS=()
if [ "$THREE_WAY" -eq 1 ]; then
    APPLY_ARGS+=(--3way)
fi

applied=0
total="${#DIFF_FILES[@]}"
index="$START_AT"

while [ "$index" -le "$total" ]; do
    diff_file="${DIFF_FILES[$((index - 1))]}"
    printf '[%s/%s] Applying: %s\n' "$index" "$total" "$diff_file"

    if ! git -C "$TOPLEVEL" apply "${APPLY_ARGS[@]}" "$diff_file"; then
        if has_changes_or_conflicts "$TOPLEVEL"; then
            resume_index=$((index + 1))
            write_state "$GIT_STATE_DIR" "$BRANCH_NAME" "$DIFF_DIR" "$resume_index" "$total" "$THREE_WAY" "$COMMIT_EACH" "$COMMIT_ALL"

            printf '\nPatch stopped with working-tree changes/conflicts:\n  %s\n\n' "$diff_file" >&2
            printf 'Resolve the conflict or finish the manual edit, then commit it.\n' >&2
            printf 'After the repo is clean, continue with:\n' >&2
            printf '  %s --resume %q\n' "$0" "$TOPLEVEL" >&2
            printf '\nSaved resume state: next diff index %s of %s\n' "$resume_index" "$total" >&2
        else
            write_state "$GIT_STATE_DIR" "$BRANCH_NAME" "$DIFF_DIR" "$index" "$total" "$THREE_WAY" "$COMMIT_EACH" "$COMMIT_ALL"

            printf '\nPatch failed without leaving changes:\n  %s\n\n' "$diff_file" >&2
            printf 'Saved resume state at the same diff index %s of %s.\n' "$index" "$total" >&2
            printf 'You may need to inspect or manually skip this diff.\n' >&2
        fi

        printf '\nUseful inspection commands:\n' >&2
        printf '  git status\n' >&2
        printf '  git diff\n' >&2
        printf '  git diff --check\n' >&2
        exit 1
    fi

    applied=$((applied + 1))

    if [ "$COMMIT_EACH" -eq 1 ]; then
        if ! is_clean_repo "$TOPLEVEL"; then
            slug="$(slug_from_diff "$diff_file")"
            git -C "$TOPLEVEL" add -A
            git -C "$TOPLEVEL" commit -m "Apply $slug"
        else
            printf '  No working tree changes from this diff; skipping commit.\n'
        fi
    fi

    index=$((index + 1))
done

if [ "$COMMIT_ALL" -eq 1 ]; then
    if ! is_clean_repo "$TOPLEVEL"; then
        git -C "$TOPLEVEL" add -A
        git -C "$TOPLEVEL" commit -m "Apply PR diffs"
    else
        printf 'No working tree changes to commit.\n'
    fi
fi

clear_state_dir "$GIT_STATE_DIR"
trap - EXIT

printf '\nDone.\n'
printf 'Applied %s diff(s) to branch %s.\n' "$applied" "$BRANCH_NAME"

if [ "$COMMIT_EACH" -eq 0 ] && [ "$COMMIT_ALL" -eq 0 ]; then
    printf '\nChanges are currently uncommitted unless you committed during conflict resolution.\n'
    printf 'Next commands you probably want:\n'
    printf '  git status\n'
    printf '  git diff --stat\n'
    printf '  git add -A\n'
    printf '  git commit -m "Apply open PR diffs"\n'
fi
