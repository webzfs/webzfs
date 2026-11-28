#!/bin/bash
# Rebuild static/css/styles.css and the vendored JavaScript in static/vendor/.
#
# Run this after changing anything under templates/ or src/, then commit the
# updated static/css/styles.css with your change. Installed systems do not
# have Node.js, so they use the committed file as it is (GitHub issue #277).
#
# Usage: dev-tools/rebuild-css.sh

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(dirname "$script_dir")"
cd "$repo_dir"

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
    echo "Error: Node.js and npm are required to rebuild the CSS."
    echo "Install Node.js 22 or newer and try again."
    exit 1
fi

node_major="$(node --version | sed 's/^v//' | cut -d. -f1)"
if [ "$node_major" -lt 22 ]; then
    echo "Error: Node.js 22 or newer is required (found $(node --version))."
    exit 1
fi

# Install the build tools on first use, or when package-lock.json is newer
# than the installed copy (for example after a dependency bump).
if [ ! -d node_modules ] || [ package-lock.json -nt node_modules/.package-lock.json ]; then
    echo "Installing Node.js build dependencies..."
    npm install
fi

echo "Rebuilding static assets..."
npm run build:css

echo
echo "Done. Files that changed:"
git status --short static/css/styles.css static/vendor
echo
echo "If static/css/styles.css is listed above, commit it with your change."
