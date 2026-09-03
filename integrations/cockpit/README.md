# WebZFS Cockpit Integration

This directory contains the Linux-only Cockpit integration for WebZFS.

## Implemented Scope

- Cockpit sidebar registration through `manifest.json`
- WebZFS `GET /` through `cockpit.http(26619)`
- Login page rendering with package-local WebZFS CSS and image assets
- Native login form POST through the Cockpit bridge
- In-memory relay of the WebZFS `token` cookie
- Redirect handling for WebZFS backend paths
- Equivalent handling for absolute localhost and 127.0.0.1 WebZFS redirects
- Authenticated dashboard rendering
- Internal link interception
- Canonical utility router paths that avoid automatic trailing-slash redirects
- Authenticated HTMX GET and state-changing requests through `cockpit.http()`
- WebZFS redirect handling without leaving the Cockpit component
- Existing inline controls and page scripts through a scoped Cockpit runtime
- Alpine.js modal and interactive control support
- Deterministic vanilla JavaScript disk-check dialogs for pool creation and vdev management
- Root-relative `fetch()` and server-sent event support through the Cockpit bridge
- Binary download relay for links, support bundles, and encrypted backup export
- Package-local HTMX, Alpine.js, and Chart.js assets
- Package-local rewriting for both root-relative and absolute localhost WebZFS assets

The adapter still does not support multipart file upload. The Settings restore
archive inspection form reports this limitation instead of navigating outside
Cockpit. All other standard URL-encoded forms use the bridge.

## Prerequisites

1. Linux with Cockpit installed.
2. The normal WebZFS Linux installer prerequisites.

The combined root-level wrapper installs or updates WebZFS before installing the
Cockpit package. If you use the Cockpit-only installer, WebZFS must already be
installed at `/opt/webzfs`, and the source tree must have its Node dependencies
and generated CSS unless `WEBZFS_ASSET_DIR` points to another completed WebZFS
tree:

```text
npm install
npm run build:css
```

## Install

For a new WebZFS and Cockpit installation from the project root, run:

```text
sudo ./install_linux_cockpit.sh
```

For an existing WebZFS installation, run the dedicated update wrapper:

```text
sudo ./update_linux_cockpit.sh
```

The install wrapper always runs `install_linux.sh`. The update wrapper requires
an existing `/opt/webzfs/.venv` and always runs `update_linux.sh`. Both wrappers
run the Cockpit installer from the source checkout while packaging generated CSS
and Node assets from the completed `/opt/webzfs` installation. The Cockpit
installer itself is not copied into `/opt/webzfs`.

To install only the Cockpit package when WebZFS is already current, run:

```text
sudo ./integrations/cockpit/install.sh
```

The installer copies the Cockpit package to `/usr/share/cockpit/webzfs`. It does
not change Cockpit listeners, TLS, services, sockets, `cockpit.conf`, or WebZFS
URL configuration. Installation uses a fresh staging directory and atomically
replaces the active package. The previous package is retained under
`/usr/share/webzfs-cockpit-backups/` for manual cleanup. Reload the Cockpit
browser page after installation.

## Test

```text
npm run test:cockpit
```

The unit tests cover URL normalization, utility route canonicalization, cookie
relay, GET and POST transport, relative and absolute localhost redirects, native
form serialization, all template form declarations, HTMX response semantics,
Fleet route contracts, disk-check modal structure, fetch, server-sent events,
binary downloads, localhost asset rewriting, and download classification.
Browser validation still requires a Linux host with Cockpit and WebZFS running.