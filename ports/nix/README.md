# Nix packaging for WebZFS

This directory contains the Nix packaging which is exposed through the flake at the repository root, so you can consume this project directly as a flake input.

## What's here

| File              | Purpose                                                             |
| ----------------- | ------------------------------------------------------------------- |
| `package.nix`     | The WebZFS package                                                  |
| `module.nix`      | A NixOS module that runs WebZFS as a systemd service                |
| `dev-shell.nix`   | A development shell with Python, Node.js, npm, and gunicorn         |

The flake at the repository root exposes:

- `nixosModules.webzfs` / `nixosModules.webzfs`
- `packages.<system>.webzfs` / `packages.<system>.default`
- `devShells.<system>.default`

Supported systems: `x86_64-linux` and `aarch64-linux`.

## Using WebZFS as a NixOS module

Add the flake as an input and import the module:

```nix
{
  inputs = {
    webzfs = {
      url = "github:webzfs/webzfs";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, webzfs, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        webzfs.nixosModules.webzfs
        {
          services.webzfs = {
            enable = true;
            # openFirewall = true;
            settings = {
              SECRET_KEY = "change-me-in-production";
              # HOST, PORT, and other env are also accepted here
            };
          };
        }
      ];
    };
  };
}
```

The module creates a dedicated `webzfs` user/group and runs the service under `systemd` with state kept in `/var/lib/webzfs`.

### What the module sets up for you

The module is self-contained: enabling `services.webzfs.enable` is all you need, there are no further dependencies to configure. Under the hood it wires up the full host integration required for WebZFS to actually work on NixOS, including a `webzfs` user with the required permissions.

The module has full support for Sanoid and Syncoid, however if their configuration is set declaratively in NixOS with `services.sanoid.enable` and `services.syncoid.enable`, then the configuration will be read only in WebZFS.

### Module options

| Option            | Type        | Default         | Description                                    |
| ----------------- | ----------- | --------------- | ---------------------------------------------- |
| `enable`          | bool        | `false`         | Whether to enable the WebZFS service.          |
| `package`         | package     | `pkgs.webzfs`   | The WebZFS package to run.                     |
| `port`            | port        | `26619`         | Port to listen on.                             |
| `host`            | string      | `127.0.0.1`     | Host address to bind to.                       |
| `settings`        | attrsOf str | `{}`            | Extra environment variables for WebZFS.        |
| `openFirewall`    | bool        | `false`         | Open the port in the firewall.                 |
| `user`            | string      | `webzfs`        | System user the service runs as.               |
| `group`           | string      | `webzfs`        | Group for the service user.                    |

> **Note:** WebZFS binds to `127.0.0.1` by default. For remote access, prefer SSH port forwarding (`ssh -L 127.0.0.1:26619:127.0.0.1:26619 host`) over exposing it directly.


## Development shell

Drop into a shell with the full toolchain for WebZFS development (Python runtime + dev tools, Node.js, npm, gunicorn):

```bash
nix develop
```

Inside the shell:

- `./run_dev.sh` — start the dev server via gunicorn
- `python3 -m config.app` — run the FastAPI app directly
- `npx postcss src/styles.css -o static/css/styles.css` — build the Tailwind CSS
- `pytest` — run the test suite
- `ruff check . && black --check . && isort --check-only .` — lint/format checks

`PYTHONPATH` is set to the repository root and `SETTINGS_MODULE=config.settings.dev`, so the
app runs without a virtualenv.


## Building

```bash
# Build the package
nix build .#webzfs        # or: nix build   (uses .#default)

# Check the flake (evaluates + checks all outputs on the host system)
nix flake check

# Show all configureable outputs
nix flake show
```
