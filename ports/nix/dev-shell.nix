{
  pkgs ? import <nixpkgs> { },
}:

let
  inherit (pkgs) lib;

  # Runtime Python dependencies derived from upstream requirements.txt.
  # Version pins are intentionally relaxed so nixpkgs can resolve its own
  # versions; the exact Poetry pins cannot be satisfied across the package set.
  #
  # ecdsa is intentionally omitted: python-jose falls back to the
  # cryptography backend, and ecdsa is flagged insecure in nixpkgs.
  pythonDeps =
    python3Packages: with python3Packages; [
      annotated-doc
      annotated-types
      anyio
      bcrypt
      cffi
      click
      colorama
      croniter
      cryptography
      fastapi
      gunicorn
      h11
      humanize
      idna
      invoke
      jinja2
      markdown-it-py
      markupsafe
      mdurl
      packaging
      paramiko
      psutil
      pyasn1
      pycparser
      pydantic
      pydantic-core
      pydantic-settings
      pygments
      pynacl
      python-dateutil
      python-dotenv
      python-jose
      python-multipart
      python-pam
      rich
      rsa
      shellingham
      six
      starlette
      typer
      typing-extensions
      typing-inspection
      uvicorn
    ];

  # Development tools from [tool.poetry.group.dev.dependencies] in pyproject.toml.
  # Some packages are optional because they are not currently packaged in
  # nixpkgs; the shell remains usable without them.
  devDeps =
    python3Packages:
    (with python3Packages; [
      ruff
      black
      isort
      mypy
      pytest
      pytest-asyncio
      pytest-mock
      pytest-socket
      pytest-xdist
      coverage
      httpx
      faker
      setuptools
    ])
    ++ lib.optional (python3Packages ? pytest-watcher) python3Packages.pytest-watcher
    ++ lib.optional (python3Packages ? pytest-freezegun) python3Packages.pytest-freezegun
    ++ lib.optional (python3Packages ? djhtml) python3Packages.djhtml;

  pythonEnv = pkgs.python3.withPackages (ps: pythonDeps ps ++ devDeps ps);
in

pkgs.mkShell {
  # nodejs bundles npm in nixpkgs, so both are satisfied by the same package.
  packages = [
    pythonEnv
    pkgs.nodejs
    pkgs.pre-commit
  ];

  # Make the repo root importable so `python3 -m config.app` and
  # `gunicorn -c config/gunicorn.conf.py` work without a virtualenv.
  PYTHONPATH = toString ../.;

  # Development settings module used by run_dev.sh.
  SETTINGS_MODULE = "config.settings.dev";

  shellHook = ''
    echo ""
    echo "  Welcome to the WebZFS development shell!"
    echo ""
    echo "  Python packages (runtime + dev) are provided by nixpkgs."
    echo "  Node.js and npm are available for Tailwind CSS builds."
    echo ""
    echo "  Quick commands:"
    echo "    ./run_dev.sh                 # start the dev server via gunicorn"
    echo "    python3 -m config.app        # run the FastAPI app directly"
    echo "    npx postcss src/styles.css -o static/css/styles.css"
    echo "    pytest                       # run the test suite"
    echo "    ruff check . && black --check . && isort --check-only ."
    echo ""
    echo "  PYTHONPATH is set to the repository root and"
    echo "  SETTINGS_MODULE=config.settings.dev."
    echo ""
  '';
}
