# Contributing to WebZFS

Thank you for your interest in contributing to WebZFS. This document covers the essentials for getting started.

## Getting Started

1. Fork and clone the repository.
2. Run the development setup script for your platform:
   - Linux: `./setup_dev_linux.sh`
   - FreeBSD: `./setup_dev_freebsd.sh`
3. Start the development server: `./run_dev.sh`
4. Access the application at `http://localhost:26619`.

See [BUILD_AND_RUN.md](BUILD_AND_RUN.md) for detailed setup and configuration instructions.

## Project Architecture

WebZFS uses a three-layer architecture:

| Layer | Directory | Responsibility |
|-------|-----------|----------------|
| Services | `services/` | Business logic, system command execution, OS detection |
| Views | `views/` | FastAPI route handlers, request/response handling |
| Templates | `templates/` | Jinja2 HTML templates with HTMX and Tailwind CSS |

Routers are registered in `views/__init__.py`. Navigation tabs are configured in `config/templates.py`.

For a complete walkthrough of adding a new feature, see [add_feature_demo/ADDING_NEW_FEATURES.md](add_feature_demo/ADDING_NEW_FEATURES.md).

## Submitting Changes

1. Create a feature branch from `main`.
2. Make your changes following the guidelines below.
3. Test on at least one supported platform (Linux or FreeBSD).
4. Submit a pull request with a clear description of your changes.

## Code Guidelines

- **Python 3.11+** is required. Use type hints.
- **Simple code over clever code.** Readability is prioritized.
- **OS compatibility**: Default to Linux, check explicitly for FreeBSD. Use the existing `is_freebsd()` / `get_os_type()` helpers in `services/utils.py`.
- **Command execution**: All system commands must go through `run_command()` in `services/utils.py`.
- **No shell=True**: Always pass commands as a list to subprocess, never as a string with `shell=True`.
- **POST-Redirect-GET**: Form submissions should redirect with `status_code=303` to prevent duplicate submissions.
- **Error handling**: Wrap service calls in try/except blocks and provide clear error messages.
- **Templates**: Use Tailwind CSS classes. Support dark mode with the `dark:` prefix.


## Reporting Issues

Use the [GitHub issue tracker](https://github.com/webzfs/webzfs/issues). Include:

- Steps to reproduce the issue.
- Operating system and version.
- Relevant log output or error messages.

## Security Issues

If you discover a security vulnerability, please see [SECURITY.md](SECURITY.md) for reporting instructions.

## Code of Conduct

All contributors are expected to follow the project's [Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
