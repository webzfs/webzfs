# WebZFS - ZFS Web Management Interface

A modern web-based management interface for ZFS pools, datasets, snapshots, and SMART disk monitoring built with Python FastAPI and HTMX.

![webzfs dashboard](screenshots/dashboard-v0.52.png)


## Features

- **ZFS Pool Management**: Create, import, export, scrub, and monitor ZFS pools
- **Dataset Management**: Create, rename, mount/unmount datasets and volumes
- **Snapshot Management**: Create, destroy, rollback, clone, and diff snapshots
- **Replication Management**: Native ZFS send/receive and Sanoid/Syncoid integration
- **Performance Monitoring**: Real-time pool I/O stats, ARC statistics, and ZFS processes
- **System Observability**: Pool history, events, kernel logs, and module parameters
- **SMART Monitoring**: Disk health, attributes, test scheduling, and error logs
- **Fleet Monitoring**: Monitor multiple remote ZFS servers (optional)
- **Modern UI**: Built with Tailwind CSS and HTMX, utilizing minimal JavaScript
- **User Management**: Relies on PAM to interact with existing local *nix user accounts on the system

## Platform Support

- **Linux**: Any distribution with OpenZFS support
- **FreeBSD**: FreeBSD 13.x and later with OpenZFS

The application automatically detects the operating system and adapts its behavior accordingly.

## Quick Start

### Prerequisites

- Linux or FreeBSD with ZFS support
- Python 3.11+
- Node.js v20+ and npm
- ZFS utilities (zpool, zfs, zdb)
- smartmontools (smartctl)
- sanoid (optional)
- smartd (optional)

### Installation

**Linux:**
```bash
git clone https://github.com/webzfs/webzfs.git
cd webzfs
chmod +x install_linux.sh
sudo ./install_linux.sh
```

The installation script automatically:
- Creates a dedicated `webzfs` system user
- Installs the application to `/opt/webzfs`
- Installs all dependencies and builds assets
- Configures sudo permissions

**FreeBSD:**
```bash
# Install required packages
sudo pkg install python311 py311-pip node npm smartmontools sanoid rust libsodium
# rust and libsodium are only a build dependencies, pydantic-core has no pre-built wheel, so it must be compiled from source which requires these)

# Install WebZFS
git clone https://github.com/webzfs/webzfs.git
cd webzfs
chmod +x install_freebsd.sh
sudo ./install_freebsd.sh
```
The installation script automatically:
- Installs the application to `/opt/webzfs`
- Installs all dependencies and builds assets


### Running

On Linux:
```bash
# Start the application
sudo -u webzfs /opt/webzfs/run.sh

# Or for development mode
sudo -u webzfs /opt/webzfs/run_dev.sh
```

On FreeBSD the application must be run as root to avoid issues with PAM:
```bash
# Start the application
/opt/webzfs/run.sh

# Or for development mode
/opt/webzfs/run_dev.sh
```


### Access

Open your browser to: **http://localhost:26619**

**Port 26619?** Z(26) + F(6) + S(19) = ZFS!

**Remote Access:** Use SSH port forwarding for security:
```bash
ssh -L 127.0.0.1:26619:127.0.0.1:26619 user@server
```

## Documentation

- **[BUILD_AND_RUN.md](BUILD_AND_RUN.md)** - Complete installation, configuration, and troubleshooting guide
- **[add_feature_demo/README.md](add_feature_demo/README.md)** - Guide for adding new features

## Configuration

Configuration is stored in `/opt/webzfs/.env`. Key settings:

- `SECRET_KEY` - Change this in production!
- `HOST` - Default: 127.0.0.1 (localhost only)
- `PORT` - Default: 26619

See [BUILD_AND_RUN.md](BUILD_AND_RUN.md) for detailed configuration options.

## System Service

To run WebZFS as a system service that starts on boot, see the complete service setup instructions in [BUILD_AND_RUN.md](BUILD_AND_RUN.md#system-service-setup).


## Project Structure

```
├── auth/               # Authentication and authorization
├── config/             # Application configuration and settings
├── services/           # Core business logic and ZFS/SMART services
├── templates/          # Jinja2 HTML templates
├── views/              # FastAPI route handlers
├── static/             # Generated static assets
└── src/                # Source CSS files
```

## Technology Stack

- **Backend**: Python 3.11, FastAPI, Uvicorn/Gunicorn
- **Frontend**: HTMX, Tailwind CSS, Jinja2 templates
- **ZFS Integration**: Shell command execution with privilege management
- **Authentication**: PAM-based authentication

## Security Considerations

- Runs as dedicated `webzfs` system user with limited sudo permissions (on Linux)
- Binds to 127.0.0.1 by default (localhost only)
- Use SSH port forwarding for remote access
- Change `SECRET_KEY` in production
- Consider running behind a reverse proxy with SSL/TLS

## Development

### Local Development (No Installation Required)

If you want to develop directly from your git clone without installing to `/opt/webzfs`:

```bash
# Clone the repository
git clone https://github.com/webzfs/webzfs.git
cd webzfs

# Run the setup script (only needed once)
chmod +x setup_dev.sh
./setup_dev.sh

# Start the development server
./run_dev.sh
```

The `setup_dev.sh` script will:
- Create a Python virtual environment (`.venv`)
- Install all Python dependencies
- Install Node.js dependencies
- Build static CSS assets
- Create a `.env` configuration file with a secure SECRET_KEY

You can then make changes, commit, and push directly from this directory.

### Development from /opt/webzfs Installation

If you've installed to `/opt/webzfs`:

```bash
cd /opt/webzfs
source .venv/bin/activate
./run_dev.sh
```

### Adding New Features

For adding new features, see [add_feature_demo/README.md](add_feature_demo/README.md).

### CSS Development

To automatically rebuild CSS when modifying Tailwind classes:

```bash
source .venv/bin/activate
npm run watch:css
```

## Contributing

Contributions are welcome! Please submit issues or pull requests.

## License

MIT License - See [LICENSE](LICENSE) file for details.

## Related Projects

- [OpenZFS](https://openzfs.org/) - Open source ZFS implementation
- [Sanoid](https://github.com/jimsalterjrs/sanoid) - Snapshot management and replication
- [SMART tools](https://www.smartmontools.org/) - Disk health monitoring

## Support

- **GitHub**: https://github.com/q5sys/webzfs
- **Issues**: Use the GitHub issue tracker

---

**For detailed installation, configuration, troubleshooting, and system service setup, see [BUILD_AND_RUN.md](BUILD_AND_RUN.md).**



## AI Usage Disclosure

- AI was used to help add comments to the source files as well as to help clean up the build and installation documentation. 
- AI was used to implement Tailwind.CSS which massively improved my previously hideous CSS. (seriously it was eyebleedingly bad, tailwind is much much better) 
- AI was used to help implement the modal JS confirmation dialogs for certain operations, because my prior attempts were rather buggy and needed to be cleaned up. 
- [Ballistic-CodeLlama-34B](https://huggingface.co/BallisticAI/Ballistic-CodeLlama-34B-v1)

## Dev Comments

I started working on this in my spare time in the Winter of 2022.  It's been slow progress, but something I've been slowly building out since then.  It's gone through a few iterations and refactoring but it's to the point where I'm happy with where it's at for my needs.

My primary focus was to design a transparent UI that interfaces with the system and doesn't rely on its own database, data collection tooling, or custom tooling to interact with the OS. While not the best for everyone, what I wanted was something that's just a UI front end to the tooling I normally would interact with on the CLI.  So I decided to use an ASGI python server and build out the code to use and run the same tooling I would as a sysadmin. These tools already exist and work, so I dont see any reason for me to try to re-invent the wheel.

FYI: You cannot delete pools or datasets with the UI, you will need to do that at the CLI. That was an intentional design choice.
