# Illumos/OmniOS Python Project Compatibility Report

Building a modern FastAPI/Pydantic stack on Illumos (specifically OmniOS) is a powerful choice, but it requires moving away from pre-compiled Linux "wheels" into a "build-from-source" workflow.

---

## 1. System Dependency Mapping
Most of your requested system packages are available via the standard `pkg` repositories (OmniOS) or `pkgsrc` (Joyent/SmartOS).

| Linux/Generic Name | OmniOS Package Name | Notes |
| :--- | :--- | :--- |
| **python311** | `runtime/python-311` | Fully supported. |
| **py311-pip** | `library/python/pip-311` | Standard. |
| **node / npm** | `runtime/nodejs` | Use `pkg install nodejs`. |
| **smartmontools** | `system/storage/smartmontools` | Native support for Illumos drivers. |
| **sanoid** | `N/A` | Install via `git` + `perl` (ZFS focus). |
| **rust** | `developer/lang/rustc` | **Required** for Pydantic/Cryptography. |
| **libsodium** | `library/security/libsodium` | Standard library. |

---

## 2. Python Requirement Analysis
Your `requirements.txt` falls into three distinct risk categories:

### 🟢 Zone 1: Pure Python (Safe)
*Includes: `fastapi`, `starlette`, `jinja2`, `uvicorn`, `anyio`, `click`, `typer`, `humanize`, `python-dotenv`.*
These contain no compiled C/Rust code. If Python 3.11 is running, these will function exactly as they do on Linux.

### 🟡 Zone 2: Compiled Extensions (Needs Toolchain)
*Includes: `pydantic-core`, `cryptography`, `pydantic`, `typing-extensions`.*
Illumos does not support "manylinux" wheels from PyPI. `pip` will compile these from source during installation.
* **Action:** Ensure `gcc` and `rustc` are in your `$PATH`.
* **Compilation:** Expect the first `pip install` to take 5–10 minutes as it builds the Rust components.

### 🔴 Zone 3: OS-Specific Hooks (Audit Required)
* **`psutil` (7.1.3):** While it supports Solaris-like kernels, Illumos has a unique `/proc` structure and uses `kstat`. Some advanced networking or disk metrics might return `NotImplementedError`.
* **`python-pam` (2.0.2):** Illumos uses OpenSolaris-descended PAM. While the API is similar to Linux-PAM, double-check your `/etc/pam.conf` logic.
* **`shellingham`:** May have trouble identifying the shell if you are using a non-standard Illumos environment (e.g., inside a Zone or using `pfexec`).

---

## 3. Recommended Build Environment
To prevent `pip` from failing due to missing headers, run this on your OmniOS instance before installing requirements:

```bash
# Update package list
pkg refresh

# Install the build toolchain
pkg install developer/gcc-11 developer/build/make developer/lang/rustc

# Install security headers for 'cryptography' and 'libsodium'
pkg install library/security/openssl library/security/libsodium

## 4. Known Workarounds
If cryptography fails to find OpenSSL, help the compiler by setting the path:

```Bash
export OPENSSL_DIR=/usr/openssl/3 # Check 'pkg info openssl' for version
pip install cryptography==44.0.0
```

$## 5. Summary Checklist
[x] FastAPI Stack: Works excellently with Illumos' high-performance networking.

[x] ZFS Integration: Perfect environment for sanoid and smartmontools.

[!] Build Times: Be patient on the first install; Rust compilation is resource-intensive.


**Would you like me to create a shell script that you can run on your OmniOS machine to