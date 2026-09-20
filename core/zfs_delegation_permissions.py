"""Fixed registries from the OpenZFS 2.3 and 2.4 zfs-allow(8) tables."""

from dataclasses import dataclass
import re


OPERATIONS = frozenset("""
allow bookmark clone create destroy diff hold load-key change-key mount promote
receive release rename rollback send share snapshot
""".split())
OTHER = frozenset("""
receive:append groupquota groupobjquota groupused groupobjused userprop userquota
userobjquota userused userobjused projectobjquota projectquota projectobjused projectused
""".split())
PROPERTIES = frozenset("""
aclinherit aclmode acltype atime canmount casesensitivity checksum compression context
copies dedup defcontext devices dnodesize encryption exec filesystem_limit fscontext
keyformat keylocation logbias mlslabel mountpoint nbmand normalization overlay pbkdf2iters
primarycache quota readonly recordsize redundant_metadata refquota refreservation relatime
reservation rootcontext secondarycache setuid sharenfs sharesmb snapdev snapdir snapshot_limit
special_small_blocks sync utf8only version volblocksize volmode volsize vscan xattr zoned
""".split())
OPENZFS_23_PERMISSIONS = OPERATIONS | OTHER | PROPERTIES
OPENZFS_24_PERMISSIONS = OPENZFS_23_PERMISSIONS | {"send:raw", "send:encrypted"}
LINUX_RESTRICTED = frozenset("mount mountpoint canmount rename share".split())
VOLUME_ONLY = frozenset("snapdev volblocksize volmode volsize".split())
FILESYSTEM_ONLY = frozenset("""
create diff mount share aclinherit aclmode acltype atime canmount casesensitivity context
defcontext devices dnodesize exec filesystem_limit fscontext mlslabel mountpoint nbmand
normalization overlay quota recordsize refquota relatime rootcontext setuid sharenfs sharesmb
snapdir special_small_blocks utf8only version vscan xattr
groupquota groupobjquota groupused groupobjused userquota userobjquota userused userobjused
projectobjquota projectquota projectobjused projectused
""".split())


@dataclass
class DelegationSupport:
    platform: str
    version: str = ""
    reason: str = ""

    @property
    def writable(self):
        return not self.reason

    @property
    def permissions(self):
        if not self.writable:
            return frozenset()
        return OPENZFS_23_PERMISSIONS if self.version == "2.3" else OPENZFS_24_PERMISSIONS


def detect_support(output: str, platform: str) -> DelegationSupport:
    if platform not in {"Linux", "FreeBSD"}:
        return DelegationSupport(platform, reason="This platform is read-only in WebZFS.")
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    user_lines = [line for line in lines if line.startswith("zfs-") and not line.startswith("zfs-kmod-")]
    kernel_lines = [line for line in lines if line.startswith("zfs-kmod-")]
    pattern = r"zfs-(?:kmod-)?(\d+)\.(\d+)\.(\d+)(?:[-+_][A-Za-z0-9_.+~-]+)?"
    user = re.fullmatch(pattern, user_lines[0]) if len(user_lines) == 1 else None
    if not user or len(lines) != len(user_lines) + len(kernel_lines):
        return DelegationSupport(platform, reason="Unable to identify the userland ZFS version.")
    version = f"{user[1]}.{user[2]}"
    if version not in {"2.3", "2.4"}:
        return DelegationSupport(platform, version, "Only OpenZFS 2.3 and 2.4 are writable.")
    if any(re.search(r"(?:[-_.~])(?:rc|alpha|beta)\d*", line, re.I) for line in lines):
        return DelegationSupport(platform, version, "Prerelease ZFS builds are read-only.")
    if kernel_lines:
        kernel = re.fullmatch(pattern, kernel_lines[0]) if len(kernel_lines) == 1 else None
        if not kernel or kernel.group(1, 2) != user.group(1, 2):
            return DelegationSupport(platform, version, "Userland and kernel versions disagree or cannot be parsed.")
    return DelegationSupport(platform, version)


def addition_reason(token: str, support: DelegationSupport, dataset_type: str, scope: str) -> str:
    if not support.writable:
        return support.reason
    if token not in support.permissions:
        return "Not in the supported version registry"
    if support.platform == "Linux" and token in LINUX_RESTRICTED:
        return "Cannot be added through the Linux delegation policy"
    if scope != "set":
        if dataset_type == "volume" and token in FILESYSTEM_ONLY:
            return "Filesystem-only permission"
        if dataset_type == "filesystem" and scope == "local" and token in VOLUME_ONLY:
            return "Volume-only permission; use descendant scope for child volumes"
    return ""