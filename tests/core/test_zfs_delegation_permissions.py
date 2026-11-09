"""Characterisation tests for delegation support detection.

These pin the behaviour that must not change when ``detect_support`` delegates
version parsing to :func:`services.zfs_capabilities.parse_zfs_version`.
"""

import pytest

from core.zfs_delegation_permissions import (
    OPENZFS_23_PERMISSIONS,
    OPENZFS_24_PERMISSIONS,
    DelegationSupport,
    detect_support,
)

VERSION_23 = "zfs-2.3.4-1\nzfs-kmod-2.3.4-1\n"
VERSION_24 = "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n"


@pytest.mark.parametrize(
    "output, expected_version, expected_permissions",
    [
        (VERSION_23, "2.3", OPENZFS_23_PERMISSIONS),
        (VERSION_24, "2.4", OPENZFS_24_PERMISSIONS),
    ],
)
def test_linux_writable(output, expected_version, expected_permissions):
    support = detect_support(output, "Linux")
    assert support == DelegationSupport("Linux", expected_version, "")
    assert support.writable is True
    assert support.permissions == expected_permissions


def test_freebsd_writable():
    support = detect_support(VERSION_24, "FreeBSD")
    assert support == DelegationSupport("FreeBSD", "2.4", "")
    assert support.permissions == OPENZFS_24_PERMISSIONS


def test_userland_only_without_kmod_is_writable():
    support = detect_support("zfs-2.3.4-1\n", "Linux")
    assert support == DelegationSupport("Linux", "2.3", "")
    assert support.writable is True


@pytest.mark.parametrize("platform_name", ["NetBSD", "OpenBSD", "Darwin", "Windows"])
def test_unsupported_platform_is_read_only(platform_name):
    support = detect_support(VERSION_24, platform_name)
    assert support == DelegationSupport(
        platform_name, "", "This platform is read-only in WebZFS."
    )
    assert support.writable is False
    assert support.permissions == frozenset()


def test_unsupported_version_is_read_only():
    support = detect_support("zfs-2.2.6-1\nzfs-kmod-2.2.6-1\n", "Linux")
    assert support == DelegationSupport(
        "Linux", "2.2", "Only OpenZFS 2.3 and 2.4 are writable."
    )
    assert support.writable is False


def test_prerelease_is_read_only():
    output = "zfs-2.4.0-rc1\nzfs-kmod-2.4.0-rc1\n"
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "2.4", "Prerelease ZFS builds are read-only."
    )
    assert support.writable is False


def test_userland_kernel_disagree_is_read_only():
    output = "zfs-2.4.1-1\nzfs-kmod-2.2.6-1\n"
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "2.4", "Userland and kernel versions disagree or cannot be parsed."
    )
    assert support.writable is False


def test_two_userland_lines_is_read_only():
    output = "zfs-2.4.1-1\nzfs-2.3.4-1\nzfs-kmod-2.4.1-1\n"
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "", "Unable to identify the userland ZFS version."
    )
    assert support.writable is False


def test_two_kernel_lines_is_read_only():
    output = "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\nzfs-kmod-2.4.1-1\n"
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "2.4", "Userland and kernel versions disagree or cannot be parsed."
    )
    assert support.writable is False


def test_unexpected_extra_line_is_read_only():
    output = "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\nunexpected\n"
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "", "Unable to identify the userland ZFS version."
    )
    assert support.writable is False


@pytest.mark.parametrize(
    "output",
    [
        "",
        "\n\n",
        "zfs-not-a-version\n",
        "zfs-kmod-2.4.1-1\n",
        "OpenZFS 2.4.1\n",
        "zfs-2.4\n",
    ],
)
def test_garbage_or_missing_userland_is_read_only(output):
    support = detect_support(output, "Linux")
    assert support == DelegationSupport(
        "Linux", "", "Unable to identify the userland ZFS version."
    )
    assert support.writable is False


def test_platform_check_precedes_parsing():
    support = detect_support("garbage\n", "NetBSD")
    assert support.reason == "This platform is read-only in WebZFS."
