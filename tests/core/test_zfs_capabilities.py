"""Unit tests for the shared ZFS version and capability helper.

Covers the acceptance criteria of webzfs/webzfs#2: version parsing shapes,
lower-of-userland-and-kmod flag resolution, cached capability probes, the
all-False fallback, and Jinja exposure without a per-render subprocess.
"""

import os

import pytest

# ``config.settings`` validates these at import time, and ``config.templates``
# imports the settings singleton. Defaults keep this test module runnable in a
# bare checkout; the task runner passes the same values explicitly.
os.environ.setdefault("CAPTION", "webzfs")
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")

from services import zfs_capabilities as zc


class _CompletedProcess:
    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.returncode = returncode


@pytest.fixture(autouse=True)
def _clear_cache():
    zc.reset_capabilities_cache()
    yield
    zc.reset_capabilities_cache()


def _install_probe(
    monkeypatch, stdout="", returncode=0, platform_name="Linux", exc=None
):
    """Replace the host probe with a deterministic fake and count invocations."""
    calls = []

    def fake_run(cmd, **kwargs):
        assert isinstance(cmd, list), "run_zfs_command must receive a list argv"
        calls.append(list(cmd))
        if exc is not None:
            raise exc
        return _CompletedProcess(stdout, returncode)

    monkeypatch.setattr(zc, "get_os_type", lambda: platform_name)
    monkeypatch.setattr(zc, "run_zfs_command", fake_run)
    return calls


# ---------------------------------------------------------------------------
# parse_zfs_version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "output, expected",
    [
        # Both lines present and matching.
        ("zfs-2.2.6-1\nzfs-kmod-2.2.6-1\n", ((2, 2, 6), (2, 2, 6))),
        ("zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n", ((2, 4, 1), (2, 4, 1))),
        # Userland only: module not loaded / not reported.
        ("zfs-2.4.1-1\n", ((2, 4, 1), None)),
        ("zfs-2.4.1-1", ((2, 4, 1), None)),
        # Mismatched userland and kernel module versions.
        ("zfs-2.4.1-1\nzfs-kmod-2.2.6-1\n", ((2, 4, 1), (2, 2, 6))),
        ("zfs-2.2.6-1\nzfs-kmod-2.4.1-1\n", ((2, 2, 6), (2, 4, 1))),
        # Kernel module only.
        ("zfs-kmod-2.2.6-1\n", (None, (2, 2, 6))),
        # Blank lines, leading/trailing whitespace and CRLF are tolerated.
        ("\n  zfs-2.3.4-1  \r\n\r\nzfs-kmod-2.3.4-1\r\n\n", ((2, 3, 4), (2, 3, 4))),
        # Garbage.
        ("", (None, None)),
        ("\n\n", (None, None)),
        ("garbage\n", (None, None)),
        ("OpenZFS 2.4.1\n", (None, None)),
        ("zfs-not-a-version\n", (None, None)),
        ("zfs-2.4\n", (None, None)),
        ("zfs-2.4.1.1-1\n", (None, None)),
        ("zfs-2.4.1-1\nextra junk\n", ((2, 4, 1), None)),
    ],
)
def test_parse_zfs_version(output, expected):
    assert zc.parse_zfs_version(output) == expected


def test_parse_zfs_version_ignores_later_duplicate_lines():
    output = "zfs-2.4.1-1\nzfs-2.3.4-1\nzfs-kmod-2.4.1-1\nzfs-kmod-2.3.4-1\n"
    assert zc.parse_zfs_version(output) == ((2, 4, 1), (2, 4, 1))


# ---------------------------------------------------------------------------
# Flag resolution: both sides must meet the floor, lower version wins
# ---------------------------------------------------------------------------


def test_matching_versions_enable_24_flags(monkeypatch):
    _install_probe(monkeypatch, "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n")
    caps = zc.get_capabilities()

    assert caps.platform == "Linux"
    assert caps.userland == (2, 4, 1)
    assert caps.kmod == (2, 4, 1)
    assert caps.effective_version == (2, 4, 1)
    assert caps.zfs_rewrite is True
    assert caps.rewrite_preserve_birth is True
    assert caps.all_pools_flag is True
    assert caps.scrub_time_range is True
    assert caps.default_quotas is True
    assert caps.json_output is True
    assert caps.draid is True
    assert caps.compatibility_property is True
    assert caps.zfs_zone is False  # FreeBSD-only flag on Linux


def test_userland_only_uses_userland_version(monkeypatch):
    _install_probe(monkeypatch, "zfs-2.4.1-1\n")
    caps = zc.get_capabilities()

    assert caps.kmod is None
    assert caps.effective_version == (2, 4, 1)
    assert caps.zfs_rewrite is True
    assert caps.json_output is True


def test_zfs_rewrite_floor_is_2_3_4(monkeypatch):
    """``zfs rewrite`` landed in 2.3.4; ``-P`` only in 2.4.0.

    Regression guard: a 2.3.4-2.3.9 host supports the subcommand but not the
    ``-P`` flag, so the two floors must differ.
    """
    _install_probe(monkeypatch, "zfs-2.3.4-1\nzfs-kmod-2.3.4-1\n")
    caps = zc.get_capabilities()

    assert caps.effective_version == (2, 3, 4)
    assert caps.zfs_rewrite is True
    assert caps.rewrite_preserve_birth is False


@pytest.mark.parametrize(
    "output",
    [
        "zfs-2.3.3-1\nzfs-kmod-2.3.3-1\n",
        # Lower side wins: a 2.3.4 userland with a 2.3.3 module lacks rewrite.
        "zfs-2.3.4-1\nzfs-kmod-2.3.3-1\n",
    ],
)
def test_zfs_rewrite_below_floor_is_false(monkeypatch, output):
    _install_probe(monkeypatch, output)
    caps = zc.get_capabilities()

    assert caps.zfs_rewrite is False
    assert caps.rewrite_preserve_birth is False


@pytest.mark.parametrize(
    "output",
    [
        "zfs-2.4.1-1\nzfs-kmod-2.2.6-1\n",
        "zfs-2.2.6-1\nzfs-kmod-2.4.1-1\n",
    ],
)
def test_mismatched_versions_resolve_flags_to_lower(monkeypatch, output):
    _install_probe(monkeypatch, output)
    caps = zc.get_capabilities()

    assert caps.effective_version == (2, 2, 6)
    # Floors above 2.2.6 must be False on both sides.
    assert caps.zfs_rewrite is False
    assert caps.rewrite_preserve_birth is False
    assert caps.all_pools_flag is False
    assert caps.scrub_time_range is False
    assert caps.default_quotas is False
    assert caps.json_output is False
    assert caps.raidz_expansion is False
    assert caps.fast_dedup is False
    assert caps.ddtprune is False
    assert caps.direct_io is False
    assert caps.reguid_with_guid is False
    # Floors at or below 2.2.6 remain True.
    assert caps.scrub_error is True
    assert caps.checksum_blake3 is True
    assert caps.redundant_metadata_some_none is True
    assert caps.initialize_uninit is True
    assert caps.draid is True
    assert caps.compatibility_property is True
    assert caps.prefetch_ddt is True


def test_freebsd_zone_flag(monkeypatch):
    _install_probe(
        monkeypatch, "zfs-2.2.6-1\nzfs-kmod-2.2.6-1\n", platform_name="FreeBSD"
    )
    caps = zc.get_capabilities()
    assert caps.platform == "FreeBSD"
    assert caps.zfs_zone is True


# ---------------------------------------------------------------------------
# Fallbacks: failed probe, garbage, unsupported platform -> all False
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stdout, returncode",
    [
        ("", 1),
        ("zfs version: command not found\n", 127),
        ("garbage\n", 0),
        ("", 0),
    ],
)
def test_unparseable_or_failed_probe_is_all_false(monkeypatch, stdout, returncode):
    _install_probe(monkeypatch, stdout, returncode=returncode)
    caps = zc.get_capabilities()

    assert caps.userland is None
    assert caps.kmod is None
    assert caps.effective_version is None
    assert all(
        value is False for value in caps.as_dict().values() if isinstance(value, bool)
    )


def test_probe_exception_is_all_false(monkeypatch):
    _install_probe(monkeypatch, exc=RuntimeError("boom"))
    caps = zc.get_capabilities()
    assert caps.effective_version is None
    assert all(
        value is False for value in caps.as_dict().values() if isinstance(value, bool)
    )


def test_unsupported_platform_is_all_false(monkeypatch):
    calls = _install_probe(
        monkeypatch,
        "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n",
        platform_name="NetBSD",
    )
    caps = zc.get_capabilities()

    assert caps.platform == "NetBSD"
    assert caps.effective_version is None
    assert all(
        value is False for value in caps.as_dict().values() if isinstance(value, bool)
    )
    # NetBSD must not even spawn the probe.
    assert calls == []


# ---------------------------------------------------------------------------
# Caching: templates/requests must not spawn a subprocess per render
# ---------------------------------------------------------------------------


def test_capabilities_are_cached(monkeypatch):
    calls = _install_probe(monkeypatch, "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n")

    first = zc.get_capabilities()
    second = zc.get_capabilities()

    assert first is second
    assert calls == [["zfs", "version"]]

    refreshed = zc.get_capabilities(force_refresh=True)
    assert refreshed is not first
    assert calls == [["zfs", "version"], ["zfs", "version"]]


def test_lazy_caps_global_probes_once(monkeypatch):
    calls = _install_probe(monkeypatch, "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n")

    assert zc.caps.zfs_rewrite is True
    assert zc.caps.json_output is True
    assert calls == [["zfs", "version"]]


def test_lazy_caps_rejects_unknown_attribute():
    with pytest.raises(AttributeError):
        getattr(zc.caps, "not_a_capability")


def test_templates_expose_caps_without_per_render_subprocess(monkeypatch):
    calls = _install_probe(monkeypatch, "zfs-2.4.1-1\nzfs-kmod-2.4.1-1\n")

    from config.templates import templates

    assert templates.env.globals["caps"] is zc.caps

    template = templates.env.from_string(
        "{% if caps.zfs_rewrite %}rewrite{% else %}no-rewrite{% endif %}"
    )
    assert template.render() == "rewrite"
    assert template.render() == "rewrite"
    assert calls == [["zfs", "version"]]


def test_template_caps_hide_controls_when_unavailable(monkeypatch):
    _install_probe(monkeypatch, "", returncode=1)

    from config.templates import templates

    template = templates.env.from_string(
        "{% if caps.zfs_rewrite %}rewrite{% else %}no-rewrite{% endif %}"
    )
    assert template.render() == "no-rewrite"


# ---------------------------------------------------------------------------
# Pool features
# ---------------------------------------------------------------------------

POOL_FEATURES_OUTPUT = (
    "feature@async_destroy\tenabled\n"
    "feature@raidz_expansion\tenabled\n"
    "feature@fast_dedup\tactive\n"
    "feature@draid\tdisabled\n"
    "size\t10737418240\n"
    "health\tONLINE\n"
)


def test_get_pool_features_filters_feature_rows(monkeypatch):
    calls = _install_probe(monkeypatch, POOL_FEATURES_OUTPUT)

    features = zc.get_pool_features("tank")

    assert features == {
        "async_destroy": "enabled",
        "raidz_expansion": "enabled",
        "fast_dedup": "active",
        "draid": "disabled",
    }
    assert calls == [["zpool", "get", "-H", "-o", "property,value", "all", "tank"]]


def test_get_pool_features_returns_empty_on_failure(monkeypatch):
    _install_probe(monkeypatch, "", returncode=1)
    assert zc.get_pool_features("tank") == {}


@pytest.mark.parametrize("pool", ["", "tank;rm -rf /", "-o", "tank pool", 42])
def test_get_pool_features_rejects_invalid_pool(monkeypatch, pool):
    calls = _install_probe(monkeypatch, POOL_FEATURES_OUTPUT)
    with pytest.raises(ValueError):
        zc.get_pool_features(pool)
    assert calls == []


@pytest.mark.parametrize(
    "feature, expected",
    [
        ("raidz_expansion", True),
        ("fast_dedup", True),
        ("draid", False),
        ("missing_feature", False),
    ],
)
def test_pool_feature_enabled(monkeypatch, feature, expected):
    _install_probe(monkeypatch, POOL_FEATURES_OUTPUT)
    assert zc.pool_feature_enabled("tank", feature) is expected


@pytest.mark.parametrize("feature", ["", "Raidz Expansion", "raidz-expansion", "../x"])
def test_pool_feature_enabled_rejects_invalid_name(monkeypatch, feature):
    calls = _install_probe(monkeypatch, POOL_FEATURES_OUTPUT)
    with pytest.raises(ValueError):
        zc.pool_feature_enabled("tank", feature)
    assert calls == []
