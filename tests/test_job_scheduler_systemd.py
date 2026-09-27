from pathlib import Path
import subprocess

import pytest
from jinja2 import Environment, FileSystemLoader

from services import job_scheduler


@pytest.fixture
def systemd_env(tmp_path, monkeypatch):
    generated = tmp_path / "generated"
    generated.mkdir()
    system = tmp_path / "system"
    system.mkdir()
    wants = system / "timers.target.wants"
    wants.mkdir()
    monkeypatch.setattr(job_scheduler, "_generated_unit_dir", lambda: generated)
    monkeypatch.setattr(job_scheduler, "SYSTEMD_UNIT_DIR", str(system))
    monkeypatch.setattr(job_scheduler, "_is_linux", lambda: True)
    monkeypatch.setattr(job_scheduler, "_service_user", lambda: "webzfs")
    monkeypatch.setattr(job_scheduler, "_service_group", lambda: "webzfs")

    calls = []

    def systemctl(*args, check=True):
        calls.append(args)
        if args[0] == "link":
            source = Path(args[1])
            target = system / source.name
            if not target.is_symlink():
                target.symlink_to(source)
        elif args[0] == "enable":
            timer = args[-1]
            target = wants / timer
            if not target.is_symlink():
                target.symlink_to(system / timer)
        elif args[0] == "disable":
            for name in args[1:]:
                if name == "--now":
                    continue
                (wants / name).unlink(missing_ok=True)
                (system / name).unlink(missing_ok=True)

    scheduler = job_scheduler.TaskScheduler()
    monkeypatch.setattr(scheduler, "_systemctl", systemctl)
    return scheduler, generated, system, calls


@pytest.mark.parametrize("task_type", job_scheduler.TASK_TYPES)
def test_register_edit_and_remove_systemd_task(systemd_env, task_type):
    scheduler, generated, system, calls = systemd_env
    base = job_scheduler.unit_base_name(task_type, "example")
    scheduler.register_task(task_type, "example", "0 2 * * *")
    service, timer = scheduler._unit_paths(base)
    assert service.stat().st_mode & 0o777 == 0o644
    assert timer.stat().st_mode & 0o777 == 0o644
    assert (system / service.name).resolve() == service
    assert (system / timer.name).resolve() == timer
    assert "OnCalendar=" in timer.read_text()
    assert "User=webzfs" in service.read_text()
    assert (system / "timers.target.wants" / timer.name).is_symlink()

    scheduler.register_task(task_type, "example", "0 3 * * *")
    assert "OnCalendar=" + job_scheduler.cron_to_oncalendar("0 3 * * *") in timer.read_text()
    assert calls.count(("link", str(service))) == 2
    scheduler.unregister_task(task_type, "example")
    assert not service.exists() and not timer.exists()
    assert not (system / timer.name).is_symlink()
    assert not (system / "timers.target.wants" / timer.name).is_symlink()


def test_collision_does_not_modify_foreign_unit(systemd_env):
    scheduler, generated, system, calls = systemd_env
    base = job_scheduler.unit_base_name("scrub", 1)
    foreign = system / f"{base}.service"
    foreign.write_text("administrator unit")
    with pytest.raises(job_scheduler.TaskSchedulerError, match="refusing to replace"):
        scheduler.register_task("scrub", 1, "0 2 * * *")
    assert foreign.read_text() == "administrator unit"
    assert not calls
    assert "refusing to replace" in scheduler.get_registration_error("scrub", 1)
    assert job_scheduler.TaskScheduler().get_registration_error("scrub", 1)
    with pytest.raises(job_scheduler.TaskSchedulerError, match="refusing to replace"):
        scheduler.unregister_task("scrub", 1)
    assert foreign.read_text() == "administrator unit"


def test_rejects_symlink_in_generated_directory(systemd_env):
    scheduler, generated, system, calls = systemd_env
    foreign = generated / "other.service"
    foreign.write_text("administrator unit")
    (generated / "webzfs-task-scrub-6.service").symlink_to(foreign)
    with pytest.raises(job_scheduler.TaskSchedulerError, match="not a regular generated unit"):
        scheduler.register_task("scrub", 6, "0 2 * * *")
    assert foreign.read_text() == "administrator unit"
    assert not calls
    assert "not a regular generated unit" in scheduler.get_registration_error("scrub", 6)


def test_reconciliation_keeps_failures_and_recovers(systemd_env, monkeypatch):
    scheduler, generated, system, calls = systemd_env
    base = job_scheduler.unit_base_name("scrub", 1)
    foreign = system / f"{base}.service"
    foreign.write_text("administrator unit")
    tasks = [
        {"task_type": "scrub", "task_id": 1, "schedule": "0 2 * * *"},
        {"task_type": "smart", "task_id": 2, "schedule": "0 3 * * *"},
    ]
    monkeypatch.setattr(job_scheduler, "collect_scheduled_tasks", lambda: tasks)
    with pytest.raises(job_scheduler.TaskSchedulerError, match="webzfs-task-scrub-1"):
        scheduler.sync_all()
    assert (generated / "webzfs-task-smart-2.timer").is_file()
    foreign.unlink()
    scheduler.sync_all()
    scheduler.sync_all()
    assert not scheduler.get_registration_error("scrub", 1)
    tasks[:] = [tasks[0]]
    scheduler.sync_all()
    assert not (generated / "webzfs-task-smart-2.timer").exists()


def test_partial_registration_recovers_on_reconciliation(systemd_env, monkeypatch):
    scheduler, generated, system, calls = systemd_env
    original = scheduler._systemctl

    def fail_once(*args, check=True):
        if args[:2] == ("link", str(generated / "webzfs-task-health-5.timer")):
            raise job_scheduler.TaskSchedulerError("link failed")
        return original(*args, check=check)

    monkeypatch.setattr(scheduler, "_systemctl", fail_once)
    with pytest.raises(job_scheduler.TaskSchedulerError, match="link failed"):
        scheduler.register_task("health", 5, "0 2 * * *")
    assert scheduler.get_registration_error("health", 5) == "link failed"
    monkeypatch.setattr(scheduler, "_systemctl", original)
    monkeypatch.setattr(
        job_scheduler, "collect_scheduled_tasks",
        lambda: [{"task_type": "health", "task_id": 5, "schedule": "0 2 * * *"}],
    )
    scheduler.sync_all()
    assert not scheduler.get_registration_error("health", 5)
    assert (system / "webzfs-task-health-5.timer").is_symlink()


def test_reconciliation_removes_disabled_task(systemd_env, monkeypatch):
    scheduler, generated, system, calls = systemd_env
    scheduler.register_task("scrub", 3, "0 2 * * *")
    monkeypatch.setattr(
        job_scheduler, "collect_scheduled_tasks",
        lambda: [{"task_type": "scrub", "task_id": 3, "enabled": False}],
    )
    scheduler.sync_all()
    assert not (generated / "webzfs-task-scrub-3.timer").exists()


def test_bsd_registration_keeps_cron_backend(monkeypatch):
    scheduler = job_scheduler.TaskScheduler()
    monkeypatch.setattr(job_scheduler, "_is_linux", lambda: False)
    called = []
    monkeypatch.setattr(scheduler, "_sync_crontab_block", lambda tasks=None: called.append(tasks))
    scheduler.register_task("scrub", 1, "0 2 * * *")
    scheduler.unregister_task("scrub", 1)
    monkeypatch.setattr(job_scheduler, "collect_scheduled_tasks", lambda: [])
    scheduler.sync_all()
    assert called == [None, None, []]


def test_scheduling_hub_displays_registration_failure():
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    template = Environment(loader=FileSystemLoader(template_dir), autoescape=True).get_template(
        "utils/scheduling/content_partial.jinja"
    )
    task = {
        "task_type": "syncoid", "task_id": 1, "title": "Replication",
        "detail": "pool/data to pool/backup", "schedule": "0 2 * * *",
        "enabled": True, "next_run": "tomorrow", "last_run": None,
        "last_status": "success", "registration_error": "refusing to replace unit",
        "manageable": False, "edit_url": "/zfs/replication/jobs/1",
    }
    rendered = template.render(
        tasks=[task], enabled_count=1,
        counts={"scrub": 0, "smart": 0, "health": 0, "syncoid": 1},
    )
    assert "Registration error" in rendered
    assert "refusing to replace unit" in rendered


def test_policy_has_no_embedded_argument_wildcards():
    policy = (
        Path(__file__).resolve().parents[1] / "sudoers.d" / "webzfs.linux"
    ).read_text()
    for line in policy.splitlines():
        if line.startswith("webzfs ALL="):
            for command in line.split("NOPASSWD: ", 1)[1].split(", "):
                assert all(part == "*" for part in command.split() if "*" in part)
    for command in ("systemctl", "ln", "rm", "mv", "chown", "chmod", "kill"):
        assert "/usr/bin/" + command in policy
    assert "/usr/bin/crontab" not in policy


def test_updater_migrates_only_recognized_legacy_pairs(tmp_path):
    script = (Path(__file__).resolve().parents[1] / "update_linux.sh").read_text()
    migration = script.split("migrate_legacy_units() {", 1)[1].split(
        "# Update CAPTION in .env from .env.example", 1
    )[0]
    migration = "migrate_legacy_units() {" + migration
    migration = migration.replace('unit_dir="/etc/systemd/system"', f'unit_dir="{tmp_path}"')
    mock_systemctl = "systemctl() { return 0; }"
    base = "webzfs-task-scrub-3"
    service = tmp_path / f"{base}.service"
    timer = tmp_path / f"{base}.timer"
    service.write_text(
        "Description=WebZFS Pool scrub task 3\n"
        "ExecStart=/opt/webzfs/.venv/bin/python -m services.task_runner --task-type scrub --task-id 3\n"
    )
    timer.write_text("Description=Timer for WebZFS scrub\nWantedBy=timers.target\n")
    command = "\n".join((migration, mock_systemctl, "migrate_legacy_units"))
    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not service.exists() and not timer.exists()
    assert subprocess.run(["bash", "-c", command], capture_output=True).returncode == 0

    service.write_text("Description=Administrator's scrub task\n")
    timer.write_text("Description=Timer for WebZFS scrub\nWantedBy=timers.target\n")
    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)
    assert result.returncode != 0
    assert service.is_file() and timer.is_file()
    assert "unrecognized" in result.stdout