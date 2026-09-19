"""Tests for the foreground systemd-scope wrapper (local carry).

Background workers get scopes unconditionally from tools/process_registry.py;
this module owns the opt-in terminal.systemd_scope foreground path.
"""

import platform

import pytest

from tools import systemd_scope


@pytest.fixture(autouse=True)
def _linux_only(monkeypatch):
    if platform.system() != "Linux":
        pytest.skip("systemd-run --user is Linux-only")
    monkeypatch.setattr(systemd_scope.platform, "system", lambda: "Linux")


def test_disabled_by_default_leaves_argv_unwrapped(monkeypatch):
    # Fresh temp HERMES_HOME (autouse conftest fixture): terminal.systemd_scope unset → off.
    argv = ["/bin/bash", "-c", "echo hi"]
    assert systemd_scope.wrap_in_systemd_scope(argv) == argv


def test_env_override_false_forces_off(monkeypatch):
    monkeypatch.setenv("HERMES_TERMINAL_SYSTEMD_SCOPE", "false")
    assert systemd_scope.systemd_scope_enabled() is False


def test_env_override_true_wraps_when_probe_available(monkeypatch):
    import tools.process_registry as pr

    monkeypatch.setenv("HERMES_TERMINAL_SYSTEMD_SCOPE", "true")
    monkeypatch.setattr(pr, "_systemd_run_user_scope_available", lambda: True)
    wrapped = systemd_scope.wrap_in_systemd_scope(["/bin/bash", "-c", "make all"], unit_prefix="hermes-terminal-fg")
    assert wrapped[0].endswith("systemd-run")
    assert "--scope" in wrapped and "--user" in wrapped
    assert wrapped[-4:] == ["--", "/bin/bash", "-c", "make all"]
    # Accounting-only foreground wrapper: no MemoryMax cap.
    assert not any("MemoryMax" in part for part in wrapped)
    assert any("MemoryAccounting" in part for part in wrapped)
    assert any("CPUAccounting" in part for part in wrapped)


def test_env_override_true_without_bus_stays_unwrapped(monkeypatch):
    import tools.process_registry as pr

    monkeypatch.setenv("HERMES_TERMINAL_SYSTEMD_SCOPE", "true")
    monkeypatch.setattr(pr, "_systemd_run_user_scope_available", lambda: False)
    argv = ["/bin/bash", "-c", "echo hi"]
    assert systemd_scope.wrap_in_systemd_scope(argv) == argv


def test_unit_name_is_sanitized_and_unique(monkeypatch):
    import tools.process_registry as pr

    monkeypatch.setenv("HERMES_TERMINAL_SYSTEMD_SCOPE", "true")
    monkeypatch.setattr(pr, "_systemd_run_user_scope_available", lambda: True)
    one = systemd_scope.wrap_in_systemd_scope(["/bin/true"], unit_prefix="bad prefix/chars!")
    two = systemd_scope.wrap_in_systemd_scope(["/bin/true"], unit_prefix="bad prefix/chars!")
    name_one = one[one.index("--unit") + 1]
    name_two = two[two.index("--unit") + 1]
    assert " " not in name_one and "/" not in name_one
    assert name_one != name_two


def test_non_linux_platform_never_wraps(monkeypatch):
    monkeypatch.setattr(systemd_scope.platform, "system", lambda: "Windows")
    monkeypatch.setenv("HERMES_TERMINAL_SYSTEMD_SCOPE", "true")
    argv = ["/bin/bash", "-c", "echo hi"]
    assert systemd_scope.wrap_in_systemd_scope(argv) == argv
