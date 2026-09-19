"""Foreground local-terminal commands in transient systemd scopes.

Hermes gateway is a long-lived systemd user service. Foreground ``execute()``
commands spawned by :class:`tools.environments.local.LocalEnvironment` inherit
the gateway service cgroup, so builds, dev servers, LSPs and their page cache
get charged to ``hermes-gateway`` (and a runaway child shares its fate).
When ``terminal.systemd_scope`` is enabled, wrap those commands with
``systemd-run --user --scope`` so the workload runs in its own transient scope
while stdout/stderr and exit codes still flow through the original Popen pipe.

Scope machinery for BACKGROUND processes (kanban/cron workers) already lives in
``tools/process_registry.py`` and is unconditional there. This module only owns
the foreground path, which is opt-in: builds may legitimately need lots of RAM,
so — unlike the worker wrapper — no ``MemoryMax`` is applied, only accounting.

Config (terminal.systemd_scope, default false) or env override
``HERMES_TERMINAL_SYSTEMD_SCOPE`` (true forces on, false forces off).
"""

from __future__ import annotations

import os
import platform
import re
import uuid
from typing import Mapping, Sequence


_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}

# Verdict cache keyed per profile home (one process serves many profiles under
# multiplex; hermes_home_key() is scope-aware). Populated only from successful
# config reads — a transient read error stays uncached so a later spawn retries.
# Config changes take effect on the next gateway restart, like most settings.
_CONFIG_VERDICT_CACHE: dict[str, bool] = {}


def _config_enabled() -> bool:
    """Return terminal.systemd_scope, defaulting false on config/read errors."""
    try:
        from hermes_constants import hermes_home_key

        key = hermes_home_key()
    except Exception:
        key = ""
    cached = _CONFIG_VERDICT_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        terminal_cfg = cfg.get("terminal") or {}
        value = terminal_cfg.get("systemd_scope", False)
    except Exception:
        return False

    if isinstance(value, bool):
        verdict = value
    elif isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUE_VALUES:
            verdict = True
        elif lowered in _FALSE_VALUES:
            verdict = False
        else:
            verdict = bool(value)
    else:
        verdict = bool(value)
    _CONFIG_VERDICT_CACHE[key] = verdict
    return verdict


def systemd_scope_enabled(env: Mapping[str, str] | None = None) -> bool:
    """Whether host terminal subprocesses should be wrapped in user scopes."""
    if platform.system() != "Linux":
        return False

    env_map = env if env is not None else os.environ
    override = str(env_map.get("HERMES_TERMINAL_SYSTEMD_SCOPE", "")).strip().lower()
    if override in _FALSE_VALUES:
        return False
    config_value = True if override in _TRUE_VALUES else _config_enabled()
    if not config_value:
        return False

    # ``systemd-run --user --scope`` needs a reachable user bus; the shared probe
    # (cached process-wide) verifies the binary AND the bus with a real spawn.
    from tools.process_registry import _systemd_run_user_scope_available

    return _systemd_run_user_scope_available()


def _safe_unit_fragment(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")
    return safe[:32] or "cmd"


def wrap_in_systemd_scope(
    args: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    unit_prefix: str = "hermes-terminal",
) -> list[str]:
    """Return args wrapped with ``systemd-run --user --scope`` when enabled.

    ``systemd-run --scope`` is synchronous: it preserves the wrapped process'
    stdout/stderr and exit status, while moving the actual workload and its
    descendants out of the parent service cgroup. Accounting-only by design —
    no ``MemoryMax`` (foreground builds may need the host's full memory; the
    worker-capped argv lives in ``tools.process_registry._systemd_scope_argv``).
    """
    arg_list = list(args)
    if not systemd_scope_enabled(env):
        return arg_list

    import shutil

    binary = shutil.which("systemd-run")
    if binary is None:
        return arg_list
    unit = f"{_safe_unit_fragment(unit_prefix)}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    return [
        binary, "--user", "--scope", "--quiet", "--unit", unit, "--collect",
        "--property", "MemoryAccounting=yes",
        "--property", "CPUAccounting=yes",
        "--", *arg_list,
    ]
