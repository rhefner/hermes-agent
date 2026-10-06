"""Explicit maintenance entry/exit. No config rewrites, restarts or rollback.

Run: python -m hermes_cli.maintenance_inference --help
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import hermes_bootstrap  # noqa: F401 -- activate the managed runtime before provider imports
from agent import maintenance_inference as guard


def profiles():
    root = Path.home() / ".hermes"
    return [root.resolve(), *sorted(p.resolve() for p in (root / "profiles").glob("*") if p.is_dir())]


def write_state(value):
    root = guard.directory()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix=".state-", dir=root)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(path, root / "state.json")
        d = os.open(root, os.O_RDONLY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(path):
            os.unlink(path)


@contextlib.contextmanager
def locked():
    root = guard.directory()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (root / "control.lock").open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def probe(model):
    """Authenticated completion through the ACTUAL runtime and auxiliary clients."""
    from hermes_cli.runtime_provider import resolve_runtime_provider
    from agent.auxiliary_client import resolve_provider_client
    runtime = resolve_runtime_provider(requested="openai-codex", target_model=model,
                                       explicit_base_url=guard.CLOUD_BASE)
    if (runtime.get("provider") != "openai-codex" or not guard.cloud_url(runtime.get("base_url"))
            or runtime.get("api_mode") != "codex_responses"):
        raise guard.MaintenanceIsolationError("Probe runtime is not independent Codex cloud")
    guard.require_route(runtime.get("provider"), model, runtime.get("base_url"),
                        runtime.get("api_mode"), resolved=True)
    client, actual = resolve_provider_client("openai-codex", model,
        explicit_base_url=guard.CLOUD_BASE, api_mode="codex_responses")
    if (client is None or actual != model or not guard.cloud_url(getattr(client, "base_url", None))
            or any(os.environ.get(k) for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"))):
        raise guard.MaintenanceIsolationError("Independent external client/auth unavailable or proxy configured")
    result = client.chat.completions.create(model=model, messages=[
        {"role": "user", "content": "Reply with exactly MAINTENANCE_OK."}])
    text = result.choices[0].message.content
    if not text or text.strip() != "MAINTENANCE_OK":
        raise guard.MaintenanceIsolationError("Independent external completion did not pass")


def probe_profile(home, model):
    env = dict(os.environ, HERMES_HOME=str(home))
    # Never inherit turn/session provider overrides into this independent probe.
    from gateway.session_context import _VAR_MAP
    for key in _VAR_MAP:
        env.pop(key, None)
    result = subprocess.run([sys.executable, "-m", "hermes_cli.maintenance_inference", "probe", "--model", model],
        cwd=str(Path(__file__).resolve().parent.parent), env=env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
    # Deliberately do not echo SDK exceptions (may contain tokens, URLs or payloads).
    if result.returncode != 0 or result.stdout.decode().strip().splitlines()[-1:] != ["MAINTENANCE_PROBE_OK"]:
        raise guard.MaintenanceIsolationError(f"External authenticated probe failed for profile {home.name}; repair profile Codex auth and retry entry. Isolation remains closed.")


def enter(model, *, drained):
    if not drained:
        raise guard.MaintenanceIsolationError("Drain existing inference and load this runtime in ALL processes first; then pass --drained. This command never restarts services.")
    if not model or not model.strip():
        raise guard.MaintenanceIsolationError("An explicit independent external model is required")
    with locked():
        # Persist fail-closed BEFORE any probe; failure/interruption is not rollback.
        value = {"version": 1, "phase": "verifying", "provider": "openai-codex", "model": model,
                 "base_url": guard.CLOUD_BASE, "verified_profiles": [], "entered_at": time.time()}
        write_state(value)
        from agent.serving_admission import observe
        observe(model)  # Real closed accounting/runtime/executor, not --drained.
        homes = profiles()
        for home in homes:
            probe_profile(home, model)
            value["verified_profiles"].append(str(home))
            write_state(value)
        if profiles() != homes:
            raise guard.MaintenanceIsolationError("Profile inventory changed during verification; retry entry")
        observe(model)  # Recheck after probes before granting active isolation.
        value["phase"] = "active"
        write_state(value)
        if guard.state() != value:
            raise guard.MaintenanceIsolationError("Maintenance state readback failed")
        return value


def exit_maintenance(approval):
    if not approval or not approval.strip():
        raise guard.MaintenanceIsolationError("Explicit case-specific Hef post-QA approval is required; no automatic restoration")
    with locked():
        current = guard.state()
        if current is None:
            raise guard.MaintenanceIsolationError("Maintenance is not active")
        write_state({"version": 1, "phase": "normal", "approval": approval, "exited_at": time.time()})
        if guard.state() is not None:
            raise guard.MaintenanceIsolationError("Exit readback failed")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("enter", "exit", "status", "probe"))
    p.add_argument("--model")
    p.add_argument("--drained", action="store_true")
    p.add_argument("--approved-by-hef")
    a = p.parse_args()
    try:
        if a.action == "enter":
            print(json.dumps(enter(a.model, drained=a.drained), sort_keys=True))
        elif a.action == "exit":
            exit_maintenance(a.approved_by_hef)
            print("NORMAL preferences released by explicit approval; no configuration changed")
        elif a.action == "probe":
            probe(a.model)
            print("MAINTENANCE_PROBE_OK")
        else:
            print(json.dumps(guard.state() or {"phase": "normal"}, sort_keys=True))
        return 0
    except Exception as exc:
        # Only our safe diagnostics are printed, never arbitrary SDK messages.
        print(str(exc) if isinstance(exc, guard.MaintenanceIsolationError) else
              "Maintenance operation failed; state retained. Inspect auth/runtime safely before retrying.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
