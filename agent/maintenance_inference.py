"""Fleet-wide inference isolation, independent of profile/config/session selection.

This is an operational guard, not a sandbox against arbitrary same-user Python or
shell code. State is read on every request, never expires, and is never rolled back.
Only the pinned, authenticated Codex cloud route is currently supported. Unknown
transports fail closed rather than guessing whether they share maintained hardware.
"""
from __future__ import annotations

import functools
import json
import os
import stat
from pathlib import Path
from urllib.parse import urlsplit

import hermes_constants
from hermes_constants import get_hermes_home

CLOUD_BASE = "https://chatgpt.com/backend-api/codex"
NOTICE = ("Maintenance inference isolation refused an unverified route. Use the verified "
          "openai-codex provider/model shown by `python -m hermes_cli.maintenance_inference status`; "
          "do not fall back to local inference. New profiles require maintenance re-entry verification.")


class MaintenanceIsolationError(RuntimeError):
    pass


def directory() -> Path:
    # Intentionally NOT HERMES_HOME, profile ContextVars or config.yaml.
    # Use the native platform seam (also hermetically isolated by the test
    # harness), with the suffix removed so HERMES_DATA_DIR_SUFFIX cannot opt out.
    native = hermes_constants._get_platform_default_hermes_home()
    return native.with_name("hermes" if os.name == "nt" else ".hermes") / "maintenance-inference"


def state():
    root = directory()
    try:
        st = root.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise MaintenanceIsolationError("Maintenance isolation storage unavailable; refusing inference") from None
    try:
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise ValueError()
        path = root / "state.json"
        fs = path.lstat()
        if not stat.S_ISREG(fs.st_mode) or fs.st_uid != os.getuid() or fs.st_mode & 0o077:
            raise ValueError()
        value = json.loads(path.read_text())
        if value.get("version") != 1 or value.get("phase") not in {"normal", "verifying", "active"}:
            raise ValueError()
        if value["phase"] == "normal":
            return None
        if (value.get("provider") != "openai-codex" or not isinstance(value.get("model"), str)
                or not value["model"].strip() or value.get("base_url") != CLOUD_BASE
                or not isinstance(value.get("verified_profiles"), list)):
            raise ValueError()
        return value
    except (OSError, ValueError, TypeError, AttributeError):
        raise MaintenanceIsolationError("Maintenance isolation state is unreadable; refusing inference. Repair state explicitly; never delete it to restore routing.") from None


def cloud_url(url):
    try:
        u = urlsplit(str(url))
        return (u.scheme == "https" and u.hostname == "chatgpt.com" and u.port in (None, 443)
                and not u.username and not u.password and not u.query and not u.fragment
                and (u.path.rstrip("/") == "/backend-api/codex"
                     or u.path.rstrip("/") == "/backend-api/codex/responses"))
    except (ValueError, TypeError):
        return False


def require_route(provider, model, base_url=None, api_mode=None, *, resolved=False):
    s = state()
    if s is None:
        return
    if (provider != s["provider"] or model != s["model"]
            or (base_url and not cloud_url(base_url))
            or (resolved and not cloud_url(base_url))
            or api_mode not in (None, "", "codex_responses")):
        raise MaintenanceIsolationError(NOTICE)
    # During entry only the pinned independent route can probe. No dispatch is
    # allowed until ALL discovered profiles have passed an authenticated call.
    if s["phase"] == "active" and str(Path(get_hermes_home()).resolve()) not in s["verified_profiles"]:
        raise MaintenanceIsolationError(NOTICE)


def require_agent(agent, kwargs=None):
    require_route(getattr(agent, "provider", None), (kwargs or {}).get("model", getattr(agent, "model", None)),
                  getattr(agent, "base_url", None), getattr(agent, "api_mode", None), resolved=True)
    if state() and getattr(agent, "acp_command", None):
        raise MaintenanceIsolationError(NOTICE)


def guarded_agent_call(fn):
    @functools.wraps(fn)
    def call(agent, api_kwargs, *args, **kwargs):
        require_agent(agent, api_kwargs)
        return fn(agent, api_kwargs, *args, **kwargs)
    return call


def check_http_request(request):
    """Request hook on Hermes-owned SDK transports, including cached clients.

    Checks every physical retry/redirect, not just provider names at resolution.
    Never print headers, request payloads, credentials or rejected URLs.
    """
    s = state()
    if s is None:
        return
    if not cloud_url(request.url) or any(os.environ.get(k) for k in (
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")):
        raise MaintenanceIsolationError(NOTICE)
    if request.method == "POST":
        try:
            body = json.loads(request.content)
            model = body["model"]
        except Exception:
            raise MaintenanceIsolationError(NOTICE) from None
        require_route(s["provider"], model, str(request.url), "codex_responses", resolved=True)


async def check_async_http_request(request):
    check_http_request(request)


def auxiliary_route(provider, model, base_url, api_key, api_mode):
    s = state()
    if s is None:
        return provider, model, base_url, api_key, api_mode
    # No carryover of local URL/key/transport. Resolve authentication in the
    # destination profile using the existing Codex auth path.
    return s["provider"], s["model"], s["base_url"], None, "codex_responses"


def dispatch_route(profile_home):
    s = state()
    if s is None:
        return None
    if s["phase"] != "active" or not profile_home or str(Path(profile_home).resolve()) not in s["verified_profiles"]:
        raise MaintenanceIsolationError(NOTICE)
    return s


def refuse_unverified_extension():
    if state():
        raise MaintenanceIsolationError("Maintenance isolation blocks unverified plugin inference/media/memory backends. Use core verified external inference or explicit read-only QA outside agent inference.")
