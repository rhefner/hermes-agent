"""Maintenance call-site regressions. No live inference or production state writes."""
import asyncio
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from agent import maintenance_inference as g
from hermes_cli import maintenance_inference as ctl

MODEL = "independent-test-model"
# Enumerate rather than freeze today's profile set. The root default and a named
# profiles/default are separate homes and both must be protected.
DISCOVERED_HOMES = ctl.profiles()


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    root = tmp_path / "maintenance-inference"
    home = tmp_path / "profile"
    monkeypatch.setattr(g, "directory", lambda: root)
    monkeypatch.setattr(g, "get_hermes_home", lambda: home)
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(k, raising=False)
    return root, home


def activate(home, phase="active"):
    value = {"version": 1, "phase": phase, "provider": "openai-codex", "model": MODEL,
             "base_url": g.CLOUD_BASE, "verified_profiles": [str(home.resolve())]}
    ctl.write_state(value)
    return value


def test_normal_routes_unchanged(isolated):
    route = ("custom:hef", "secondary", "http://llm-lab:8002/v1", "sentinel", "chat_completions")
    assert g.auxiliary_route(*route) == route
    g.require_route(*route[:3], route[4], resolved=True)
    assert g.dispatch_route("anything") is None


@pytest.mark.parametrize("home", DISCOVERED_HOMES, ids=lambda p: str(p))
def test_every_discovered_profile_and_session_override(isolated, monkeypatch, home):
    # Preserve the discovered profile identity, but run against a private home.
    home = isolated[1] / ("root-default" if home == DISCOVERED_HOMES[0] else home.name)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(g, "get_hermes_home", lambda: home)
    activate(home)
    from hermes_cli.runtime_provider import resolve_runtime_provider
    for provider, model, url in [
        ("custom:hef", "primary", "http://llm-lab:8002/v1"),
        ("custom:hef", "secondary", "http://spark-3:8000/v1"),
        ("openai-codex", MODEL, "http://192.168.0.158:8002/v1"),
    ]:
        with pytest.raises(g.MaintenanceIsolationError):
            resolve_runtime_provider(requested=provider, target_model=model, explicit_base_url=url)
    g.require_route("openai-codex", MODEL, g.CLOUD_BASE, "codex_responses", resolved=True)


@pytest.mark.parametrize("provider", ["primary", "secondary", "hef", "custom:hef", "custom", "vllm", "ollama", "llamacpp", "auto", "moa", "openai", "copilot-acp"])
def test_aliases_and_unknown_transports_fail_closed(isolated, provider):
    activate(isolated[1])
    with pytest.raises(g.MaintenanceIsolationError):
        g.require_route(provider, MODEL, g.CLOUD_BASE, resolved=True)


@pytest.mark.parametrize("url", ["http://llm-lab:8002/v1", "http://spark-1:8000/v1", "http://spark-2:8000/v1",
    "http://spark-3:8000/v1", "http://192.168.0.158:11434/v1", "http://127.0.0.1/v1", "http://[::1]:8000/v1",
    "https://chatgpt.com.evil.invalid/backend-api/codex", "https://chatgpt.com@localhost/backend-api/codex",
    "https://chatgpt.com:8002/backend-api/codex", "https://chatgpt.com/backend-api/codex/../../v1",
    "https://chatgpt.com/backend-api/codex?token=not-a-secret", "https://public-facade.invalid/v1"])
def test_direct_local_and_disguised_urls(isolated, url):
    activate(isolated[1])
    with pytest.raises(g.MaintenanceIsolationError):
        g.require_route("openai-codex", MODEL, url, resolved=True)


def test_new_profile_refused(isolated):
    activate(isolated[1])
    with pytest.raises(g.MaintenanceIsolationError):
        g.dispatch_route(isolated[1] / "future")


def test_profile_env_cannot_move_guard(monkeypatch):
    before = g.directory()
    monkeypatch.setenv("HERMES_HOME", "/arbitrary/future/profile")
    assert g.directory() == before


@pytest.mark.parametrize("payload", ["{", "null", '{"version":1,"phase":"bogus"}', '{"version":1,"phase":"active"}'])
def test_malformed_state_fails_closed(isolated, payload):
    root, _ = isolated
    root.mkdir(mode=0o700)
    (root / "state.json").write_text(payload)
    (root / "state.json").chmod(0o600)
    with pytest.raises(g.MaintenanceIsolationError):
        g.state()


def test_missing_state_in_enrolled_directory_fails_closed(isolated):
    isolated[0].mkdir()
    with pytest.raises(g.MaintenanceIsolationError):
        g.state()


def test_persisted_state_fresh_interpreter(isolated):
    value = activate(isolated[1])
    script = ("from pathlib import Path; from agent import maintenance_inference as g; "
              f"g.directory=lambda:Path({str(isolated[0])!r}); "
              "import json; print(json.dumps(g.state(),sort_keys=True))")
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) == value


@pytest.mark.parametrize("name", ["direct_api_call", "interruptible_api_call", "interruptible_streaming_api_call"])
def test_main_physical_call_sites_block_session_switch(isolated, name):
    activate(isolated[1])
    from agent import chat_completion_helpers as h
    stale = SimpleNamespace(provider="custom:hef", model="secondary", base_url="http://spark-3:8000/v1", api_mode="chat_completions")
    with pytest.raises(g.MaintenanceIsolationError):
        getattr(h, name)(stale, {"model": "primary"})


def test_cached_http_client_and_redirect_rechecked(isolated, monkeypatch):
    from agent.process_bootstrap import build_keepalive_http_client
    client = build_keepalive_http_client("http://llm-lab:8002/v1")
    assert client is not None
    sent = []
    transport = httpx.MockTransport(lambda r: sent.append(r) or httpx.Response(200))
    client._mounts = {}
    client._transport = transport
    activate(isolated[1])
    with pytest.raises(g.MaintenanceIsolationError):
        client.post("http://llm-lab:8002/v1/chat/completions", json={"model": "primary"})
    assert not sent
    client.post(g.CLOUD_BASE + "/responses", json={"model": MODEL})
    assert len(sent) == 1
    with pytest.raises(g.MaintenanceIsolationError):
        client.post(g.CLOUD_BASE + "/responses", json={"model": "secondary"})
    monkeypatch.setenv("HTTPS_PROXY", "http://llm-lab:3128")
    with pytest.raises(g.MaintenanceIsolationError):
        client.post(g.CLOUD_BASE + "/responses", json={"model": MODEL})
    client.close()


def test_async_transport_hook(isolated):
    activate(isolated[1])
    from agent.process_bootstrap import build_keepalive_http_client
    async def run():
        client = build_keepalive_http_client("http://spark-3:8000/v1", async_mode=True)
        try:
            with pytest.raises(g.MaintenanceIsolationError):
                await client.post("http://spark-3:8000/v1/chat/completions", json={"model": "secondary"})
        finally:
            await client.aclose()
    asyncio.run(run())


@pytest.mark.parametrize("task", ["compression", "title_generation", "web_extract", "background_review", "session_search", "vision", "review"])
def test_critical_auxiliary_pins_discard_local_credentials(isolated, task):
    activate(isolated[1])
    from agent.auxiliary_client import _resolve_task_provider_model
    assert _resolve_task_provider_model(task, "custom:hef", "secondary", "http://llm-lab:8002/v1", "local-key") == (
        "openai-codex", MODEL, g.CLOUD_BASE, None, "codex_responses")


@pytest.mark.parametrize("name", ["_relay_sync_completion", "_relay_sync_stream", "_relay_async_completion"])
def test_aux_fallback_physical_calls_refuse_local(isolated, name):
    activate(isolated[1])
    from agent import auxiliary_client as a
    callback = Mock()
    client = SimpleNamespace(base_url="http://spark-3:8000/v1")
    with pytest.raises(g.MaintenanceIsolationError):
        result = getattr(a, name)(client, {"model": "secondary"}, provider="custom:hef", api_mode="chat_completions")
        if name == "_relay_async_completion":
            asyncio.run(result)
    callback.assert_not_called()


def test_delegation_overrides_secondary_before_auth(isolated, monkeypatch):
    activate(isolated[1])
    from tools import delegate_tool_config as d
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: isolated[1])
    resolve = Mock(return_value={"ok": True})
    monkeypatch.setattr(d, "_runtime_provider_credentials", resolve)
    assert d._resolve_delegation_credentials({"provider": "custom:hef", "model": "secondary", "base_url": "http://llm-lab:8002/v1"}, None) == {"ok": True}
    route = resolve.call_args.args[0]
    assert route["provider"] == "openai-codex" and route["model"] == MODEL
    assert route["api_key"] is None and route["base_url"] == g.CLOUD_BASE


@pytest.mark.parametrize("phase", ["active", "verifying"])
def test_dispatch_workers_reviewers_custom_spawns_refused(isolated, phase):
    activate(isolated[1], phase)
    from hermes_cli.kanban_db_dispatch import _call_spawn_fn
    spawn = Mock()
    with pytest.raises(g.MaintenanceIsolationError):
        _call_spawn_fn(spawn, SimpleNamespace(assignee="ops"), "/unused", None)
    spawn.assert_not_called()


def test_entry_verifies_every_profile_and_failure_stays_closed(isolated, monkeypatch):
    # This test isolates all-profile probing; serving tests exercise real accounting.
    monkeypatch.setattr('agent.serving_admission.observe', lambda model: {})
    homes = [isolated[1], isolated[1] / "second"]
    monkeypatch.setattr(ctl, "profiles", lambda: homes)
    probe = Mock(side_effect=[None, RuntimeError("auth failure")])
    monkeypatch.setattr(ctl, "probe_profile", probe)
    with pytest.raises(RuntimeError):
        ctl.enter(MODEL, drained=True)
    assert g.state()["phase"] == "verifying"
    assert g.state()["verified_profiles"] == [str(homes[0])]
    with pytest.raises(g.MaintenanceIsolationError):
        g.dispatch_route(homes[0])
    assert probe.call_count == 2
    monkeypatch.setattr(ctl, "probe_profile", Mock())
    assert ctl.enter(MODEL, drained=True)["phase"] == "active"
    assert g.state()["verified_profiles"] == [str(p) for p in homes]


def test_exit_requires_approval_and_preserves_preferences(isolated):
    activate(isolated[1])
    with pytest.raises(g.MaintenanceIsolationError):
        ctl.exit_maintenance("")
    assert g.state()["phase"] == "active"
    ctl.exit_maintenance("fixture explicit post-QA approval")
    assert g.state() is None
    assert g.auxiliary_route("custom:hef", "primary", "http://llm-lab:8002/v1", None, None)[1] == "primary"


def test_entry_requires_drain(isolated):
    with pytest.raises(g.MaintenanceIsolationError):
        ctl.enter(MODEL, drained=False)
    assert not isolated[0].exists()


def test_media_and_memory_block_before_plugins(isolated):
    activate(isolated[1])
    from tools.image_generation_tool import _handle_image_generate
    from tools.video_generation_tool import _handle_video_generate
    for fn in (_handle_image_generate, _handle_video_generate):
        with pytest.raises(g.MaintenanceIsolationError):
            fn({"prompt": "test"})
    from plugins.memory import load_memory_provider
    assert load_memory_provider("mem0") is None


def test_hooks_do_not_run_unverified_inference(isolated):
    activate(isolated[1])
    from hermes_cli.plugins_dispatch import PluginDispatchMixin
    hook = Mock()
    bus = PluginDispatchMixin()
    bus._hooks = {"pre_llm_call": [hook], "pre_tool_call": [hook]}
    assert bus.invoke_hook("pre_llm_call") == []
    assert bus.invoke_hook("pre_tool_call")[0]["action"] == "block"
    hook.assert_not_called()


def test_main_fallback_cannot_mutate_to_local(isolated):
    activate(isolated[1])
    from agent.chat_completion_helpers import try_activate_fallback
    agent = SimpleNamespace(provider="openai-codex", model=MODEL, _fallback_index=0,
        _fallback_chain=[{"provider": "custom:hef", "model": "secondary"}])
    assert try_activate_fallback(agent) is False
    assert agent.provider == "openai-codex" and agent._fallback_index == 0


def test_agent_constructor_blocks_before_side_effects(isolated):
    activate(isolated[1])
    from agent.agent_init import init_agent
    with pytest.raises(g.MaintenanceIsolationError):
        init_agent(SimpleNamespace(), provider="custom:hef", model="primary", base_url="http://llm-lab:8002/v1")


@pytest.mark.parametrize("job", [
    {"id": "fixture", "provider": "custom:hef", "model": "secondary"},
    {"id": "fixture", "no_agent": True, "script": "/never-run"},
    {"id": "fixture", "provider": "openai-codex", "model": MODEL, "fallback_providers": [{"provider": "custom:hef", "model": "primary"}]},
])
def test_cron_fails_before_script_or_fallback(isolated, monkeypatch, job):
    activate(isolated[1])
    from cron import scheduler
    prepare = Mock(side_effect=AssertionError("must not prepare scripts"))
    monkeypatch.setattr(scheduler, "_prepare_job_prompt", prepare)
    result = scheduler.run_job(job)
    assert result[0] is False and "Maintenance" in result[3]
    prepare.assert_not_called()


@pytest.mark.parametrize("base", [None, "http://llm-lab:8002/v1", "https://unknown-facade.invalid/v1"])
def test_resolved_auxiliary_endpoint_guard(isolated, monkeypatch, base):
    activate(isolated[1])
    from agent import auxiliary_client as a
    client = SimpleNamespace(base_url=base)
    monkeypatch.setitem(a._EXPLICIT_PROVIDER_BRANCHES, "openai-codex", lambda req: (client, MODEL))
    with pytest.raises(g.MaintenanceIsolationError):
        a.resolve_provider_client("custom:hef", "primary")


def test_no_credential_never_falls_back(isolated, monkeypatch):
    activate(isolated[1])
    from agent import auxiliary_client as a
    monkeypatch.setitem(a._EXPLICIT_PROVIDER_BRANCHES, "openai-codex", lambda req: (None, None))
    with pytest.raises(g.MaintenanceIsolationError, match="authentication"):
        a.resolve_provider_client("custom:hef", "secondary")


def test_safe_resolved_auxiliary_route(isolated, monkeypatch):
    activate(isolated[1])
    from agent import auxiliary_client as a
    client = SimpleNamespace(base_url=g.CLOUD_BASE)
    monkeypatch.setitem(a._EXPLICIT_PROVIDER_BRANCHES, "openai-codex", lambda req: (client, req.model))
    assert a.resolve_provider_client("custom:hef", "secondary") == (client, MODEL)


def test_audio_and_local_fallback_fail_closed(isolated):
    activate(isolated[1])
    from tools.tts_tool import text_to_speech_tool
    from tools.transcription_tools import transcribe_audio, transcribe_audio_local_fallback
    for fn in (text_to_speech_tool, transcribe_audio, transcribe_audio_local_fallback):
        with pytest.raises(g.MaintenanceIsolationError):
            fn("fixture")


def test_suffix_cannot_opt_out(monkeypatch):
    root = g.directory()
    monkeypatch.setenv("HERMES_DATA_DIR_SUFFIX", "-different")
    assert g.directory() == root


def test_symlink_state_fails_closed(isolated):
    root, home = isolated
    activate(home)
    original = root / "saved.json"
    (root / "state.json").rename(original)
    (root / "state.json").symlink_to(original)
    with pytest.raises(g.MaintenanceIsolationError):
        g.state()
