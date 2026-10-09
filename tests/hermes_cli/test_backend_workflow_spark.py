import json
import os
from pathlib import Path

import httpx
import pytest

from agent import backend_isolation
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from hermes_cli import maintenance_inference as legacy


REVISION = "a" * 40


def plan():
    from hermes_cli.backend_workflow_store import QA_CHECKS

    return {
        "adapter": "spark-pair-eight-stream",
        "backends": ["spark-pair"],
        "controller_revision": REVISION,
        "delta": {
            "PARALLEL": "8",
            "TF_GLM_MULTI_WINDOW": "64",
            "TF_ROCE_MAX_KB": "1024",
            "MEMORY_RESERVE_GIB": "19.6",
        },
        "preserve": {
            "MODEL_ID": "org/model",
            "MODEL_REVISION": "b" * 40,
            "DFLASH2_ID": "org/draft",
            "DFLASH2_REVISION": "c" * 40,
            "RECIPE_REVISION": "d" * 40,
            "IMAGE_DIGEST": "sha256:" + "e" * 64,
            "CONTEXT": "1048576",
            "THINKING": "1",
            "VISION": "1",
            "TP": "2",
            "KV": "fp8",
            "DENSE": "q4",
            "DRAFTER": "dflash2",
            "PORT": "8000",
            "SERVED_NAME": "glm-5.3-flash",
        },
        "qa": sorted(QA_CHECKS),
        "budget_seconds": 120,
        "margin_seconds": 60,
        "tasks": [],
    }


class Controller:
    DELTA = {"PARALLEL": "8", "TF_GLM_MULTI_WINDOW": "64", "TF_ROCE_MAX_KB": "1024"}
    PINS = {
        "MODEL_ID": "org/model", "MODEL_REVISION": "b" * 40,
        "DFLASH2_ID": "org/draft", "DFLASH2_REVISION": "c" * 40,
        "CONTEXT": "1048576", "THINKING": "1", "VISION": "1", "TP": "2",
        "KV": "fp8", "DENSE": "q4", "DRAFTER": "dflash2", "PORT": "8000",
        "SERVED_NAME": "glm-5.3-flash",
    }
    RECIPE = "d" * 40
    DIGEST = "sha256:" + "e" * 64

    def __init__(self):
        self.counter = 0

    def preflight(self, io):
        self.counter += 1
        return {
            "before": [{"id": "container-0", "fingerprint": "f0", "local_hash": "local-0"},
                       {"id": "container-1", "fingerprint": "f1"}],
            "health": {"ok": True, "requests_running": self.counter,
                       "streams": {"max": 8, "decoding": 0}, "context_length": 1048576},
            "reserve": "19.6", "already_applied": True,
        }

    def apply(self, io, save, wait_seconds=900):
        save("before", {"token": "do-not-record", "error": "private completion text",
                        "status": "ready"})
        io.rpc(0, "status")
        io.rpc(0, "config", id="container-0", fingerprint="f0",
               local_hash="local-0", reserve="19.6")
        io.rpc(1, "stop", id="container-1", fingerprint="f1")
        return {"status": "READY_FOR_QA"}


class Live:
    def __init__(self, operation="1" * 32):
        self.operation = operation
        self.calls = []

    def admission(self):
        return self.operation

    def rpc(self, rank, action, **kw):
        self.calls.append((rank, action, kw))
        return {"ok": True, "action": action, "token": "never-record"}


def adapter(**overrides):
    from hermes_cli.backend_workflow_spark import SparkAdapter

    values = {
        "controller": Controller(), "live": Live(),
        "qa_runner": QaRunner(),
        "revision_reader": lambda: REVISION,
        "credential_reader": lambda: {"expires_at": 10_000, "restrictions": 3},
        "now": lambda: 100,
        "start_identity_reader": lambda rank, identifier: f'2026-10-09T00:00:0{rank}.000000000Z',
        "guard_check": lambda backends: {
            "operation": "1" * 32, "backends": list(backends), "status": "BACKEND_ADMISSION_OK"
        },
        "metadata_inner_transport": httpx.MockTransport(lambda request: httpx.Response(
            200, json={'status': 'ok', 'models': ['primary', 'glm-5.3-flash', 'secondary']})),
        "normal_inner_transport": httpx.MockTransport(
            lambda request: httpx.Response(200, json=_completion())),
    }
    values.update(overrides)
    subject = SparkAdapter(plan(), **values)
    hashes = {name: 'a' * 64 for name in ('glm53_eight_stream.py', 'maintenance_admission.py', 'renewal_ssh.py')}
    subject._node_source_hashes = lambda: dict(hashes)
    subject._remote_source_hashes = lambda rank: dict(hashes)
    return subject


def test_node_attestation_hashes_pinned_blobs_and_uses_fixed_readonly_command(monkeypatch):
    import hashlib
    from types import SimpleNamespace
    from hermes_cli import backend_workflow_spark as module
    subject = adapter()
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0] == 'git':
            assert command[3:5] == ['cat-file', 'blob']
            assert command[5].startswith(REVISION + ':')
            return SimpleNamespace(stdout=b'approved source')
        assert command[8:10] == ['spark-1.hef.run', 'sha256sum']
        assert kwargs['timeout'] == 25 and kwargs['check'] is True
        return SimpleNamespace(stdout=''.join('a' * 64 + '  ' + path + '\n' for path in command[10:]))

    monkeypatch.setattr(module.subprocess, 'run', run)
    expected = module.SparkAdapter._node_source_hashes(subject)
    assert expected == {name: hashlib.sha256(b'approved source').hexdigest() for name in module._NODE_SOURCE_FILES}
    actual = module.SparkAdapter._remote_source_hashes(subject, 0)
    assert actual == {name: 'a' * 64 for name in module._NODE_SOURCE_FILES}
    assert len(calls) == 4


def test_direct_qa_checks_current_operation_before_network():
    from hermes_cli.backend_workflow_spark import _GuardedLive
    live = Live()
    guarded = _GuardedLive(live, '1' * 32, lambda targets: {'operation': '2' * 32}, lambda *args: None)
    with pytest.raises(guard.MaintenanceIsolationError, match='QA admission'):
        guarded.rpc(0, 'qa', operation='1' * 32, body={'model': 'glm-5.3-flash'})
    assert live.calls == []


def test_node_admission_source_drift_blocks_before_controller_observation():
    subject = adapter()
    subject._remote_source_hashes = lambda rank: {}
    with pytest.raises(guard.MaintenanceIsolationError, match='Node'):
        subject.preflight()
    assert subject.controller.counter == 0 and subject.live.calls == []


def test_restart_identity_uses_only_bounded_fixed_docker_inspection(monkeypatch):
    from types import SimpleNamespace
    from hermes_cli import backend_workflow_spark as module
    calls = []

    def inspect(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout='2026-10-09T00:00:00.123456789Z\n')

    subject = adapter()
    monkeypatch.setattr(module.subprocess, 'run', inspect)
    assert subject._container_start_time(1, 'c' * 64) == '2026-10-09T00:00:00.123456789Z'
    assert calls[0][0][8:] == ['spark-2.hef.run', 'docker', 'inspect', '--format', '{{.State.StartedAt}}', 'c' * 64]
    assert calls[0][1]['timeout'] == 25 and calls[0][1]['check'] is True
    with pytest.raises(guard.MaintenanceIsolationError):
        subject._container_start_time(1, 'container; forbidden')
    assert len(calls) == 1


def test_same_container_restart_changes_observed_identity():
    subject = adapter()
    starts = ['first-start-0', 'first-start-1']
    subject._start_identity_reader = lambda rank, identifier: starts[rank]
    before = subject.inspect()['identity']
    starts[0] = 'second-start-0'
    after = subject.inspect()['identity']
    assert before['containers'] == after['containers']
    assert before['config'] == after['config']
    assert before != after


def test_missing_facade_routes_fail_preflight_before_controller_observation():
    subject = adapter()
    seen = []

    def metadata(request):
        seen.append(request)
        return httpx.Response(200, json={'status': 'ok', 'models': ['secondary']})

    subject._metadata_inner_transport = httpx.MockTransport(metadata)
    with pytest.raises(guard.MaintenanceIsolationError, match='facade'):
        subject.preflight()
    assert len(seen) == 1 and seen[0].method == 'GET'
    assert subject.controller.counter == 0
    assert subject.live.calls == []


def test_apply_refuses_drift_since_engine_preflight():
    controller = Controller()
    subject = adapter(controller=controller)
    subject.preflight()
    original = controller.preflight

    def changed(io):
        result = original(io)
        result['before'][0]['local_hash'] = 'changed-before-apply'
        return result

    controller.preflight = changed

    def apply_current(io, save):
        row = controller.preflight(io)['before'][0]
        io.rpc(0, 'config', id=row['id'], fingerprint=row['fingerprint'],
               local_hash=row['local_hash'], reserve='19.6')
        return {'status': 'READY_FOR_QA'}

    controller.apply = apply_current
    with pytest.raises(guard.MaintenanceIsolationError, match='preflight'):
        subject.apply('1' * 32, lambda *args: None)
    assert subject.live.calls == []


def test_inspect_identity_is_stable_when_health_counters_change():
    subject = adapter()
    first = subject.inspect()
    second = subject.inspect()
    assert first["identity"] == second["identity"]
    assert json.dumps(first["identity"], sort_keys=True)
    assert first["credentials_expires_at"] == 10_000
    assert first["healthy"] is True


def test_inspect_rejects_controller_reserve_that_differs_from_approved_plan():
    controller = Controller()
    original = controller.preflight
    controller.preflight = lambda io: dict(original(io), reserve="20.0", already_applied=True)
    with pytest.raises(guard.MaintenanceIsolationError, match="reserve"):
        adapter(controller=controller).inspect()


def test_identity_changes_when_observed_fingerprint_or_local_config_changes():
    controller = Controller()
    original = controller.preflight
    observations = iter((
        {"before": [{"id": "container-0", "fingerprint": "f0", "local_hash": "local-a"},
                    {"id": "container-1", "fingerprint": "f1"}]},
        {"before": [{"id": "container-0", "fingerprint": "changed", "local_hash": "local-b"},
                    {"id": "container-1", "fingerprint": "f1"}]},
    ))

    def observed(io):
        result = original(io)
        result.update(next(observations))
        return result

    controller.preflight = observed
    subject = adapter(controller=controller)
    assert subject.inspect()["identity"] != subject.inspect()["identity"]


@pytest.mark.parametrize("field", ["delta", "preserve"])
def test_incomplete_controller_contract_is_refused(field):
    from hermes_cli.backend_workflow_spark import SparkAdapter

    invalid = plan()
    invalid[field].pop(next(iter(invalid[field])))
    with pytest.raises(guard.MaintenanceIsolationError):
        SparkAdapter(invalid, controller=Controller(), live=Live(),
                     revision_reader=lambda: REVISION,
                     credential_reader=lambda: {"expires_at": 10_000, "restrictions": 3})


def test_expired_credentials_block_preflight_without_remote_mutation():
    live = Live()
    subject = adapter(live=live, credential_reader=lambda: {"expires_at": 200, "restrictions": 3})
    with pytest.raises(guard.MaintenanceIsolationError, match="credential"):
        subject.preflight()
    assert live.calls == []


def test_read_only_paths_never_call_mutating_rpc():
    live = Live()
    subject = adapter(live=live)
    subject.inspect()
    subject.preflight()
    assert all(action not in {"config", "stop", "archive", "create", "start"}
               for _, action, _ in live.calls)


def deployment_adapter(restart_at=None):
    initial = ['1' * 64, '2' * 64]
    replacement = ['a' * 64, 'b' * 64]
    current = list(initial)
    starts = {(rank, identifier): f'2026-10-09T00:00:0{rank}Z' for rank, identifier in enumerate(initial)}
    zero = '0001-01-01T00:00:00Z'

    class DeploymentLive(Live):
        def rpc(self, rank, action, **kwargs):
            self.calls.append((rank, action, kwargs))
            if action == 'create':
                current[rank] = replacement[rank]
                starts[rank, current[rank]] = zero
                return {'id': current[rank], 'fingerprint': f'new-{rank}'}
            if action == 'start':
                assert starts[rank, kwargs['id']] == zero, 'unexpected physical duplicate start'
                starts[rank, kwargs['id']] = f'2026-10-09T00:01:0{rank}Z'
                return {'started': True}
            return {'ok': True}

    class DeploymentController(Controller):
        def preflight(self, io):
            result = super().preflight(io)
            for rank, row in enumerate(result['before']):
                row['id'] = current[rank]
                row['fingerprint'] = f'new-{rank}' if current[rank] == replacement[rank] else f'f{rank}'
            result['already_applied'] = current == replacement
            return result

        def apply(self, io, save, wait_seconds=900):
            io.rpc(0, 'config', id=initial[0], fingerprint='f0', local_hash='local-0', reserve='19.6')
            if restart_at == 'after-config':
                starts[1, initial[1]] = '2026-10-09T00:02:01Z'
            for rank in (0, 1):
                io.rpc(rank, 'stop', id=initial[rank], fingerprint=f'f{rank}')
            for rank in (1, 0):
                io.rpc(rank, 'archive', id=initial[rank], fingerprint=f'f{rank}')
                created = io.rpc(rank, 'create', id=initial[rank], fingerprint=f'f{rank}', reserve='19.6')
                if restart_at == 'before-candidate-start':
                    starts[rank, created['id']] = '2026-10-09T00:02:01Z'
                io.rpc(rank, 'start', id=created['id'], fingerprint=created['fingerprint'])
            if restart_at == 'after-ready':
                starts[0, current[0]] = '2026-10-09T00:03:00Z'
            if restart_at == 'retained-after-ready':
                starts[0, initial[0]] = '2026-10-09T00:03:00Z'
            return {'status': 'READY_FOR_QA'}

    subject = adapter(controller=DeploymentController(), live=DeploymentLive(),
                      start_identity_reader=lambda rank, identifier: starts[rank, identifier])
    return subject


@pytest.mark.parametrize('when', ['after-config', 'before-candidate-start', 'after-ready', 'retained-after-ready'])
def test_restart_during_apply_is_not_adopted_as_new_baseline(when):
    subject = deployment_adapter(when)
    with pytest.raises(guard.MaintenanceIsolationError, match='[Rr]estart|[Ii]dentity'):
        subject.apply('1' * 32, lambda *args: None)
    if when == 'after-config':
        assert [action for _, action, _ in subject.live.calls] == ['config']
    if when == 'before-candidate-start':
        assert all(action != 'start' for _, action, _ in subject.live.calls)


def test_all_nine_mutations_allow_only_approved_new_container_starts():
    subject = deployment_adapter()
    result = subject.apply('1' * 32, lambda *args: None)
    assert result['already_applied'] is True
    assert result['identity']['containers'] == ['a' * 64, 'b' * 64]
    assert result['identity']['starts'] == ['2026-10-09T00:01:00Z', '2026-10-09T00:01:01Z']
    assert [action for _, action, _ in subject.live.calls] == [
        'config', 'stop', 'stop', 'archive', 'create', 'start', 'archive', 'create', 'start']


def test_apply_checks_exact_guard_before_each_mutation_and_redacts_records():
    events = []
    checks = []

    def checked(backends):
        checks.append(tuple(backends))
        return {"operation": "1" * 32, "backends": list(backends)}

    subject = adapter(guard_check=checked)
    subject.apply("1" * 32, lambda step, phase, data: events.append((step, phase, data)))
    phases = [(step, phase) for step, phase, _ in events]
    assert phases.index(("rank-0-config", "intent")) < phases.index(("rank-0-config", "done"))
    assert phases.index(("rank-1-stop", "intent")) < phases.index(("rank-1-stop", "done"))
    # One target admission before execution, then one fresh check per mutation.
    assert checks == [("spark-pair",), ("spark-pair",), ("spark-pair",)]
    assert "do-not-record" not in json.dumps(events)
    assert "never-record" not in json.dumps(events)
    assert "private completion text" not in json.dumps(events)


def test_apply_refuses_changed_admission_before_base_rpc():
    live = Live()
    subject = adapter(live=live, guard_check=lambda _: {"operation": "2" * 32})
    with pytest.raises(guard.MaintenanceIsolationError, match="operation"):
        subject.apply("1" * 32, lambda *args: None)
    assert not any(action in {"config", "stop", "archive", "create", "start"}
                   for _, action, _ in live.calls)


@pytest.mark.parametrize("drift", ["identity", "reserve"])
def test_apply_refuses_preflight_to_first_mutation_drift(drift):
    class GapController(Controller):
        def apply(self, io, save, wait_seconds=900):
            before = [io.rpc(rank, "inspect") for rank in (0, 1)]
            io.rpc(0, "config", id=before[0]["id"],
                   fingerprint=before[0]["fingerprint"],
                   local_hash=before[0]["local_hash"],
                   reserve="20.0" if drift == "reserve" else "19.6")
            return {"status": "READY_FOR_QA"}

    class InspectLive(Live):
        def rpc(self, rank, action, **kw):
            if action == "inspect":
                row = {"id": f"container-{rank}", "fingerprint": f"f{rank}"}
                if rank == 0:
                    row["local_hash"] = "local-0"
                    if drift == "identity":
                        row["fingerprint"] = "changed"
                self.calls.append((rank, action, kw))
                return row
            return super().rpc(rank, action, **kw)

    live = InspectLive()
    subject = adapter(controller=GapController(), live=live)
    with pytest.raises(guard.MaintenanceIsolationError, match="reserve|snapshot"):
        subject.apply("1" * 32, lambda *args: None)
    assert not any(action in {"config", "stop", "archive", "create", "start"}
                   for _, action, _ in live.calls)


def test_sanitize_preserves_strict_recovery_evidence_and_hashes_unknown_status():
    from hermes_cli.backend_workflow_spark import _sanitize

    operation = "1" * 32
    evidence = _sanitize({
        "id": "a" * 64,
        "fingerprint": "b" * 64,
        "local_hash": "c" * 64,
        "backup": "/home/hef/src/glm53-tf-mia/scripts/eight-stream-before-" + operation,
        "backup_sha256": "d" * 64,
        "original_mode": 0o755,
        "logs": {"raw_log_retained_in_container": "e" * 64},
        "status": "SECRET_TOKEN",
        "env": {"TOKEN": "must-not-survive"},
        "argv": ["--secret", "must-not-survive"],
    })

    assert evidence["id"] == "a" * 64
    assert evidence["fingerprint"] == "b" * 64
    assert evidence["local_hash"] == "c" * 64
    assert evidence["backup"].endswith("eight-stream-before-" + operation)
    assert evidence["backup_sha256"] == "d" * 64
    assert evidence["original_mode"] == 0o755
    assert evidence["logs"]["raw_log_retained_in_container"] == "e" * 64
    assert evidence["status"] == {"sha256":
        "62b6fbe339718e7b8d54f3df2e33d59feb83aaa36504e3721e9c22c2fb1049ab", "bytes": 12}
    serialized = json.dumps(evidence)
    assert "SECRET_TOKEN" not in serialized
    assert "must-not-survive" not in serialized


class QaRunner:
    def __init__(self, peak=8):
        self.peak = peak

    def run(self, io, save, interval=1.0):
        save("direct", {"status": "pass"})
        return {"status": "BOUNDED_QA_PASS", "peak_requests_running": self.peak,
                "min_available_kib": [1, 1]}

    @staticmethod
    def red_png():
        return "data:image/png;base64,fixture"

    @staticmethod
    def sse(response):
        assert response["status"] == 200
        text = ""
        done = False
        finish = None
        for line in response["body"].splitlines():
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                done = True
                continue
            choice = json.loads(payload)["choices"][0]
            text += choice.get("delta", {}).get("content", "")
            finish = choice.get("finish_reason") or finish
        return {"done": done, "text": text, "finish_reason": finish}


def facade(check, operation=None):
    return {"ok": True, "check": check, "operation": operation,
            "completion": "READY", "finish_reason": "stop"}


def test_qa_rejects_queued_only_capacity_and_false_facade_result():
    with pytest.raises(guard.MaintenanceIsolationError, match="decoding"):
        adapter(qa_runner=QaRunner(peak=7), facade_transport=facade).qa("1" * 32, lambda *a: None)
    with pytest.raises(guard.MaintenanceIsolationError, match="facade"):
        adapter(qa_runner=QaRunner(), facade_transport=lambda *a, **k: {"ok": False}).qa(
            "1" * 32, lambda *a: None)


def test_qa_maps_only_actual_results_to_complete_checklist():
    result = adapter(qa_runner=QaRunner(), facade_transport=facade).qa(
        "1" * 32, lambda *a: None)
    assert result["checks"] == {name: True for name in plan()["qa"]}
    assert result["evidence"]["peak_decoding"] == 8


def test_qa_rechecks_admission_before_every_facade_request():
    calls = []
    checks = 0

    def admission(_backends):
        nonlocal checks
        checks += 1
        return {"operation": ("1" if checks < 3 else "2") * 32}

    with pytest.raises(guard.MaintenanceIsolationError, match="operation"):
        adapter(guard_check=admission,
                facade_transport=lambda check, operation=None: calls.append(check) or {"ok": True}).qa(
                    "1" * 32, lambda *args: None)
    assert calls == ["text"]


def test_qa_rechecks_clean_source_before_every_facade_request():
    calls = []
    revisions = iter((REVISION, REVISION, REVISION, "b" * 40))
    subject = adapter(revision_reader=lambda: next(revisions),
                      facade_transport=lambda check, operation=None:
                      calls.append(check) or {"ok": True})
    with pytest.raises(guard.MaintenanceIsolationError, match="source"):
        subject.qa("1" * 32, lambda *args: None)
    assert calls == ["text"]


@pytest.fixture
def isolated_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "directory", lambda: tmp_path / "guard")
    monkeypatch.setattr(accounting, "directory", lambda: tmp_path / "accounting")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(key, raising=False)
    state = {
        "version": 2, "phase": "active",
        "backends": {"spark-pair": ["http://llm-lab.hef.run:8002/v1"],
                     "spark-3": ["http://llm-lab.hef.run:8002/v1"]},
        "facade_models": {"primary": "spark-pair", "secondary": "spark-3"},
        "affected": ["spark-pair"], "provider": "openai-codex", "model": "gpt-5",
        "base_url": guard.CLOUD_BASE, "operation": "1" * 32, "executor": {}, "verified_at": 1,
    }
    legacy.write_state(state)
    return state


def _completion(text="READY", *, usage=True, reasoning=None):
    message = {"role": "assistant", "content": text}
    if reasoning is not None:
        message["reasoning_content"] = reasoning
    value = {"choices": [{"message": message, "finish_reason": "stop"}]}
    if usage:
        value["usage"] = {"completion_tokens": 1}
    return value


def test_normal_routes_use_real_guarded_transport(isolated_guard):
    traffic = []

    def handler(request):
        traffic.append(json.loads(request.content)["model"])
        return httpx.Response(200, json=_completion())

    subject = adapter(normal_inner_transport=httpx.MockTransport(handler))
    with pytest.raises(guard.MaintenanceIsolationError, match="excludes backend"):
        subject.normal_routes()
    assert traffic == []
    legacy.write_state(dict(isolated_guard, phase="normal", affected=[]))
    assert subject.normal_routes() == {"primary": True, "secondary": True}
    assert traffic == ["primary", "secondary"]


def test_http_transport_validates_all_fixed_protocols():
    seen = []

    def handler(request):
        body = json.loads(request.content)
        check = body["messages"][0]["content"]
        seen.append(body)
        if body.get("stream"):
            stream = ('data: {"choices":[{"delta":{"content":"READY"},"finish_reason":"stop"}]}\n\n'
                      'data: [DONE]\n\n').encode()
            return httpx.Response(200, content=stream)
        if body.get("tools"):
            return httpx.Response(200, json={"choices": [{"finish_reason": "tool_calls", "message": {
                "tool_calls": [{"function": {"name": "echo", "arguments": '{"value":"READY"}'}}]}}],
                "usage": {"completion_tokens": 1}})
        if body.get("chat_template_kwargs", {}).get("enable_thinking"):
            return httpx.Response(200, json=_completion("323", reasoning="17*19"))
        if isinstance(check, list):
            return httpx.Response(200, json=_completion("red"))
        return httpx.Response(200, json=_completion())

    subject = adapter(qa_inner_transport=httpx.MockTransport(handler))
    for check in ("text", "sse", "tools", "thinking", "vision"):
        assert subject._http_transport(check, operation="1" * 32)["ok"] is True
    assert all(body.get("chat_template_kwargs", {}).get("enable_thinking") is False
               for body in seen if body["messages"][0]["content"] !=
               "What is 17 times 19? Explain briefly then give the answer.")
    thinking = next(body for body in seen
                    if body.get("chat_template_kwargs", {}).get("enable_thinking") is True)
    assert thinking["max_tokens"] == 2048


@pytest.mark.parametrize("response", [
    httpx.Response(200, json=_completion("not-the-sentinel")),
    httpx.Response(200, json=_completion(usage=False)),
    httpx.Response(200, content=b"x" * (2 * 1024 * 1024 + 1)),
])
def test_http_transport_rejects_false_or_truncated_text(response):
    subject = adapter(qa_inner_transport=httpx.MockTransport(lambda request: response))
    with pytest.raises(guard.MaintenanceIsolationError):
        subject._http_transport("text", operation="1" * 32)


@pytest.mark.parametrize(("check", "response"), [
    ("sse", httpx.Response(200, content=(
        b'data: {"choices":[{"delta":{"content":"READY"},"finish_reason":"stop"}]}\n\n'))),
    ("tools", httpx.Response(200, json={"choices": [{"finish_reason": "tool_calls", "message": {
        "tool_calls": [{"function": {"name": "echo", "arguments": '{"value":"READY"}'}},
                       {"function": {"name": "echo", "arguments": '{"value":"READY"}'}}]}}],
        "usage": {"completion_tokens": 1}})),
    ("thinking", httpx.Response(200, json=_completion("323"))),
    ("vision", httpx.Response(200, json=_completion("blue"))),
])
def test_http_transport_rejects_protocol_specific_false_results(check, response):
    subject = adapter(qa_inner_transport=httpx.MockTransport(lambda request: response))
    with pytest.raises(guard.MaintenanceIsolationError):
        subject._http_transport(check, operation="1" * 32)


def test_http_transport_rechecks_exact_admission_at_entry():
    traffic = []
    checks = []

    def admission(backends):
        checks.append(tuple(backends))
        return {"operation": "1" * 32}

    subject = adapter(guard_check=admission, qa_inner_transport=httpx.MockTransport(
        lambda request: traffic.append(request) or httpx.Response(200, json=_completion())))
    with pytest.raises(guard.MaintenanceIsolationError, match="operation"):
        subject._http_transport("text", operation=None)
    assert traffic == []
    assert subject._http_transport("text", operation="1" * 32) == {"ok": True}
    assert checks == [("spark-pair",), ("spark-pair",)]


def test_whole_response_deadline_closes_guarded_stream_lock(isolated_guard):
    class Clock:
        value = 0

    class Dribble(httpx.SyncByteStream):
        closed = False

        def __iter__(self):
            Clock.value = 100
            yield b'{"choices":'
            Clock.value = 181
            yield b'[]}'

        def close(self):
            self.closed = True

    stream = Dribble()
    subject = adapter(monotonic=lambda: Clock.value,
                      normal_inner_transport=httpx.MockTransport(
                          lambda request: httpx.Response(200, stream=stream)))
    with pytest.raises(guard.MaintenanceIsolationError, match="deadline"):
        subject._normal_request("secondary")
    assert stream.closed is True
    fd = backend_isolation.lock_backend("spark-3", exclusive=True)
    os.close(fd)


def test_credential_metadata_parses_exact_installer_syntax_and_rejects_duplicate(tmp_path, monkeypatch):
    ssh = tmp_path / ".ssh"
    ssh.mkdir()
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    prefix = ("restrict,no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-pty,no-user-rc,"
              'expiry-time="20990101000000Z",from="{}",command="/usr/bin/python3 -I '
              '/home/hef/nix/users/hef/local-ai-services/spark-lanes/renewal-only-command.py" ')
    sources = (("spark-1", "192.168.0.39"), ("spark-2", "192.168.0.33"),
               ("spark-3", "192.168.0.213"))
    lines = [prefix.format(source) + "ssh-ed25519 fixture hermes-renewal-only-20990101-" + host
             for host, source in sources]
    path = ssh / "authorized_keys"
    path.write_text("\n".join(lines) + "\n")
    assert adapter()._credential_metadata()["restrictions"] == 3
    path.write_text("\n".join([lines[0].replace("restrict,", "restrict,restrict,"), *lines[1:]]) + "\n")
    with pytest.raises(guard.MaintenanceIsolationError, match="ambiguous"):
        adapter()._credential_metadata()


def test_normal_routes_require_valid_guarded_primary_and_secondary_completions():
    broken = adapter(normal_inner_transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=_completion("wrong"))))
    with pytest.raises(guard.MaintenanceIsolationError):
        broken.normal_routes()
