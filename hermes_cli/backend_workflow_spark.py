"""Source-pinned Spark implementation of the maintenance workflow adapter."""
from __future__ import annotations

import calendar
from decimal import Decimal, InvalidOperation
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import time

import httpx

from agent import backend_isolation
from agent.backend_transport import BackendTransport
from agent import maintenance_inference as guard
from hermes_cli import backend_workflow_store as store


_MUTATIONS = frozenset({"config", "stop", "archive", "create", "start"})
_SPARK_RELATIVE = Path("users/hef/local-ai-services/spark-lanes")
_SOURCE_FILES = ("maintenance_admission.py", "renewal_ssh.py", "renewal-only-command.py",
                 "glm53_eight_stream.py", "glm53_eight_stream_qa.py")
_NODE_SOURCE_FILES = ('glm53_eight_stream.py', 'maintenance_admission.py', 'renewal_ssh.py')
_FACADE_URL = "http://llm-lab.hef.run:8002/v1/chat/completions"
_PRIMARY_MODEL = "glm-5.3-flash"
_SECONDARY_MODEL = "secondary"
_MAX_BODY = 2 * 1024 * 1024
_REQUEST_DEADLINE_SECONDS = 180
_SAFE_KEYS = frozenset({
    "action", "archived", "automatic_rollback", "context_length", "created", "done",
    "backup", "backup_sha256", "errors", "exit_code", "fingerprint", "finish_reason",
    "id", "live_acceptance", "local_hash", "logs", "ok", "oom", "original_mode", "rank",
    "raw_log_retained_in_container", "running", "started", "status", "stopped", "streams",
    "tail_bytes", "tail_sha256",
})
_SAFE_HASH_KEYS = frozenset({
    "backup_sha256", "fingerprint", "id", "local_hash",
    "raw_log_retained_in_container", "tail_sha256",
})
_SAFE_ENUMS = {
    "action": frozenset({
        "archive", "config", "create", "health", "inspect", "memory", "start", "status",
        "stop", "TEXT", "SSE", "TOOLS", "THINKING", "VISION", "SECONDARY",
    }),
    "finish_reason": frozenset({"stop", "tool_calls"}),
    "status": frozenset({
        "ALREADY_APPLIED", "BACKEND_ADMISSION_OK", "BOUNDED_QA_PASS", "READY_FOR_QA",
    }),
}
_HEX_SHA256 = re.compile(r"[a-f0-9]{64}")
_BACKUP_PATH = re.compile(
    r"/home/hef/src/glm53-tf-mia/scripts/eight-stream-before-[a-f0-9]{32}")


def _require(condition, message):
    if not condition:
        raise guard.MaintenanceIsolationError(message)


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    _require(spec is not None and spec.loader is not None, "Pinned Spark source is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _sanitize(value, key=None):
    """Retain validated recovery evidence; hash all other allowlisted strings."""
    if isinstance(value, dict):
        result = {}
        for item_key, item in value.items():
            item_key = str(item_key)
            if item_key not in _SAFE_KEYS:
                continue
            result[item_key] = _sanitize(item, item_key)
        return result
    if isinstance(value, (list, tuple)):
        return [_sanitize(item, key) for item in value]
    if key == "original_mode":
        return value if type(value) is int and 0 <= value <= 0o7777 else type(value).__name__
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        if key in _SAFE_HASH_KEYS and _HEX_SHA256.fullmatch(value):
            return value
        if key == "backup" and _BACKUP_PATH.fullmatch(value):
            return value
        if value in _SAFE_ENUMS.get(key, ()):
            return value
        return {"sha256": hashlib.sha256(value.encode()).hexdigest(), "bytes": len(value.encode())}
    return type(value).__name__


class _GuardedLive:
    def __init__(self, base, operation, check, record, *, approved=None, reserve=None,
                 source_check=None, starts=None):
        self._base = base
        self._operation = operation
        self._check = check
        self._record = record
        self._approved = approved
        self._reserve = reserve
        self._mutated = False
        self._source_check = source_check
        self._starts = starts

    def admission(self):
        operation = self._base.admission()
        _require(operation == self._operation, "Maintenance admission operation changed")
        return operation

    def rpc(self, rank, action, **kwargs):
        if (action in _MUTATIONS or action == 'qa') and self._source_check is not None:
            self._source_check()
        if action not in _MUTATIONS:
            if action == 'qa':
                admitted = self._check(['spark-pair'])
                _require(admitted.get('operation') == self._operation, 'QA admission operation changed')
                self._record(f'rank-{rank}-qa-request', 'evidence', {'rank': rank, 'action': 'QA'})
            result = self._base.rpc(rank, action, **kwargs)
            if self._approved is not None and not self._mutated and action == "inspect":
                expected = self._approved[rank]
                keys = ("id", "fingerprint", "local_hash") if rank == 0 else ("id", "fingerprint")
                _require(all(result.get(key) == expected[key] for key in keys),
                         "Spark pre-mutation snapshot changed after approval")
            self._record(f"rank-{rank}-{action}", "evidence", _sanitize(result))
            return result
        if action in {"config", "create"} and self._reserve is not None:
            try:
                exact_reserve = Decimal(str(kwargs.get("reserve"))) == self._reserve
            except (InvalidOperation, TypeError):
                exact_reserve = False
            _require(exact_reserve, "Spark mutation reserve differs from approved reserve")
        if self._approved is not None and not self._mutated:
            expected = self._approved[rank]
            _require(kwargs.get("id") == expected["id"]
                     and kwargs.get("fingerprint") == expected["fingerprint"]
                     and (rank != 0 or action != "config"
                          or kwargs.get("local_hash") == expected["local_hash"]),
                     "Spark first mutation does not match approved snapshot")
        if self._starts is not None:
            self._starts.before(rank, action, kwargs)
        admitted = self._check(["spark-pair"])
        _require(admitted.get("operation") == self._operation,
                 "Guard operation does not match maintenance operation")
        step = f"rank-{rank}-{action}"
        self._record(step, "intent", {"rank": rank, "action": action})
        result = self._base.rpc(rank, action, **kwargs)
        self._mutated = True
        if self._starts is not None:
            try:
                self._starts.after(rank, action, kwargs, result)
            except guard.MaintenanceIsolationError:
                # Preserve recovery IDs/backup hashes without marking completion
                # or clearing the pending intent of an unverified transition.
                self._record(step + '-unverified-response', 'evidence', _sanitize(result))
                raise
        self._record(step, "done", _sanitize(result))
        return result


class _ContainerStarts:
    """Track original/retained and approved candidates across every mutation."""
    NEVER_STARTED = '0001-01-01T00:00:00Z'

    def __init__(self, before, starts, reader):
        self.reader = reader
        self.current = [row['id'] for row in before]
        self.expected = {(rank, row['id']): starts[rank] for rank, row in enumerate(before)}
        self.created = set()

    def check(self):
        for (rank, identifier), expected in self.expected.items():
            _require(self.reader(rank, identifier) == expected,
                     'Container restart identity changed during approved apply')

    def before(self, rank, action, arguments):
        self.check()
        key = (rank, arguments.get('id'))
        _require(key in self.expected, 'Mutation container identity is not recorded')
        if action == 'create':
            _require(not any(r == rank for r, _ in self.created), 'Replacement identity already created')
        if action == 'start':
            _require(key in self.created and self.expected[key] == self.NEVER_STARTED,
                     'Start requires an approved never-started replacement identity')

    def after(self, rank, action, arguments, result):
        if action == 'create':
            identifier = result.get('id') if isinstance(result, dict) else None
            _require(isinstance(identifier, str) and _HEX_SHA256.fullmatch(identifier)
                     and (rank, identifier) not in self.expected, 'Invalid replacement container identity')
            key = (rank, identifier)
            self.expected[key] = self.NEVER_STARTED
            self.created.add(key)
            self.current[rank] = identifier
        elif action == 'start':
            key = (rank, arguments['id'])
            _require(isinstance(result, dict) and result.get('started') is True,
                     'Missing approved start identity result')
            started = self.reader(*key)
            _require(isinstance(started, str) and started and started != self.NEVER_STARTED,
                     'Approved replacement has no start identity')
            self.expected[key] = started
        self.check()

    def observe(self, identity):
        _require(identity['containers'] == self.current
                 and identity['starts'] == [self.expected[rank, identifier]
                                           for rank, identifier in enumerate(self.current)],
                 'Post-apply restart identity differs from approved mutations')
        # Archived originals are retained, not silently forgotten after create.
        self.check()


class SparkAdapter:
    """Concrete adapter for the one reviewed Spark pair/eight-stream operation."""

    def __init__(self, plan, *, repository=None, controller=None, qa_runner=None, live=None,
                 revision_reader=None, credential_reader=None, facade_transport=None,
                 qa_inner_transport=None, normal_inner_transport=None, metadata_inner_transport=None,
                 guard_check=None, now=None,
                 monotonic=None, start_identity_reader=None):
        store.validate_plan(plan)
        self.plan = json.loads(json.dumps(plan))
        self.repository = Path(repository) if repository is not None else Path.home() / "nix"
        self._now = now or time.time
        self._monotonic = monotonic or time.monotonic
        self._guard_check = guard_check or backend_isolation.require_mutation
        self._facade_transport = facade_transport or self._http_transport
        self._qa_inner_transport = qa_inner_transport
        self._normal_inner_transport = normal_inner_transport
        self._metadata_inner_transport = metadata_inner_transport
        self._revision_reader = revision_reader or self._source_revision
        self._credential_reader = credential_reader or self._credential_metadata
        self._preflight_identity = None
        self._start_identity_reader = start_identity_reader or self._container_start_time
        if controller is None:
            controller, qa_runner = self._load_reviewed_sources()
        self.controller = controller
        self.qa_runner = qa_runner
        self.live = live or controller.Live()
        self._validate_contract()

    def _verify_source(self):
        _require(self._revision_reader() == self.plan["controller_revision"],
                 "Spark controller source revision or cleanliness changed")

    def _node_source_hashes(self):
        """Bind node dependencies to immutable approved Git blobs, not loose bytes."""
        expected = {}
        try:
            for name in _NODE_SOURCE_FILES:
                blob = self.plan['controller_revision'] + ':' + str(_SPARK_RELATIVE / name)
                raw = subprocess.run(['git', '-C', str(self.repository), 'cat-file', 'blob', blob],
                                     capture_output=True, check=True, timeout=10).stdout
                _require(len(raw) <= 1024 * 1024, 'Pinned node source exceeds bound')
                expected[name] = hashlib.sha256(raw).hexdigest()
        except (OSError, subprocess.SubprocessError):
            raise guard.MaintenanceIsolationError('Pinned node dependency source unavailable') from None
        return expected

    def _remote_source_hashes(self, rank):
        _require(type(rank) is int and rank in (0, 1), 'Invalid node attestation target')
        paths = ['/home/hef/nix/' + str(_SPARK_RELATIVE / name) for name in _NODE_SOURCE_FILES]
        command = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                   '-o', 'ConnectTimeout=10', f'spark-{rank + 1}.hef.run', 'sha256sum', *paths]
        try:
            raw = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                 text=True, timeout=25, check=True).stdout
        except (OSError, subprocess.SubprocessError):
            raise guard.MaintenanceIsolationError('Node admission source attestation unavailable') from None
        _require(len(raw) <= 4096, 'Node source attestation exceeds bound')
        result = {}
        for line in raw.splitlines():
            sha, separator, path = line.partition('  ')
            _require(separator and _HEX_SHA256.fullmatch(sha) and path in paths
                     and path not in result, 'Node source attestation malformed')
            result[path] = sha
        _require(set(result) == set(paths), 'Node source attestation incomplete')
        return {name: result[path] for name, path in zip(_NODE_SOURCE_FILES, paths)}

    def _verify_execution_sources(self):
        self._verify_source()
        expected = self._node_source_hashes()
        observed = {}
        for rank in (0, 1):
            actual = self._remote_source_hashes(rank)
            _require(actual == expected, 'Node controller/admission source differs from approved revision')
            observed[str(rank)] = actual
        return observed

    def _source_revision(self):
        paths = [str(_SPARK_RELATIVE / name) for name in _SOURCE_FILES]
        try:
            revision = subprocess.run(
                ["git", "-C", str(self.repository), "rev-parse", "HEAD"], check=True,
                text=True, capture_output=True, timeout=10).stdout.strip()
            dirty = subprocess.run(
                ["git", "-C", str(self.repository), "status", "--porcelain", "--", *paths],
                check=True, text=True, capture_output=True, timeout=10).stdout
        except (OSError, subprocess.SubprocessError):
            raise guard.MaintenanceIsolationError("Pinned Spark source cannot be verified") from None
        _require(not dirty, "Pinned Spark controller source has local drift")
        return revision

    def _load_reviewed_sources(self):
        _require(self._source_revision() == self.plan["controller_revision"],
                 "Spark controller revision does not match plan")
        root = self.repository / _SPARK_RELATIVE
        # Pin adjacent imports before either reviewed runner executes.
        _load(root / "renewal_ssh.py", "renewal_ssh")
        _load(root / "maintenance_admission.py", "maintenance_admission")
        controller = _load(root / "glm53_eight_stream.py", "glm53_eight_stream")
        qa_runner = _load(root / "glm53_eight_stream_qa.py", "glm53_eight_stream_qa")
        return controller, qa_runner

    def _validate_contract(self):
        self._verify_source()
        expected_delta = dict(self.controller.DELTA)
        memory = self.plan["delta"].get("MEMORY_RESERVE_GIB")
        _require(memory is not None, "Complete Spark plan delta is required")
        expected_delta["MEMORY_RESERVE_GIB"] = memory
        expected_preserve = dict(self.controller.PINS)
        expected_preserve.update(RECIPE_REVISION=self.controller.RECIPE,
                                 IMAGE_DIGEST=self.controller.DIGEST)
        _require(self.plan["delta"] == expected_delta,
                 "Plan delta does not match reviewed Spark controller")
        _require(self.plan["preserve"] == expected_preserve,
                 "Preserved pins do not match reviewed Spark controller")
        _require(callable(getattr(self.controller, "preflight", None))
                 and callable(getattr(self.controller, "apply", None)),
                 "Reviewed Spark controller interface is unavailable")

    def _credential_metadata(self):
        """Read authorized_keys restrictions; do not retain/emit key material or read private keys."""
        try:
            lines = (Path.home() / ".ssh/authorized_keys").read_text().splitlines()
        except OSError:
            raise guard.MaintenanceIsolationError("Spark credential metadata is unavailable") from None
        entries = [line for line in lines if re.search(r" hermes-renewal-only-\d{8}-spark-[123]$", line)]
        required = {"restrict", "no-agent-forwarding", "no-port-forwarding",
                    "no-X11-forwarding", "no-pty", "no-user-rc"}
        expected_sources = {"192.168.0.39", "192.168.0.33", "192.168.0.213"}
        expected_command = ("/usr/bin/python3 -I /home/hef/nix/users/hef/local-ai-services/"
                            "spark-lanes/renewal-only-command.py")
        expiries, sources = set(), set()
        for line in entries:
            metadata, separator, _key = line.partition(" ssh-ed25519 ")
            options = metadata.split(",")
            # Options are comma-delimited; none of the three quoted values may contain commas.
            expiry = [m for option in options
                      for m in re.findall(r'^expiry-time="(\d{14}Z)"$', option)]
            source = [m for option in options for m in re.findall(r'^from="([0-9.]+)"$', option)]
            command = [m for option in options for m in re.findall(r'^command="([^"]+)"$', option)]
            flags = [option for option in options if option in required]
            _require(separator and len(options) == 9 and len(flags) == 6
                     and set(flags) == required and len(expiry) == len(source) == len(command) == 1
                     and command[0] == expected_command,
                     "Spark credential restrictions are missing or ambiguous")
            expiries.add(expiry[0])
            sources.add(source[0])
        _require(len(entries) == 3 and len(expiries) == 1 and sources == expected_sources,
                 "Exactly three source-restricted Spark credentials are required")
        expires_at = calendar.timegm(time.strptime(expiries.pop(), "%Y%m%d%H%M%SZ"))
        return {"expires_at": expires_at, "restrictions": 3}

    @staticmethod
    def _container_start_time(rank, identifier):
        # Docker's config fingerprint and ID survive an in-place restart. The
        # existing node report lacks StartedAt, so read this one fixed metadata
        # field with a bounded read-only command; no generic SSH surface exists.
        _require(type(rank) is int and rank in (0, 1) and isinstance(identifier, str)
                 and _HEX_SHA256.fullmatch(identifier), 'Invalid container start-identity target')
        command = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                   '-o', 'ConnectTimeout=10', f'spark-{rank + 1}.hef.run', 'docker', 'inspect',
                   '--format', '{{.State.StartedAt}}', identifier]
        try:
            value = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                   text=True, timeout=25, check=True).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            raise guard.MaintenanceIsolationError('Container restart identity unavailable') from None
        _require(value == _ContainerStarts.NEVER_STARTED
                 or re.fullmatch(r'20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z', value),
                 'Invalid container restart identity')
        return value

    def _observe(self):
        node_sources = self._verify_execution_sources()
        credentials = self._credential_reader()
        _require(credentials.get("restrictions") == 3
                 and isinstance(credentials.get("expires_at"), (int, float)),
                 "Spark credential metadata is unavailable")
        report = self.controller.preflight(self.live)
        before = report.get("before")
        _require(isinstance(before, list) and len(before) == 2
                 and all(isinstance(row.get("id"), str)
                         and isinstance(row.get("fingerprint"), str) and row["fingerprint"]
                         for row in before)
                 and isinstance(before[0].get("local_hash"), str) and before[0]["local_hash"],
                 "Spark inspection did not return complete observed identities")
        try:
            reserve_matches = (Decimal(str(report.get("reserve"))) ==
                               Decimal(self.plan["delta"]["MEMORY_RESERVE_GIB"]))
        except (InvalidOperation, TypeError):
            reserve_matches = False
        _require(reserve_matches, "Observed Spark proposal reserve differs from approved reserve")
        identity = {
            "source": self.plan["controller_revision"],
            "node_sources": node_sources,
            "config": {"fingerprints": [row["fingerprint"] for row in before],
                       "local_hash": before[0]["local_hash"]},
            "containers": [row["id"] for row in before],
            "starts": [self._start_identity_reader(rank, row['id']) for rank, row in enumerate(before)],
        }
        _require(all(start != _ContainerStarts.NEVER_STARTED for start in identity['starts']),
                 'Healthy observation contains an unstarted container identity')
        health = report.get("health", {})
        healthy = (health.get("ok") is True and health.get("context_length") == 1048576
                   and health.get("streams", {}).get("max") in (4, 8))
        return report, credentials, identity, healthy, reserve_matches

    def inspect(self):
        report, credentials, identity, healthy, reserve_matches = self._observe()
        return self._inspection(report, credentials, identity, healthy, reserve_matches)

    @staticmethod
    def _inspection(report, credentials, identity, healthy, reserve_matches):
        return {"identity": identity, "credentials_expires_at": credentials["expires_at"],
                "already_applied": report.get("already_applied") is True and reserve_matches,
                "healthy": healthy}

    def _preflight_observation(self):
        self._verify_source()
        credentials = self._credential_reader()
        _require(credentials.get("restrictions") == 3
                 and isinstance(credentials.get("expires_at"), (int, float)),
                 "Spark credential metadata is unavailable")
        required_until = self._now() + self.plan["budget_seconds"] + self.plan["margin_seconds"]
        _require(credentials["expires_at"] > required_until,
                 "Spark credential lifetime does not cover maintenance and QA")
        _require(self.qa_runner is not None and callable(getattr(self.qa_runner, "run", None))
                 and callable(self._facade_transport), "Required Spark QA capability is unavailable")
        self._facade_capability()
        return self._observe()

    def preflight(self):
        observed = self._preflight_observation()
        snapshot = self._inspection(*observed)
        self._preflight_identity = json.loads(json.dumps(snapshot['identity']))
        return snapshot

    def _admit(self, operation):
        admitted = self._guard_check(["spark-pair"])
        _require(admitted.get("operation") == operation,
                 "Guard operation does not match maintenance operation")
        _require(self.live.admission() == operation, "Controller admission operation changed")

    def apply(self, guard_operation, record):
        observed = self._preflight_observation()
        if self._preflight_identity is not None:
            _require(self._inspection(*observed)['identity'] == self._preflight_identity,
                     'Spark identity changed since workflow preflight')
        report = observed[0]
        self._verify_source()
        self._admit(guard_operation)
        before = report["before"]
        approved = {rank: {key: row[key] for key in
                    (("id", "fingerprint", "local_hash") if rank == 0 else ("id", "fingerprint"))}
                    for rank, row in enumerate(before)}
        starts = _ContainerStarts(before, observed[2]['starts'], self._start_identity_reader)

        guarded = _GuardedLive(
            self.live, guard_operation, self._guard_check, record,
            approved=approved, reserve=Decimal(str(report["reserve"])),
            source_check=self._verify_execution_sources, starts=starts)
        mutation_steps = tuple(f"{rank}-{action}" for rank in (0, 1) for action in _MUTATIONS)

        def evidence(step, data):
            if not any(step.startswith(prefix) or step.endswith(action)
                       for prefix in mutation_steps for action in _MUTATIONS):
                record(step, "evidence", _sanitize(data))

        result = self.controller.apply(guarded, evidence)
        _require(result.get("status") in ("READY_FOR_QA", "ALREADY_APPLIED"),
                 "Spark controller did not reach a QA-ready state")
        snapshot = self.inspect()
        _require(snapshot['already_applied'], 'Reviewed controller did not reach approved goal')
        starts.observe(snapshot['identity'])
        return snapshot

    def qa(self, guard_operation, record):
        self._verify_source()
        self._admit(guard_operation)
        guarded = _GuardedLive(self.live, guard_operation, self._guard_check, record,
                               source_check=self._verify_execution_sources)
        direct = self.qa_runner.run(
            guarded, lambda step, data: record(step, "evidence", _sanitize(data)))
        _require(direct.get("status") == "BOUNDED_QA_PASS", "Direct Spark QA failed")
        _require(direct.get("peak_requests_running") == 8,
                 "Eight active decoding requests were not observed")
        facade_results = {}
        for check in ("text", "sse", "tools", "thinking", "vision", "secondary"):
            self._verify_source()
            self._admit(guard_operation)
            record("facade-" + check + "-request", "evidence", {"action": check.upper()})
            result = (self._normal_request("secondary") if check == "secondary" else
                      self._facade_transport(check, operation=guard_operation))
            _require(isinstance(result, dict) and result.get("ok") is True,
                     "Required facade QA failed: " + check)
            facade_results[check] = _sanitize(result)
            record("facade-" + check, "evidence", facade_results[check])
        snapshot = self.inspect()
        _require(snapshot["healthy"] and snapshot["already_applied"],
                 "Final health and preserved settings were not proven")
        checks = {name: True for name in self.plan["qa"]}
        return {"checks": checks, "identity": snapshot["identity"],
                "evidence": {"peak_decoding": 8, "facade": facade_results,
                             "direct_status": direct["status"]}}

    def normal_routes(self):
        result = {}
        for route in ("primary", "secondary"):
            evidence = self._normal_request(route)
            _require(isinstance(evidence, dict) and evidence.get("ok") is True,
                     "Normal " + route + " route did not complete successfully")
            result[route] = True
        return result

    def _http_transport(self, check, operation=None):
        _require(check in {"text", "sse", "tools", "thinking", "vision"},
                 "Unsupported fixed facade QA check")
        model = _PRIMARY_MODEL
        prompt = "Reply with READY."
        if check == "thinking":
            prompt = "What is 17 times 19? Explain briefly then give the answer."
        elif check == "tools":
            prompt = "Use the echo tool with value READY."
        content = prompt
        if check == "vision":
            content = [{"type": "text", "text": "Name the solid color in the image."},
                       {"type": "image_url", "image_url": {"url": self.qa_runner.red_png()}}]
        body = {"model": model, "messages": [{"role": "user", "content": content}],
                "max_tokens": 2048 if check == "thinking" else 512 if check == "tools" else 256,
                "temperature": 0, "stream": check == "sse",
                "chat_template_kwargs": {"enable_thinking": check == "thinking"}}
        if check == "tools":
            body["tools"] = [{"type": "function", "function": {"name": "echo",
                "description": "Echo", "parameters": {"type": "object",
                "properties": {"value": {"type": "string"}}, "required": ["value"]}}}]
            body["tool_choice"] = {"type": "function", "function": {"name": "echo"}}
        self._verify_source()
        self._admit(operation)
        # This explicit target has no bypass header or mutable routing argument.
        status, raw = self._post(self._qa_inner_transport, body, guarded=False)
        if check == "sse":
            try:
                parsed = self.qa_runner.sse({"status": status, "body": raw.decode()})
            except (RuntimeError, ValueError, TypeError, UnicodeError):
                raise guard.MaintenanceIsolationError("Fixed facade SSE QA failed") from None
            _require(parsed.get("done") is True and parsed.get("finish_reason") == "stop"
                     and "READY" in parsed.get("text", ""), "Fixed facade SSE QA failed")
            return {"ok": True}
        try:
            value = json.loads(raw)
        except (ValueError, TypeError, UnicodeError):
            raise guard.MaintenanceIsolationError("Fixed facade QA response was invalid") from None
        choices = value.get("choices", [])
        usage = value.get("usage", {})
        _require(status == 200 and bool(choices)
                 and type(usage.get("completion_tokens")) is int
                 and usage["completion_tokens"] > 0, "Fixed facade QA was incomplete")
        choice = choices[0]
        message = choice.get("message", {})
        if check == "tools":
            calls = message.get("tool_calls", [])
            try:
                actual = json.loads(calls[0]["function"]["arguments"])
            except (IndexError, KeyError, TypeError, ValueError):
                return {"ok": False}
            _require(len(calls) == 1 and choice.get("finish_reason") == "tool_calls"
                     and calls[0]["function"].get("name") == "echo"
                     and actual == {"value": "READY"}, "Fixed facade tool QA failed")
            return {"ok": True}
        answer = message.get("content")
        if not isinstance(answer, str) or not answer.strip() or choice.get("finish_reason") != "stop":
            raise guard.MaintenanceIsolationError("Fixed facade QA completion was empty")
        if check == "thinking":
            _require("323" in answer and bool(message.get("reasoning_content") or message.get("reasoning")),
                     "Fixed facade thinking QA failed")
            return {"ok": True}
        if check == "vision":
            _require("red" in answer.lower(), "Fixed facade vision QA failed")
            return {"ok": True}
        _require("READY" in answer, "Fixed facade text QA sentinel missing")
        return {"ok": True}

    def _facade_capability(self):
        """Fixed metadata GET only: establish QA routes before traffic exclusion."""
        deadline = self._monotonic() + 20
        inner = self._metadata_inner_transport or httpx.HTTPTransport(retries=0)
        try:
            with httpx.Client(transport=inner, follow_redirects=False, trust_env=False,
                              timeout=httpx.Timeout(20, connect=5)) as client:
                with client.stream('GET', 'http://llm-lab.hef.run:8002/health') as response:
                    value = json.loads(self._read_bounded(response, deadline, limit=256 * 1024))
                    _require(response.status_code == 200 and value.get('status') == 'ok'
                             and isinstance(value.get('models'), list)
                             and {'primary', _PRIMARY_MODEL, _SECONDARY_MODEL} <= set(value['models']),
                             'Required facade QA routes are unavailable before maintenance')
        except guard.MaintenanceIsolationError:
            raise
        except (httpx.HTTPError, OSError, ValueError, TypeError, AttributeError):
            raise guard.MaintenanceIsolationError('Required facade QA metadata is unavailable') from None

    def _read_bounded(self, response, deadline, limit=_MAX_BODY):
        chunks, total = [], 0
        for chunk in response.iter_bytes():
            _require(self._monotonic() <= deadline, "Facade response deadline exceeded")
            total += len(chunk)
            _require(total <= limit, "Facade response exceeded bound")
            chunks.append(chunk)
        _require(self._monotonic() <= deadline, "Facade response deadline exceeded")
        return b"".join(chunks)

    def _post(self, inner, body, *, guarded):
        deadline = self._monotonic() + _REQUEST_DEADLINE_SECONDS
        inner = inner or httpx.HTTPTransport(retries=0)
        transport = BackendTransport(inner) if guarded else inner
        try:
            with httpx.Client(transport=transport, follow_redirects=False,
                              trust_env=False, timeout=httpx.Timeout(180, connect=10)) as client:
                _require(self._monotonic() <= deadline, "Facade response deadline exceeded")
                with client.stream("POST", _FACADE_URL, json=body) as response:
                    _require(self._monotonic() <= deadline, "Facade response deadline exceeded")
                    # A blocked header/chunk read may consume one socket timeout;
                    # therefore total wall time is deadline plus at most that timeout.
                    return response.status_code, self._read_bounded(response, deadline)
        except guard.MaintenanceIsolationError:
            raise
        except (httpx.HTTPError, OSError, ValueError, TypeError):
            raise guard.MaintenanceIsolationError("Fixed facade request failed") from None

    def _normal_request(self, route):
        _require(route in {"primary", "secondary"}, "Unsupported normal facade route")
        body = {"model": route, "messages": [{"role": "user", "content": "Reply with READY."}],
                "max_tokens": 256, "temperature": 0, "stream": False,
                "chat_template_kwargs": {"enable_thinking": False}}
        status, raw = self._post(self._normal_inner_transport, body, guarded=True)
        try:
            value = json.loads(raw)
            choice = value["choices"][0]
            usage = value["usage"]
            answer = choice["message"]["content"]
        except (ValueError, TypeError, KeyError, IndexError, UnicodeError):
            raise guard.MaintenanceIsolationError("Normal facade completion was invalid") from None
        _require(status == 200 and choice.get("finish_reason") == "stop"
                 and isinstance(answer, str) and "READY" in answer
                 and type(usage.get("completion_tokens")) is int
                 and usage["completion_tokens"] > 0,
                 "Normal facade completion did not prove the route")
        return {"ok": True}
