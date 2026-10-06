"""Linux exact-PID runtime attestation; not a sandbox against same-PID code.

Kernel SO_PEERCRED binds the responder to the enrolled executor. A fresh nonce
prevents replay. Evidence lives in that runtime's memory and is created only by
an authenticated cloud completion executed there, never from a CLI string/file.
No provider call is made until explicit certify/renew against active isolation.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import struct
import threading
import time
import weakref

from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard

TIMEOUT = 35
REFRESH_AFTER = 60
_server = None


def address(identity):
    # Abstract sockets avoid stale files and long profile paths. Linux-only:
    # lack of SO_PEERCRED is a closed failure, never a portable weaker fallback.
    return '\0hermes-admission-' + str(os.getuid()) + '-' + str(identity['pid']) + '-' + str(identity['start'])


def peer(sock):
    return struct.unpack('3i', sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))


def receive(sock):
    data = bytearray()
    while not data.endswith(b'\n'):
        chunk = sock.recv(8192)
        if not chunk or len(data) + len(chunk) > 65536:
            raise ValueError('invalid attestation frame')
        data.extend(chunk)
    return json.loads(data)


def request(proof, state_hash, action, previous=None):
    from agent.serving_admission import refuse
    expected = proof['executor']
    nonce = secrets.token_hex(32)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(TIMEOUT)
            conn.connect(address(expected))
            pid, uid, _ = peer(conn)
            if pid != expected['pid'] or uid != os.getuid():
                refuse('attestation peer is not the exact executor PID')
            if any(accounting.identity(pid)[k] != expected[k] for k in ('pid', 'start', 'boot')):
                refuse('attestation executor identity changed')
            conn.sendall(json.dumps({'nonce': nonce, 'action': action,
                                    'proof': proof, 'state_sha256': state_hash,
                                    'previous': previous}).encode() + b'\n')
            result = receive(conn)
        if result.get('nonce') != nonce or result.get('ok') is not True:
            refuse('runtime did not attest authenticated completion')
        evidence = result['evidence']
        from agent.serving_admission import MAX_AGE, digest
        if (evidence['executor'] != {k: expected[k] for k in ('pid', 'start', 'boot')}
                or evidence['proof_sha256'] != digest(proof)
                or evidence['state_sha256'] != state_hash
                or not 0 <= time.time() - evidence['completed_at'] <= MAX_AGE
                or evidence['provider'] != 'openai-codex'
                or not isinstance(evidence['completion_nonce'], str)
                or len(evidence['completion_nonce']) != 64):
            refuse('stale or mismatched runtime completion evidence')
        return evidence
    except (OSError, ValueError, KeyError, TypeError, AttributeError, accounting.AdmissionClosed):
        refuse('exact executor attestation unavailable or malformed')


class RuntimeAttestor:
    def __init__(self, agent):
        self.agent = weakref.ref(agent)
        self.identity = accounting.identity()
        self.evidence = None
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(address(self.identity))
        self.sock.listen(4)
        self.thread = threading.Thread(target=self.serve, name='executor-attestation', daemon=True)
        self.thread.start()

    def respond(self, message):
        from agent.serving_admission import active_state, observe, digest, refuse, MAX_AGE
        from hermes_cli.maintenance_inference import probe
        from hermes_constants import get_hermes_home
        agent = self.agent()
        value = active_state()
        proof = observe(value['model'])
        if (agent is None or os.getpid() != self.identity['pid']
                or proof['executor']['pid'] != os.getpid()
                or agent.provider != 'openai-codex' or agent.model != value['model']
                or agent.api_mode != 'codex_responses' or not guard.cloud_url(agent.base_url)
                or str(get_hermes_home().resolve()) != proof['profile_home']
                or message['proof'] != proof or message['state_sha256'] != digest(value)):
            refuse('runtime executor/route/state mismatch')
        action = message['action']
        if action not in ('certify', 'renew', 'check'):
            refuse('unknown attestation action')
        if self.evidence and (self.evidence['proof_sha256'] != digest(proof)
                              or self.evidence['state_sha256'] != digest(value)):
            self.evidence = None
        if action == 'renew' and (self.evidence is None or message.get('previous') != self.evidence):
            refuse('renewal has no matching prior runtime certification')
        if action == 'certify' or (action == 'renew' and
                (self.evidence is None or time.time() - self.evidence['completed_at'] >= REFRESH_AFTER)):
            self.evidence = None  # Failed completion cannot leave old authority.
            challenge = secrets.token_hex(32)
            started = time.time()
            probe(value['model'], challenge=challenge)  # No subprocess: exact executing PID.
            if (time.time() - started > TIMEOUT - 5 or observe(value['model']) != proof
                    or active_state() != value or agent.provider != 'openai-codex'
                    or agent.model != value['model'] or agent.api_mode != 'codex_responses'
                    or not guard.cloud_url(agent.base_url)
                    or str(get_hermes_home().resolve()) != proof['profile_home']):
                refuse('runtime changed or completion deadline exceeded')
            self.evidence = {'executor': self.identity, 'provider': 'openai-codex',
                             'proof_sha256': digest(proof), 'state_sha256': digest(value),
                             'completion_nonce': challenge, 'completed_at': time.time()}
        if not self.evidence or not 0 <= time.time() - self.evidence['completed_at'] <= MAX_AGE:
            refuse('no fresh authenticated completion in executor runtime')
        return self.evidence

    def serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(TIMEOUT)
                try:
                    if peer(conn)[1] != os.getuid():
                        continue
                    message = receive(conn)
                    nonce = message['nonce']
                    if not isinstance(nonce, str) or len(nonce) != 64:
                        raise ValueError('invalid challenge')
                    evidence = self.respond(message)
                    result = {'ok': True, 'nonce': nonce, 'evidence': evidence}
                except Exception:
                    self.evidence = None
                    result = {'ok': False}  # Never disclose SDK/auth exceptions.
                try:
                    conn.sendall(json.dumps(result).encode() + b'\n')
                except OSError:
                    self.evidence = None


def register(agent):
    """Called after real CLI agent initialization; non-CLI/background agents excluded."""
    global _server
    from agent.serving_admission import process, is_runtime, option
    # Non-Codex/non-Linux CLIs have no attestation capability; normal chat is
    # unchanged and serving admission still refuses the missing capability.
    if (os.name != 'posix' or not hasattr(socket, 'SO_PEERCRED')
            or getattr(agent, 'provider', None) != 'openai-codex'
            or getattr(agent, 'api_mode', None) != 'codex_responses'):
        return
    current = process(os.getpid())
    argv = current['argv']
    if (not is_runtime(argv) or 'gateway' in argv or 'hermes-gateway' in current['cgroup']
            or option(argv, '--provider') != 'openai-codex'
            or option(argv, '-m', '--model') != agent.model
            or agent.provider != 'openai-codex' or agent.api_mode != 'codex_responses'
            or not guard.cloud_url(agent.base_url)):
        return
    if _server is None:
        _server = RuntimeAttestor(agent)
    else:
        _server.agent = weakref.ref(agent)
        _server.evidence = None
