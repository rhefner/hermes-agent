"""Shared-facade backend identity, scoped draining and transport-shutdown locks.

Regression ground for the shared inference facade: one origin serves several
physical backends, so backend identity is the inventory's explicit model
ownership, never the model string or a broad origin grouping.
"""
import asyncio
import fcntl
import json
import os
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import httpx
import pytest

from agent import backend_isolation as b
from agent import backend_transport as bt
from agent import maintenance_admission as accounting
from agent import maintenance_inference as g
from agent import process_bootstrap
from hermes_cli import backend_isolation as ctl
from hermes_cli import maintenance_inference as legacy

# Exact facade models: spark-pair serves the primary lane, spark-3 the secondary.
PRIMARY_MODELS = ('primary', 'glm-5.3-flash', 'first2')
SECONDARY_MODELS = ('secondary', 'Qwen3.8-Flash-Next', 'last2')


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(g, 'directory', lambda: tmp_path / 'maintenance-inference')
    monkeypatch.setattr(accounting, 'directory', lambda: tmp_path / 'maintenance-admission')
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(key, raising=False)
    legacy.write_state({'version': 1, 'phase': 'normal'})
    return tmp_path


@contextmanager
def serve():
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length') or 0))
            body = json.dumps({'id': 'fixture', 'choices': [{'index': 0, 'message': {
                'role': 'assistant', 'content': 'fixture-response'}, 'finish_reason': 'stop'}]}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/v1'
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


def activate(affected):
    value = b.configuration()
    value.update(phase='active', affected=list(affected), operation='fixture-operation',
                 executor=accounting.identity(), verified_at=1)
    legacy.write_state(value)


@pytest.mark.platforms('linux')
def test_shared_facade_identity_and_scoped_drain(isolated):
    with serve() as facade, serve() as alias, serve() as independent:
        inventory = {'spark-pair': [facade, alias], 'spark-3': [facade],
                     'independent': [independent],
                     'facade_models': {model: 'spark-pair' for model in PRIMARY_MODELS}
                                      | {model: 'spark-3' for model in SECONDARY_MODELS}}
        expected = dict(inventory['facade_models'])
        value = ctl.migrate(inventory, approval='fixture shared facade')
        assert value['facade_models'] == expected
        assert 'facade_models' not in value['backends']
        b.validate(value)  # the stored shape validates idempotently

        resolved = b.configuration()
        # A dedicated alias resolves with no model at all; the shared facade needs one.
        assert b.backend(resolved, alias) == 'spark-pair'
        assert b.backend(resolved, facade) is b.UNRESOLVED
        assert b.backend(resolved, facade, 'secondary') == 'spark-3'
        assert b.backend(resolved, facade, 'primary') == 'spark-pair'

        client = process_bootstrap.build_keepalive_http_client(facade)
        try:
            # Normal enrollment rejects nothing: unmapped models, missing models
            # and malformed bodies on the shared facade all keep working.
            assert client.post(facade + '/chat/completions', json={'model': 'primary'}).status_code == 200
            assert client.post(facade + '/chat/completions', json={'model': 'unmapped-lane'}).status_code == 200
            assert client.post(facade + '/chat/completions').status_code == 200
            request = client.build_request('POST', facade + '/chat/completions', content=b'not-json{')
            assert client.send(request).status_code == 200
            g.require_route('custom', 'unmapped', 'http://uninventoried:9/v1', resolved=True)

            # Both lanes start before isolation: the primary response is then
            # in flight across the flip, which is exactly what the drain waits for.
            blocked = client.send(client.build_request(
                'POST', facade + '/chat/completions', json={'model': 'primary'}), stream=True)
            scoped = client.send(client.build_request(
                'POST', facade + '/chat/completions', json={'model': 'secondary'}), stream=True)
            activate(['spark-pair'])
            # Same URL: every primary-lane identity is excluded, secondary passes.
            for model in PRIMARY_MODELS:
                with pytest.raises(g.MaintenanceIsolationError, match='excludes backend'):
                    client.post(facade + '/chat/completions', json={'model': model})
            for model in SECONDARY_MODELS:
                assert client.post(facade + '/chat/completions', json={'model': model}).status_code == 200
            with pytest.raises(g.MaintenanceIsolationError, match='excludes backend'):
                g.require_route('custom', 'primary', alias, 'chat_completions', resolved=True)
            # Ownership is exact: case, staleness and omission fail closed.
            for model in ('Primary', 'stale-alias'):
                with pytest.raises(g.MaintenanceIsolationError, match='attribute'):
                    client.post(facade + '/chat/completions', json={'model': model})
            with pytest.raises(g.MaintenanceIsolationError, match='attribute'):
                client.post(facade + '/chat/completions')
            request = client.build_request('POST', facade + '/chat/completions', content=b'not-json{')
            with pytest.raises(g.MaintenanceIsolationError, match='attribute'):
                client.send(request)
            with pytest.raises(g.MaintenanceIsolationError, match='uninventoried'):
                g.require_route('custom', 'unmapped', 'http://uninventoried:9/v1', resolved=True)

            # Draining is scoped: an in-flight secondary response never blocks the
            # affected backend, an in-flight primary one does until it closes.
            try:
                with pytest.raises(g.MaintenanceIsolationError, match='in flight'):
                    b.require_mutation(['spark-pair'])
                blocked.close()
                assert b.require_mutation(['spark-pair'])['backends'] == ['spark-pair']
            finally:
                scoped.close()
                blocked.close()
            with process_bootstrap.build_keepalive_http_client(independent) as other:
                assert other.post(independent + '/chat/completions', json={'model': 'qwen'}).status_code == 200
        finally:
            client.close()

        async def secondary_async():
            async with process_bootstrap.build_keepalive_http_client(facade, async_mode=True) as async_client:
                response = await async_client.send(async_client.build_request(
                    'POST', facade + '/chat/completions', json={'model': 'secondary'}), stream=True)
                activate(['spark-pair'])
                try:
                    assert (await async_client.post(
                        facade + '/chat/completions', json={'model': 'secondary'})).status_code == 200
                    with pytest.raises(g.MaintenanceIsolationError, match='attribute'):
                        await async_client.post(facade + '/chat/completions', json={'model': 'Primary'})
                finally:
                    await response.aclose()
                assert b.require_mutation(['spark-pair'])['backends'] == ['spark-pair']
        asyncio.run(secondary_async())
        ctl.release('fixture facade release')
        assert b.configuration()['phase'] == 'normal'


@pytest.mark.platforms('linux')
def test_normal_phase_keeps_unrelated_and_proxied_inference(isolated, monkeypatch):
    with serve() as endpoint:
        ctl.migrate({'only': [endpoint]}, approval='fixture single')
        # Uninventoried routes and proxies are refused only under isolation.
        g.require_route('custom', 'any', 'http://uninventoried:9/v1', resolved=True)
        monkeypatch.setenv('HTTPS_PROXY', 'http://127.0.0.1:9')
        g.require_route('custom', 'any', endpoint, resolved=True)
        monkeypatch.setattr(process_bootstrap, '_get_proxy_for_base_url', lambda url: 'http://127.0.0.1:9')
        client = process_bootstrap.build_keepalive_http_client(endpoint)
        assert client is not None
        client.close()
        activate(['only'])
        with pytest.raises(g.MaintenanceIsolationError, match='proxy'):
            g.require_route('custom', 'any', endpoint, resolved=True)
        with pytest.raises(g.MaintenanceIsolationError):
            process_bootstrap.build_keepalive_http_client(endpoint)
        monkeypatch.delenv('HTTPS_PROXY')
        with pytest.raises(g.MaintenanceIsolationError, match='uninventoried'):
            g.require_route('custom', 'any', 'http://uninventoried:9/v1', resolved=True)


def _pinned_lock(tmp_path, monkeypatch, name):
    path = tmp_path / name
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    monkeypatch.setattr(bt, 'begin_request', lambda request: [fd])
    return path, fd


def _exclusive_ok(path):
    probe = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(probe)
    return True


def _body_response():
    return httpx.Response(200, stream=httpx.ByteStream(b'body'))


@pytest.mark.platforms('linux')
def test_transport_shutdown_releases_outstanding_locks(tmp_path, monkeypatch):
    request = httpx.Request('POST', 'http://fixture/v1/chat/completions', json={'model': 'primary'})
    # Sync: the outstanding body pins the lock until closed, never after.
    path, _ = _pinned_lock(tmp_path, monkeypatch, 'plain-close.lock')
    transport = bt.BackendTransport(httpx.MockTransport(lambda request: _body_response()))
    response = transport.handle_request(request)
    assert not _exclusive_ok(path)
    response.close()
    assert _exclusive_ok(path)

    # Shutdown sweeps an abandoned, still-pinning response; later close is idempotent.
    path, _ = _pinned_lock(tmp_path, monkeypatch, 'shutdown-sweep.lock')
    transport = bt.BackendTransport(httpx.MockTransport(lambda request: _body_response()))
    abandoned = transport.handle_request(request)
    assert not _exclusive_ok(path)
    transport.close()
    assert _exclusive_ok(path)
    abandoned.close()

    # Failure while taking the response closes the taken lock.
    path, _ = _pinned_lock(tmp_path, monkeypatch, 'failure.lock')
    def broken(request):
        raise RuntimeError('fixture transport failure')
    with pytest.raises(RuntimeError, match='fixture'):
        bt.BackendTransport(httpx.MockTransport(broken)).handle_request(request)
    assert _exclusive_ok(path)

    # Async: the abandoned, never-read response pins the lock until aclose.
    async def abandoned_async():
        path, _ = _pinned_lock(tmp_path, monkeypatch, 'async-abandoned.lock')
        async_transport = bt.AsyncBackendTransport(httpx.MockTransport(lambda request: _body_response()))
        response = await async_transport.handle_async_request(httpx.Request(
            'POST', 'http://fixture/v1/chat/completions', json={'model': 'primary'}))
        assert not _exclusive_ok(path)
        await async_transport.aclose()
        assert _exclusive_ok(path)
        await response.aclose()  # idempotent after the sweep
    asyncio.run(abandoned_async())
