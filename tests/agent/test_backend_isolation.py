"""Backend isolation invariants using real HTTP, file locks and durable state."""
import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import subprocess
import sys
import threading
from types import SimpleNamespace

import httpx
import pytest

from agent import backend_isolation as b
from agent import maintenance_admission as accounting
from agent import maintenance_inference as g
from hermes_cli import backend_isolation as ctl
from hermes_cli import maintenance_inference as legacy


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(g, 'directory', lambda: tmp_path / 'maintenance-inference')
    monkeypatch.setattr(accounting, 'directory', lambda: tmp_path / 'maintenance-admission')
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(key, raising=False)
    legacy.write_state({'version': 1, 'phase': 'normal'})
    return tmp_path


@contextmanager
def serve_endpoint():
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
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


@pytest.fixture
def endpoints():
    with serve_endpoint() as affected, serve_endpoint() as independent:
        yield affected, independent


@pytest.mark.platforms('linux')
def test_backend_boundary_and_real_stream_drain(isolated, endpoints, monkeypatch):
    from agent.process_bootstrap import build_keepalive_http_client
    endpoint, independent = endpoints
    from agent import chat_completion_helpers as main
    from agent import auxiliary_client as aux
    from hermes_cli.runtime_provider import resolve_runtime_provider

    inventory = {'pair': [endpoint, 'http://pair-alias:8000'],
                 'independent': [independent]}
    ctl.migrate(inventory, approval='fixture deployment')
    client = build_keepalive_http_client(endpoint)
    assert client is not None
    response = client.send(client.build_request('POST', endpoint + '/chat/completions',
                                                json={'model': 'primary'}), stream=True)
    try:
        value = b.configuration()
        value.update(phase='active', affected=['pair'], operation='fixture-operation',
                     executor=accounting.identity(), verified_at=1)
        legacy.write_state(value)
        with pytest.raises(g.MaintenanceIsolationError, match='in flight'):
            b.require_mutation(['pair'])
        # Unrelated runtime work is not dependent on the executor or legacy ledger.
        with accounting.work('unrelated-chat'):
            for home in ('A', 'B', 'A'):
                monkeypatch.setenv('HERMES_HOME', str(isolated / home))
                g.require_route('custom', 'qwen', independent, resolved=True)
                with build_keepalive_http_client(independent) as other:
                    assert other.post(independent + '/chat/completions', json={'model': 'qwen'}).status_code == 200
                with pytest.raises(g.MaintenanceIsolationError):
                    resolve_runtime_provider(requested='custom', explicit_api_key='fixture',
                                             explicit_base_url=endpoint, target_model='alias')
        for url in (endpoint, 'http://PAIR-ALIAS.:8000/v1'):
            for model in ('primary', 'secondary', 'arbitrary-alias'):
                with pytest.raises(g.MaintenanceIsolationError, match='excludes backend'):
                    resolve_runtime_provider(requested='custom', explicit_api_key='fixture',
                                             explicit_base_url=url, target_model=model)
        stale = SimpleNamespace(provider='custom', model='primary', base_url=endpoint,
                                api_mode='chat_completions')
        assert main.try_activate_fallback(stale) is False
        from tools.delegate_tool_config import _resolve_child_runtime
        for generation in range(2):
            with pytest.raises(g.MaintenanceIsolationError):
                _resolve_child_runtime(stale, {}, 'fixture', model=f'child-{generation}',
                    override_provider='custom', override_base_url=endpoint, override_api_key='fixture',
                    override_api_mode='chat_completions', override_acp_command=None, override_acp_args=None)
        for task in ('compression', 'background_review', 'title_generation'):
            with pytest.raises(g.MaintenanceIsolationError):
                aux.call_llm(task, provider='custom', model='alias', base_url=endpoint, api_key='fixture',
                             messages=[{'role': 'user', 'content': 'fixture'}])
        for fn in (main.direct_api_call, main.interruptible_api_call, main.interruptible_streaming_api_call):
            with pytest.raises(g.MaintenanceIsolationError):
                fn(stale, {'model': 'other-alias'})
        for fn in (aux._relay_sync_completion, aux._relay_sync_stream):
            with pytest.raises(g.MaintenanceIsolationError):
                fn(stale, {'model': 'primary'}, provider='custom', api_mode='chat_completions')
        with pytest.raises(g.MaintenanceIsolationError):
            asyncio.run(aux._relay_async_completion(stale, {'model': 'primary'}, provider='custom', api_mode='chat_completions'))
        with pytest.raises(g.MaintenanceIsolationError):
            client.post(endpoint + '/chat/completions', json={'model': 'renamed'})
    finally:
        response.close()
        client.close()
    assert b.require_mutation(['pair'])['backends'] == ['pair']
    with pytest.raises(g.MaintenanceIsolationError):
        b.require_mutation(['independent'])
    # Actual stock policy callbacks still run and retain their veto.
    from hermes_cli.plugins import PluginManager
    bus = PluginManager()
    bus._hooks = {'pre_tool_call': [lambda **kw: {'action': 'block', 'message': 'stock policy'}]}
    assert bus.invoke_hook('pre_tool_call')[0]['message'] == 'stock policy'
    ctl.release('fixture explicit post-QA approval')
    with build_keepalive_http_client(endpoint) as client:
        assert client.post(endpoint + '/chat/completions', json={'model': 'primary'}).status_code == 200

    async def physical_async():
        async with build_keepalive_http_client(endpoint, async_mode=True) as client:
            response = await client.send(client.build_request('POST', endpoint + '/chat/completions',
                                          json={'model': 'primary'}), stream=True)
            legacy.write_state(value)
            try:
                with pytest.raises(g.MaintenanceIsolationError, match='in flight'):
                    b.require_mutation(['pair'])
                with pytest.raises(g.MaintenanceIsolationError):
                    await client.post(endpoint + '/chat/completions', json={'model': 'alias'})
                assert (await client.post(independent + '/chat/completions', json={'model': 'qwen'})).status_code == 200
            finally:
                await response.aclose()
            assert b.require_mutation(['pair'])['backends'] == ['pair']
    asyncio.run(physical_async())
    ctl.release('fixture async QA release')
    with monkeypatch.context() as m:
        from agent import process_bootstrap
        m.setattr(process_bootstrap, 'build_keepalive_http_client', lambda *a, **kw: None)
        with pytest.raises(g.MaintenanceIsolationError, match='unguarded'):
            aux._openai_http_client_kwargs(endpoint)


@pytest.mark.platforms('linux')
@pytest.mark.parametrize('legacy_phase,closed', [('normal', False), ('active', True), ('normal', True), ('verifying', True)])
def test_migration_crash_resume_and_provider_failure(isolated, monkeypatch, legacy_phase, closed):
    accounting.initialize(accounting.directory())
    reservation = accounting.reserve('historic-orphan')
    old = {'version': 1, 'phase': legacy_phase, 'provider': 'openai-codex', 'model': 'external',
           'base_url': g.CLOUD_BASE, 'verified_profiles': []}
    legacy.write_state(old)
    if closed:
        with accounting.transaction() as conn:
            conn.execute('UPDATE control SET body=? WHERE id=1', (json.dumps({
                'version': 1, 'closed': True, 'executor': {'pid': 2147483647}, 'expires': 0}),))
    before = (accounting.directory() / 'work.sqlite').read_bytes()
    value = ctl.migrate({'pair': ['http://pair:8000'], 'independent': ['http://other:8000']}, approval='fixture')
    assert (accounting.directory() / 'work.sqlite').read_bytes() == before
    assert value['phase'] == ('interrupted' if closed else 'normal')
    assert accounting.reserve('unrelated') is None
    assert g.dispatch_route('/new/profile') is None
    assert list(g.directory().glob('legacy-*.json'))
    if closed:
        affected = ['pair', 'independent']
    else:
        affected = ['pair']
    def unavailable(model):
        raise RuntimeError('fixture quota unavailable')
    monkeypatch.setattr(legacy, 'probe', unavailable)
    with pytest.raises(RuntimeError, match='quota'):
        b.enter(affected, 'external', executor_pid=os.getpid(), resume=closed)
    assert b.configuration()['phase'] == 'interrupted'
    with pytest.raises(g.MaintenanceIsolationError):
        g.require_route('custom', 'alias', 'http://pair:8000', resolved=True)
    with accounting.work('unrelated-job'):
        pass
    monkeypatch.setattr(legacy, 'probe', lambda model: None)
    value = b.enter(affected, 'external', executor_pid=os.getpid(), resume=True)
    assert b.require_mutation(['pair'])['operation'] == value['operation']
    value['executor'] = {'pid': 2147483647, 'start': '0', 'boot': 'old'}
    legacy.write_state(value)
    assert b.observed()['phase'] == 'interrupted'
    with pytest.raises(g.MaintenanceIsolationError, match='resume'):
        b.require_mutation(['pair'])
    # A fresh interpreter still sees persisted exclusions, with no lease age check.
    script = ('from pathlib import Path; from agent import maintenance_inference as g; '
              f'g.directory=lambda:Path({str(g.directory())!r}); '
              'from agent.backend_isolation import observed; print(observed()["phase"])')
    result = subprocess.run([sys.executable, '-c', script], text=True, capture_output=True, check=True)
    assert result.stdout.strip() == 'interrupted'
    assert (accounting.directory() / 'work.sqlite').read_bytes() == before
    with pytest.raises(g.MaintenanceIsolationError):
        ctl.release('')
    ctl.release('fixture approved release')
    assert g.state() is None
    assert b.configuration()['backends'] == value['backends']
