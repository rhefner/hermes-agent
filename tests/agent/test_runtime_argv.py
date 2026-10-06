"""Exact installed launchers and hostile lookalikes; never run argv code."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from agent.runtime_argv import ROOT, runtime_argv, runtime_kind
from agent import serving_admission as admission
from agent import executor_attestation as att
from hermes_cli._launchers import _launcher_script, runtime_command
from hermes_cli.venv_sync import relaunch_command

ARGS = ['--provider', 'openai-codex', '--model', 'gpt-6-astra', '--resume', 'exact-session']


def legacy(args):
    return relaunch_command(Path('/managed/bin/python3'), ROOT,
                            [str(ROOT / 'hermes_cli/main.py'), *args],
                            ['python', '-m', 'hermes_cli.main'], 'hermes_cli.main')


def published(args):
    return ['/managed/bin/python3', '-I', '-c', _launcher_script('hermes', ROOT, None), *args]


@pytest.mark.parametrize('make', [legacy, published, lambda args: runtime_command(ROOT, args, python='/python3')])
@pytest.mark.parametrize('args,kind', [(ARGS, 'cli'), (['gateway', 'run'], 'gateway'),
                                       (['dashboard', '--no-open'], 'dashboard'),
                                       (['relay', 'start'], 'relay')])
def test_shipped_launchers(make, args, kind):
    argv = make(args)
    assert runtime_argv(argv) == ['hermes', *args]
    assert runtime_kind(argv) == kind
    assert admission.is_runtime(argv)
    assert admission.option(argv, '--provider') == ('openai-codex' if kind == 'cli' else None)
    assert admission.option(argv, '-m', '--model') == ('gpt-6-astra' if kind == 'cli' else None)


@pytest.mark.parametrize('argv', [
    ['python3', '-m', 'hermes_cli.main', *ARGS],
    ['python3', '-I', '-B', '-W', 'ignore', '-X', 'utf8', '-m', 'hermes_cli.main', *ARGS],
    ['hermes', *ARGS],
    ['python3', str(ROOT / 'hermes_cli/main.py'), *ARGS],
])
def test_direct_roots(argv):
    assert runtime_kind(argv) == 'cli'
    assert admission.option(argv, '-m', '--model') == 'gpt-6-astra'


@pytest.mark.parametrize('argv', [
    ['python3', str(ROOT / 'tools/mcp_death_supervisor.py'), 'hermes_cli.main'],
    ['python3', '-m', 'tools.mcp_death_supervisor', str(ROOT / 'run_agent.py')],
    ['python3', '-c', "print('hermes_cli.main')"],
    ['python3-evil', '-m', 'hermes_cli.main'],
    ['python3', '/untrusted/run_agent.py'],
    ['python3', '-m', 'unrelated', 'hermes_cli.main'],
    ['sh', '-c', 'python3 -m hermes_cli.main'],
    ['hermes', '--run-module', 'tools.mcp_death_supervisor'],
    published(['--run-module', 'tools.mcp_death_supervisor']),
    published(['--print-runtime-command']),
    ['python3', '-m'], ['python3', '-I', '-c'], [],
])
def test_helpers_and_lookalikes_are_not_roots(argv):
    assert runtime_argv(argv) is None
    assert admission.option(argv, '--provider') is None


@pytest.mark.parametrize('mutation', [
    lambda s: s + '; __import__("os").system("false")',
    lambda s: 'if False:\n ' + s,
    lambda s: s.replace("alter_sys=True", "alter_sys=False"),
    lambda s: s.replace(str(ROOT), '/untrusted/hermes-agent'),
    lambda s: s.replace("sys.argv = [", "sys.argv = list([", 1),
    lambda s: s.replace("import sys, runpy", "import sys, runpy as other"),
    lambda s: s.replace("'openai-codex'", "__import__('os').getcwd()"),
    lambda s: 'pass; ' + s,
    lambda s: s.replace("runpy.run_module", "fake.run_module"),
])
def test_entire_ast_must_match(mutation):
    argv = legacy(ARGS)
    argv[3] = mutation(argv[3])
    assert runtime_argv(argv) is None


def test_never_executes_payload(tmp_path):
    sentinel = tmp_path / 'MUST_NOT_EXIST'
    argv = legacy(ARGS)
    argv[3] += f'; open({str(sentinel)!r}, "w").write("bad")'
    assert runtime_argv(argv) is None
    assert not sentinel.exists()


def test_ast_accepts_formatting_not_extra_statements():
    argv = legacy(ARGS)
    argv[3] = argv[3].replace('; ', '\n') + '\n# harmless formatting\n'
    assert runtime_argv(argv) == ['hermes', *ARGS]


@pytest.mark.parametrize('source', ['x' * 65537, '(' * 5000, '\x00'])
def test_bounded_malformed_source(source):
    assert runtime_argv(['python3', '-I', '-c', source]) is None


@pytest.mark.parametrize('kind', ['cli', 'gateway', 'dashboard', 'relay'])
def test_registration_uses_decoded_role(monkeypatch, kind):
    args = ARGS if kind == 'cli' else [kind, *ARGS]
    monkeypatch.setattr(admission, 'process', lambda pid: {'argv': legacy(args), 'cgroup': ''})
    monkeypatch.setattr(att, '_server', None)
    server = Mock()
    monkeypatch.setattr(att, 'RuntimeAttestor', server)
    agent = SimpleNamespace(provider='openai-codex', api_mode='codex_responses',
                            model='gpt-6-astra', base_url=att.guard.CLOUD_BASE)
    att.register(agent)
    assert server.call_count == (1 if kind == 'cli' else 0)


def test_python_module_is_not_a_model():
    assert admission.option(['python3', '-m', 'hermes_cli.main'], '-m', '--model') is None


def test_literal_run_path_root():
    argv = relaunch_command(Path('/python3'), ROOT, [str(ROOT / 'run_agent.py'), *ARGS],
                            ['python3', str(ROOT / 'run_agent.py')], None)
    assert runtime_argv(argv) == ['run_agent.py', *ARGS]


def test_source_module_selection():
    assert runtime_argv(published(['--run-module', 'hermes_cli.main', *ARGS])) == ['hermes', *ARGS]


@pytest.mark.parametrize('pid,kind', [('401845', 'cli'), ('403831', 'gateway'), ('403850', 'dashboard'), ('489505', 'cli')])
def test_exact_observed_installed_launchers(monkeypatch, pid, kind):
    import json
    import agent.runtime_argv as parser
    fixture = Path(__file__).parent / 'fixtures/installed_hermes_launchers.json'
    argv = json.loads(fixture.read_text())[pid]
    monkeypatch.setattr(parser, 'ROOT', Path('/home/hef/.hermes/hermes-agent'))
    assert runtime_kind(argv) == kind
    if kind == 'cli':
        assert admission.option(argv, '--provider') == 'openai-codex'
        assert admission.option(argv, '--model') == 'gpt-6-astra'
        if pid == '401845':
            assert admission.option(argv, '--resume') == '20261005_212622_16a997'
