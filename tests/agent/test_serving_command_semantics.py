"""Data operands are not execution; unknown code still needs admission."""
import shlex
import subprocess
import sys

import pytest

from agent import serving_execution_policy as policy


@pytest.mark.parametrize('text', [
    'ssh spark-1 docker inspect --format "{{.HostConfig.RestartPolicy}}" glm53-flash-tf',
    'ssh spark-2 "docker ps --filter name=restart --format {{.Names}}"',
    'docker logs glm53-start-worker',
    'systemctl --user show spark3-secondary.service -p ExecStart',
    'ssh spark-1 "docker inspect glm53-flash-tf && docker logs glm53-start-worker"',
    'cat "spark3/docker-restart-fixture.json"',
    'printf "%s" "docker restart spark-3"',
    "printf '%s' '$(docker restart spark-3)'",
    'cat "spark3/docker-[restart]-fixture.json"',
    'cat scripts/glm53-mia-ablit-cutover.py',
    'docker exec glm53-flash-tf cat /etc/start-config',
    "fixture = {'argv': ['ssh', 'spark-1', 'docker', 'restart', 'glm53-flash-tf']}\nprint(fixture)",
    "import ast\nfixture = 'subprocess.run([\"ssh\", \"spark-1\", \"docker restart glm53\"])'\nparsed = ast.parse(fixture)",
    "import subprocess\nsubprocess.run(['ssh', 'spark-1', 'docker inspect glm53-start-worker'], check=True, capture_output=True)",
    "python3 -c " + shlex.quote("fixture = 'docker restart spark-3'; print(fixture)"),
    "python3 -c " + shlex.quote("fixture = ['docker', 'restart', 'spark-3']; print(fixture)"),
    "python3 - <<'PY'\nfixture = ['docker', 'restart', 'spark-3']\nprint(fixture)\nPY",
])
def test_readonly_and_source_fixtures_do_not_request_admission(text, monkeypatch, tmp_path):
    from agent import backend_isolation

    def unexpected():
        pytest.fail('read-only input must not consult live isolation state')

    monkeypatch.setattr(backend_isolation, 'configuration', unexpected)
    assert not policy.recognizable_mutation(text)
    policy.check(text)
    # Exercise the literal Python fixtures for real, in a private working dir.
    # Inspection commands above are classifier inputs only: never contact hosts.
    if text.startswith(('fixture =', 'import ast')):
        sandbox = tmp_path / 'fixture-work'
        sandbox.mkdir()
        result = subprocess.run([sys.executable, '-c', text], cwd=sandbox,
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == 0, result.stderr
        assert not list(sandbox.iterdir())


@pytest.mark.parametrize('text', [
    'ssh spark-1 "docker inspect glm53-flash-tf; docker restart glm53-flash-tf"',
    'ssh spark-1 docker inspect glm53-flash-tf && ssh spark-3 docker stop secondary',
    'docker logs glm53-flash-tf\nsystemctl restart spark3-secondary.service',
    'docker inspect "$(ssh spark-1 docker stop glm53-flash-tf)"',
    'printf "%s" "`ssh spark-1 docker stop glm53-flash-tf`"',
    'cat <(ssh spark-1 docker stop glm53-flash-tf)',
    "ssh spark-1 'docker inspect $(docker stop glm53-flash-tf)'",
    "bash -c 'docker inspect $(docker stop glm53-flash-tf)'",
    'echo safe > spark3/docker-restart-config',
    "python3 - <<PY\nfixture = '$(docker restart spark-3)'\nPY",
    "python3 - <<'PY'\nfixture = '''docker restart spark-3\nPY\ndocker restart spark-3\nPY",
    'docker exec glm53-flash-tf sh -c "systemctl restart vllm"',
    'ssh -o ProxyCommand="docker stop glm53-flash-tf" spark-1 docker inspect glm53-flash-tf',
    'cat scripts/glm53-mia-ablit-cutover.py | python3',
    'python3 tests/restart-spark3-docker-fixture.py',
    "import subprocess\nsubprocess.run(['ssh', 'spark-1', 'docker restart glm53-flash-tf'])",
    "import subprocess\ncmd = ['ssh', 'spark-1', 'docker restart glm53-flash-tf']\nsubprocess.run(cmd)",
    "import subprocess\nsubprocess.run('docker restart glm53-flash-tf', shell=True)",
    "fixture = 'docker restart spark-3'\nexec(fixture)",
    "fixture = 'docker restart spark-3'\nunknown_helper(fixture)",
    "python3 -c " + shlex.quote("import os; os.system('docker restart spark-3')"),
    "python3 -c " + shlex.quote("fixture = 'docker restart spark-3' # data\nimport os; os.system(fixture)"),
    'docker inspect glm53-flash-tf &&\n docker restart glm53-flash-tf',
    "import subprocess\nsubprocess.run(['docker', 'inspect', 'spark-3'], executable='docker-restart')",
    "import ast\nprint = ast.literal_eval\nprint('docker restart spark-3')",
    "python3 - <<'PY'\nimport os\nos.system('docker restart spark-3')\nPY",
    "python3 - <<'PY'\nfixture = 'docker restart spark-3'\nPY\nssh spark-3 docker stop secondary",
    'docker stop $(docker ps -q)',
    'systemctl restart docker.service',
    '"$(docker restart spark-3)"',
    "'glm53-mia-ablit-cutover.py'",
    "'glm53-mia-ablit-cutover.py'; fixture = 1",
    'docker; restart; spark3',
    'ssh spark-2 do\'ck\'er st\'op\' glm53-flash-tf',
])
def test_nested_mutations_and_opaque_code_still_request_admission(text, monkeypatch):
    from agent import backend_isolation

    monkeypatch.setattr(backend_isolation, 'configuration', lambda: {'backends': ['spark-pair', 'spark-3']})
    admitted = []
    monkeypatch.setattr(backend_isolation, 'require_mutation', admitted.append)
    assert policy.recognizable_mutation(text)
    policy.check(text)
    assert admitted, 'recognition must still reach actual backend mutation admission'
