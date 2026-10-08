"""Offline diff operands must not become serving execution evidence."""
import pytest
from agent import serving_execution_policy as policy

PATH = 'users/hef/local-ai-services/spark3-scripts/spark3-lane-switch'

@pytest.mark.parametrize('command', [
    f'git diff 039531e4^ 039531e4 -- {PATH}',
    f'git diff --no-ext-diff --no-textconv --stat HEAD -- {PATH}',
    'git diff HEAD -- users/hef/local-ai-services/spark-lanes/glm53-mia-ablit-cutover.py',
])
def test_plain_diff_operands(command):
    assert not policy.recognizable_mutation(command)

@pytest.mark.parametrize('command', [
    f'git diff -- {PATH}; spark3-lane-switch secondary',
    f'git diff -- {PATH}\nspark3-lane-switch secondary',
    f'git diff -- {PATH}\rspark3-lane-switch secondary',
    'git diff $(spark3-lane-switch secondary)',
    'git diff `spark3-lane-switch secondary`',
    f'git diff -- {PATH} > result',
    f'git diff --ext-diff -- {PATH}',
    f'git diff --textconv -- {PATH}',
    f'git diff --output={PATH}',
    f'git -c diff.external=spark3-lane-switch diff -- {PATH}',
    'spark3-lane-switch secondary',
    'ssh spark-1 systemctl restart tensorfold',
])
def test_execution_and_rich_forms_remain_guarded(command):
    assert policy.recognizable_mutation(command)
