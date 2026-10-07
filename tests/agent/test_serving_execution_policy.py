"""Regressions for the serving classifier's operand-vs-execution boundary.

Regression for the candidate checkpoint follow-up to 95bba724e8: a bare
``git add`` whose path operands merely CONTAIN serving names
(spark3-lane-switch, spark3, ...) falsely satisfied
TARGET+MUTATE+EXEC and forced maintenance admission for offline staging.
"""
from agent import serving_execution_policy as policy


def test_offline_git_add_path_operand_is_not_a_mutation():
    # The reported false positive: TARGET/MUTATE/EXEC all match inside the
    # path operand's interior names, but nothing executes.
    assert not policy.recognizable_mutation(
        'git add users/hef/local-ai-services/spark3-scripts/spark3-lane-switch')


def test_git_add_flagged_paths_and_bare_operand_stay_harmless():
    assert not policy.recognizable_mutation('git add -A spark3-lane-switch')
    assert not policy.recognizable_mutation('git add -- spark3/secondary.txt')
    assert not policy.recognizable_mutation('git add spark3-lane-switch')


def test_quoted_git_add_path_is_still_harmless():
    assert not policy.recognizable_mutation(
        "git add 'spark3-lane-switch secondary.service'")


def test_execution_appended_after_git_add_still_guarded():
    assert policy.recognizable_mutation(
        'git add notes.md && spark3-lane-switch secondary --restart')


def test_git_add_command_substitution_still_guarded():
    # Substitution runs a real program; conservative whole-command scan.
    assert policy.recognizable_mutation('git add $(spark3-lane-switch --restart)')


def test_newline_separated_execution_still_guarded():
    assert policy.recognizable_mutation('git add notes.md\nspark3-lane-switch secondary')


def test_direct_switch_invocation_untouched():
    # Not a git add: the path IS the executable and must keep matching.
    assert policy.recognizable_mutation('bin/spark3-lane-switch secondary')
    assert policy.recognizable_mutation('python scripts/glm53-mia-ablit-cutover.py')


def test_git_add_redirect_form_stays_conservative():
    assert policy.recognizable_mutation('git add spark3-lane-switch > /tmp/log 2>&1')
