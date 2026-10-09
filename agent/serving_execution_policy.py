"""Shared operational guard for recognizable serving-control commands/code.

This is deliberately NOT a shell parser, syscall filter, or same-UID sandbox.
Opaque scripts/encoded commands/raw sockets cannot be proven safe by text scanning.
Sanctioned entrypoints enforce admission themselves; this closes common direct
terminal/SSH/execute_code paths without banning read-only SSH or changing approval.
"""
import re
import shlex

from agent import serving_admission
from agent.serving_readonly import python_readonly, shell_readonly

TARGET = re.compile(r'(?:spark[-_ ]?[123]|spark-lane|spark3|glm[-_ ]?5|glm53|vllm|sglang|'
                    r'ollama|comfyui|tensorfold|primary(?:-ablit)?|secondary|'
                    r'192\.168\.(?:0\.(?:39|33|213)|100\.(?:10|11))|'
                    r'active-inference-lane|spark-model-surgery|lanectl)', re.I)
MUTATE = re.compile(r'\b(?:stop|start|restart|recreate|rm|remove|kill|pkill|killall|'
                    r'down|up|(?<![.\w-])run(?![\w-])|create|update|scale|switch|cutover|rollback|restore|'
                    r'disable|enable|mask|unmask|reboot|shutdown|poweroff|exec|'
                    r'reload|reload-or-restart|try-restart)\b', re.I)
EXEC = re.compile(r'\b(?:docker|podman|nerdctl|systemctl|service|ssh|paramiko|fabric|'
                  r'ansible|subprocess|os\.system|pkill|killall|kill|curl|requests|httpx|'
                  r'lanectl|spark3-lane-switch)\b', re.I)
WRAPPER = re.compile(r'(?:glm53-(?:mia-ablit-cutover\.py|tf-cutover-v1\.7\.1\.sh)|'
                     r'spark-model-surgery-routing\.py)', re.I)


def _git_add_scan_view(normalized):
    # A standalone `git add` whose arguments are plain path operands stages
    # files without executing anything: interior path names must not
    # fabricate the conjunction. Anything richer (command substitution,
    # redirects, compound junctions inside the argument run) keeps the
    # conservative whole-command scan; only flag tokens survive as scan
    # evidence because path operands are data, not execution proof.
    match = re.fullmatch(r'git\s+add\s+(\S.*?)\s*', normalized)
    if not match or re.search(r'[$`"&;|<>()]', match.group(1)):
        return normalized
    return 'git add ' + ' '.join(w for w in match.group(1).split()
                                  if w.startswith('-'))


def _plain_git_diff(text):
    """Recognize only standalone diff with data operands, not shell execution.

    This is an operational classifier, not Git's security sandbox. Explicit
    helper/config/output options and shell syntax retain conservative scanning.
    Operators should also use --no-ext-diff --no-textconv for inspection.
    """
    if re.search(r'[\n\r$`;&|<>()\\\\*?\[\]{}#]', text):
        return False
    try:
        words = shlex.split(text, comments=False)
    except ValueError:
        return False
    if words[:2] != ['git', 'diff']:
        return False
    safe_flags = {'--no-ext-diff', '--no-textconv', '--stat', '--name-only',
                  '--name-status', '--check', '--cached', '--staged', '--quiet',
                  '--exit-code', '--no-color', '--color=never', '-p', '--patch'}
    paths = False
    for word in words[2:]:
        if word == '--':
            paths = True
        elif not paths and word.startswith('-') and word not in safe_flags:
            return False
    return True


def recognizable_mutation(text):
    if not isinstance(text, str):
        return False
    if _plain_git_diff(text) or shell_readonly(text):
        return False
    # This entrypoint also receives shell text. A Python string expression can
    # be a shell command substitution, so never infer Python across expansions.
    if '$' not in text and '`' not in text and python_readonly(text):
        return False
    # Handle shell token splicing and Python argv-list punctuation, retaining raw
    # text too so quoted remote payloads and nested interpreter code remain visible.
    try:
        normalized = ' '.join(shlex.split(text, comments=True))
    except ValueError:
        normalized = text
    normalized = normalized.replace("'", '').replace('"', '')
    combined = text + '\n' + normalized
    if WRAPPER.search(combined):
        # Legacy paths are intentionally conservative; status via new CLI is read-only.
        return True
    # Shell newlines are command separators; shlex normalization erases them.
    # Never grant the operand-only exception after losing that raw structure.
    stripped = normalized if '\n' in text or '\r' in text else _git_add_scan_view(normalized)
    scan = stripped if stripped is not normalized else combined
    if TARGET.search(scan) and MUTATE.search(scan) and EXEC.search(scan):
        return True
    # Aggregate daemon/all-container mutations have implicit serving targets.
    return bool(re.search(r'\b(?:docker|podman)\b.*\b(?:stop|kill|rm|restart|prune)\b.*(?:\$|--all|\s-a\b)', normalized)
                or re.search(r'\bsystemctl\b.*\b(?:stop|restart|kill)\b.*\b(?:docker|containerd)\b', normalized))


def check(text):
    if recognizable_mutation(text):
        from agent.backend_isolation import configuration, require_mutation
        value = configuration()
        if value is None:
            serving_admission.require_admission()
            return
        hosts = set(re.findall(r'spark[-_ ]?([123])', text, re.I))
        targets = set()
        if hosts & {'1', '2'} or re.search(r'glm53-(?:mia-ablit-cutover|tf-cutover)', text):
            targets.add('spark-pair')
        if '3' in hosts:
            targets.add('spark-3')
        # Ambiguous aggregate mutations need every inventoried backend excluded.
        require_mutation(sorted(targets or value['backends']))
