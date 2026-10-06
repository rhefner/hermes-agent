"""Shared operational guard for recognizable serving-control commands/code.

This is deliberately NOT a shell parser, syscall filter, or same-UID sandbox.
Opaque scripts/encoded commands/raw sockets cannot be proven safe by text scanning.
Sanctioned entrypoints enforce admission themselves; this closes common direct
terminal/SSH/execute_code paths without banning read-only SSH or changing approval.
"""
import re
import shlex

from agent import serving_admission

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


def recognizable_mutation(text):
    if not isinstance(text, str):
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
    if TARGET.search(combined) and MUTATE.search(combined) and EXEC.search(combined):
        return True
    # Aggregate daemon/all-container mutations have implicit serving targets.
    return bool(re.search(r'\b(?:docker|podman)\b.*\b(?:stop|kill|rm|restart|prune)\b.*(?:\$|--all|\s-a\b)', normalized)
                or re.search(r'\bsystemctl\b.*\b(?:stop|restart|kill)\b.*\b(?:docker|containerd)\b', normalized))


def check(text):
    if recognizable_mutation(text):
        serving_admission.require_admission()
