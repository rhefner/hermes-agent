"""Narrow data/read-only recognition for the serving operational classifier.

Unknown syntax returns False and keeps the caller's conservative scan. This is
not a sandbox: command lookup, imported modules and operator-owned executables
have the same trust assumptions as the existing command classifier.
"""
import ast
import re
import shlex


_READ_COMMANDS = {'cat', 'head', 'tail', 'wc', 'ls', 'stat', 'sha256sum',
                  'pwd', 'printf', 'echo', 'true', 'false', 'nvidia-smi'}
_DOCKER_READ = {'inspect', 'ps', 'logs', 'stats', 'top', 'version', 'info'}
_SYSTEMCTL_READ = {'status', 'show', 'is-active', 'is-enabled', 'list-units',
                   'list-unit-files', 'list-jobs'}


def _options(words, flags=(), values=()):
    """Consume only understood options; never guess where the command starts."""
    words = list(words)
    while words and words[0].startswith('-'):
        option = words.pop(0)
        if option == '--':
            break
        if option in flags:
            continue
        if option in values and words:
            words.pop(0)
            continue
        if '=' in option and option.split('=', 1)[0] in values:
            continue
        return None
    return words


def _sudo(args, depth):
    rest = _options(args, flags={'-n', '--non-interactive'})
    return rest is not None and _simple(rest, depth + 1)


def _ssh(args, depth):
    rest = _options(args, flags={'-T', '-n'}, values={'-p', '-l', '-i'})
    # Remote argv is reassembled by SSH and parsed by the remote shell.
    return bool(rest and len(rest) > 1 and shell_readonly(' '.join(rest[1:]), depth + 1))


def _shell(args, depth):
    return len(args) == 2 and args[0] == '-c' and shell_readonly(args[1], depth + 1)


def _docker(args, depth):
    rest = _options(args, values={'-H', '--host', '-c', '--context'})
    if not rest:
        return False
    if rest[0] in _DOCKER_READ:
        return True
    if rest[0] in {'container', 'image'}:
        return len(rest) > 1 and rest[1] in {'inspect', 'ls'}
    if rest[0] == 'exec':
        nested = _options(rest[1:], flags={'-i', '-t', '-it'},
                          values={'-u', '--user', '-w', '--workdir'})
        return bool(nested and len(nested) > 1 and _simple(nested[1:], depth + 1))
    return False


def _systemctl(args, depth):
    rest = _options(args, flags={'--user', '--system', '--no-pager', '--plain',
                                '--all', '--full'})
    return bool(rest and rest[0] in _SYSTEMCTL_READ)


def _python(args, depth):
    return len(args) == 2 and args[0] == '-c' and python_readonly(args[1], depth + 1)


_COMMANDS = {'sudo': _sudo, 'ssh': _ssh, 'systemctl': _systemctl,
             **dict.fromkeys(('docker', 'podman', 'nerdctl'), _docker),
             **dict.fromkeys(('sh', 'bash', 'dash'), _shell)}


def _simple(words, depth):
    if not words or depth > 8:
        return False
    command = words[0].rsplit('/', 1)[-1]
    args = words[1:]
    if command in _READ_COMMANDS:
        # nvidia-smi also has setters; only its query/default forms are read-only.
        return command != 'nvidia-smi' or all(
            a in {'-L', '-q', '-x'} or a.startswith(('--query-', '--format='))
            for a in args)
    handler = _python if re.fullmatch(r'python(?:[23](?:\.\d+)?)?', command) else _COMMANDS.get(command)
    return handler(args, depth) if handler else False


def _unsupported_shell_syntax(text):
    quote = None
    for char in text:
        if quote == "'":
            if char == "'":
                quote = None
            continue
        if char in '$`\\':
            return True
        if char == quote:
            quote = None
        elif quote is None:
            if char in "'\"":
                quote = char
            elif char in '<>*?[]':
                return True
    return quote is not None


def shell_readonly(text, depth=0):
    # Do not lose expansion/substitution or redirect execution when dequoting.
    # Single-quoted data is inert; SSH and shell payloads are checked again at
    # their execution boundary. Unsupported grammar keeps conservative scanning.
    if depth > 8:
        return False
    heredoc = re.fullmatch(
        r"\s*(?:/usr/bin/)?python(?:[23](?:\.\d+)?)?\s+-\s+<<\s*'([A-Za-z_][A-Za-z_0-9]*)'\n(.*?)\n\1\n?",
        text, re.S)
    if heredoc:
        # The first delimiter ends the shell's heredoc, even inside a Python
        # triple-quoted string. Never swallow a subsequent shell command.
        delimiter, body = heredoc.groups()
        return delimiter not in body.splitlines() and python_readonly(body, depth + 1)
    if _unsupported_shell_syntax(text):
        return False
    lexer = shlex.shlex(text, posix=True, punctuation_chars=';&|()\n\r')
    lexer.whitespace = ' \t'
    lexer.whitespace_split = True
    lexer.commenters = ''
    try:
        words = list(lexer)
    except ValueError:
        return False
    segment = []
    for word in words:
        if word in {';', '&&', '||', '|', '&', '\n', '\r'}:
            if not segment or not _simple(segment, depth):
                return False
            segment = []
        elif word and all(c in ';&|()\n\r' for c in word):
            return False
        else:
            segment.append(word)
    return bool(segment and _simple(segment, depth))


def python_readonly(text, depth=0):
    """Recognize inert Python fixtures and literal read-only subprocess calls.

    No test-name/module-path exemption: arbitrary calls, dynamic argv, attribute
    access and imported project code retain the conservative scan. In particular
    assigning command text then passing it to an opaque helper is not proof.
    """
    if depth > 8:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    allowed = (ast.Module, ast.Expr, ast.Assign, ast.Name, ast.Load, ast.Store,
               ast.Constant, ast.List, ast.Tuple, ast.Dict, ast.Set,
               ast.Assert, ast.Compare, ast.Eq, ast.NotEq, ast.In, ast.NotIn,
               ast.UnaryOp, ast.Not, ast.USub, ast.UAdd)
    imports = set()
    for stmt in tree.body:
        # Bare expressions may also be shell executable names. Only calls have
        # an unambiguous executable boundary that is validated below.
        if isinstance(stmt, ast.Expr) and not isinstance(stmt.value, ast.Call):
            return False
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                if alias.name not in {'ast', 'json', 'shlex', 'subprocess'} or alias.asname:
                    return False
                imports.add(alias.name)
        if isinstance(stmt, ast.Assign):
            if any(not isinstance(t, ast.Name) or t.id in
                   {'print', 'len', 'ast', 'json', 'shlex', 'subprocess'} for t in stmt.targets):
                return False
    safe_calls = {'ast': {'parse', 'literal_eval'}, 'json': {'loads'}, 'shlex': {'split'}}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.alias)):
            continue
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in {'print', 'len'} and not node.keywords:
                continue
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                module = func.value.id
                if module not in imports:
                    return False
                if func.attr in safe_calls.get(module, set()) and not node.keywords:
                    continue
                if module == 'subprocess' and func.attr in {'run', 'check_output', 'check_call'}:
                    if len(node.args) != 1:
                        return False
                    try:
                        argv = ast.literal_eval(node.args[0])
                        options = {k.arg: ast.literal_eval(k.value) for k in node.keywords}
                    except (ValueError, TypeError):
                        return False
                    if not set(options) <= {'check', 'capture_output', 'text', 'timeout', 'shell'}:
                        return False
                    if options.get('shell', False) is not False:
                        return False
                    if not isinstance(argv, (list, tuple)) or not all(isinstance(w, str) for w in argv):
                        return False
                    if not _simple(argv, depth + 1):
                        return False
                    continue
            return False
        if isinstance(node, ast.Attribute):
            # Attributes are accepted only as a validated call's callee.
            if not any(isinstance(parent, ast.Call) and parent.func is node
                       for parent in ast.walk(tree)):
                return False
            continue
        if isinstance(node, ast.keyword):
            continue
        if not isinstance(node, allowed):
            return False
    return True
