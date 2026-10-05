#!/usr/bin/env python3
"""Default-profile finite monitor launcher; --check never sends or loads secrets."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    default = (Path.home() / '.hermes').resolve()
    if Path(os.environ.get('HERMES_HOME', str(default))).resolve() != default:
        parser.error('this launcher is default-profile only')
    os.environ['HERMES_HOME'] = str(default)
    from gateway.wake_receipts import settings
    from gateway.wake_monitor import main as monitor
    if args.check:
        print(json.dumps({'imports': 'ok', 'profile': 'default', 'enabled': settings()['enabled'], 'sent': False}))
        return 0
    from hermes_cli.env_loader import load_hermes_dotenv
    load_hermes_dotenv(hermes_home=str(default))
    return asyncio.run(monitor())


if __name__ == '__main__':
    raise SystemExit(main())
