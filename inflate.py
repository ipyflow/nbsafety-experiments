#!/usr/bin/env python
import argparse
from collections import defaultdict
import glob
import json
import logging
import pathlib
import re
import sqlite3
import subprocess
import sys

DEFAULT_MAX_SESSIONS = -1
DEFAULT_NUM_REPOS = -1
DEFAULT_MIN_CELLS_PER_SESSION = 10
NB_TRACE_DIR = pathlib.Path('./data/traces')
IMPORT_RE = re.compile(r'(^|\n) *(from|import) *(\w+)')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_imports(s):
    return set(map(lambda m: m[2], IMPORT_RE.findall(s)))


def make_session_filters(args, allowed_imports):
    return [
        lambda sess: 'spark' not in sess and 'SPARK' not in sess,
        lambda sess: 'sklearn.datasets' in sess or 'uci.edu/ml' in sess,
        lambda sess: len(sess.split('\n# @@ Cell')) >= args.min_cells_per_session,
        # lambda sess: get_imports(sess).issubset(allowed_imports)
    ]


def main(args, conn):
    all_imports = set()
    per_trace_imports = defaultdict(set)
    with open('./data/allowed-imports.json') as f:
        allowed_imports = set(json.loads(f.read())['allow_imports'])
        session_filters = make_session_filters(args, allowed_imports)
    successes = 0
    NB_TRACE_DIR.mkdir(exist_ok=True)
    curse = conn.cursor()
    all_traces = curse.execute('SELECT trace FROM cell_execs')
    all_traces = set(tup[0] for tup in all_traces)
    curse.close()
    total_unfiltered = 0
    for trace in all_traces:
        logger.info(f'Working on entry {trace + 1} of {len(all_traces)}')
        try:
            curse = conn.cursor()
            sessions = map(lambda t: t[0].strip(), curse.execute(f"""
SELECT GROUP_CONCAT('# @@ Cell ' || counter || '\n' || source || '\n', '\n')
FROM cell_execs
WHERE trace = {trace}
GROUP BY session
ORDER BY session, counter ASC"""))
            sessions = filter(lambda sess: len(sess) > 0, sessions)
            for sess_filter in session_filters:
                sessions = filter(sess_filter, sessions)
            sessions = list(sessions)
            total_unfiltered += len(sessions)
            if len(sessions) == 0:
                raise ValueError('not enough stuff')
            trace_path = NB_TRACE_DIR.joinpath(str(trace))
            trace_path.mkdir()
            for sess_idx, session in enumerate(sessions):
                if args.max_sessions > 0 and sess_idx >= args.max_sessions:
                    break
                session_imports = get_imports(session)
                all_imports |= session_imports
                per_trace_imports[trace] |= session_imports
                with open(trace_path.joinpath(f'{sess_idx}.py'), 'w') as f:
                    f.write(session)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.info("Exception while grabbing nb history for repo: %s", e)
            continue
        finally:
            curse.close()
        successes += 1
        if args.num_repos > 0 and successes >= args.num_repos:
            break
    logger.info(f'total unfiltered sessions: {total_unfiltered}')
    imports_json = {
        'all_imports': sorted(all_imports),
        'per_trace_imports': {k: sorted(v) for k, v in per_trace_imports.items()}
    }
    with open('./data/imports.json', 'w') as f:
        f.write(json.dumps(imports_json, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Grab notebook traces from github')
    parser.add_argument('--num-repos', type=int, default=DEFAULT_NUM_REPOS)
    parser.add_argument('--max-sessions', type=int, default=DEFAULT_MAX_SESSIONS)
    parser.add_argument('--min-cells-per-session', '--min-cells', type=int, default=DEFAULT_MIN_CELLS_PER_SESSION)
    args = parser.parse_args()
    conn = sqlite3.connect('./traces.sqlite')
    try:
        sys.exit(main(args, conn))
    finally:
        conn.close()
