#!/usr/bin/env python
import argparse
import glob
import json
import logging
import os
import pathlib
import sqlite3
import subprocess
import sys

DEFAULT_NUM_REPOS = 10
TEMP_DIR = pathlib.Path('./data/temp')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(args, conn):
    TEMP_DIR.mkdir(exist_ok=True)
    with open('./data/traces.json') as f:
        trace_json = json.loads(f.read())
    successes = 0
    curse = conn.cursor()
    seen_traces = curse.execute('SELECT trace FROM cell_execs')
    seen_traces = set(tup[0] for tup in seen_traces)
    curse.close()
    if os.path.exists('./data/seen-traces.json'):
        with open('./data/seen-traces.json') as f:
            seen_traces |= set(map(int, json.loads(f.read())['seen']))
    for entry in trace_json:
        trace_id = int(entry['id'])
        if trace_id in seen_traces:
            logger.info('Skipping already download nb trace %d', trace_id)
            continue
        seen_traces.add(trace_id)
        logger.info("Working on entry %s", entry['html_url'])
        try:
            subprocess.check_output(['wget', '-q', '-O', './data/temp/temp.sqlite', f'{entry["html_url"]}?raw=true'])
            curse = conn.cursor()
            curse.execute('attach "./data/temp/temp.sqlite" as t')
            curse.execute(f"""
INSERT INTO cell_execs
SELECT {trace_id}, session, line AS counter, source
FROM t.history""".strip())
            conn.commit()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.info("Exception while grabbing nb history for repo: %s", e)
            continue
        finally:
            try:
                curse.execute('detach t')
            except Exception as e:
                logger.info("Exception while detaching from temp.sqlite: %s", e)
            finally:
                curse.close()
                subprocess.check_call(['rm', '-f'] + glob.glob('./data/temp/*'))
        successes += 1
        if 0 < args.num_repos <= successes:
            break
    seen_traces = {'seen': sorted(seen_traces)}
    with open('./data/seen-traces.json', 'w') as f:
        f.write(json.dumps(seen_traces, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Grab notebook traces from github')
    parser.add_argument('--num-repos', type=int, default=DEFAULT_NUM_REPOS)
    args = parser.parse_args()
    conn = sqlite3.connect('./data/traces.sqlite')
    try:
        sys.exit(main(args, conn))
    finally:
        conn.commit()
        conn.close()
        subprocess.check_call(['rm', '-f'] + glob.glob('./data/temp/*'))
