#!/usr/bin/env python
import argparse
import glob
import json
import logging
import os
import sqlite3
import subprocess
import sys

DEFAULT_NUM_REPOS = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(args, conn):
    with open('repos.json') as f:
        repos = json.loads(f.read())
    successes = 0
    curse = conn.cursor()
    seen_repos = curse.execute('select repo from traces')
    seen_repos = set(tup[0] for tup in seen_repos)
    curse.close()
    if os.path.exists('seen-repos.json'):
        with open('seen-repos.json') as f:
            seen_repos |= set(map(int, json.loads(f.read())['seen']))
    for repo_idx, entry in enumerate(repos):
        repo_id = int(entry['repo']['id'])
        if repo_id in seen_repos:
            logger.info('Skipping already download nb trace for repo %d', repo_id)
            continue
        seen_repos.add(repo_id)
        logger.info("Working on entry %s", entry['html_url'])
        try:
            subprocess.check_output(['wget', '-q', '-O', 'temp.sqlite', f'{entry["html_url"]}?raw=true'])
            curse = conn.cursor()
            curse.execute('attach "temp.sqlite" as t')
            curse.execute(f'insert into traces select {repo_id}, session, line, source from t.history')
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
                subprocess.check_call(['rm', '-f'] + glob.glob('temp.sqlite*'))
        successes += 1
        if 0 < args.num_repos <= successes:
            break
    seen_repos = {'seen': sorted(seen_repos)}
    with open('seen-repos.json', 'w') as f:
        f.write(json.dumps(seen_repos, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Grab notebook traces from github')
    parser.add_argument('--num-repos', type=int, default=DEFAULT_NUM_REPOS)
    args = parser.parse_args()
    conn = sqlite3.connect('./traces.sqlite')
    try:
        sys.exit(main(args, conn))
    finally:
        conn.commit()
        conn.close()
        subprocess.check_call(['rm', '-f'] + glob.glob('temp.sqlite*'))
