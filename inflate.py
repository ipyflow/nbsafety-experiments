#!/usr/bin/env python
import json
import subprocess
import sys

SESSION_START = '# + Cell 1\n'
MAX_SESSIONS = 1
MAX_REPOS = 100

def main():
    with open('data.json') as f:
        data = json.loads(f.read())
    new_entries = []
    successes = 0
    for entry in data:
        print(entry)
        try:
            subprocess.check_output(['wget', '-O', 'temp.sqlite', f'{entry["html_url"]}?raw=true'], stderr=subprocess.STDOUT)
            subprocess.check_call('sqlite3 temp.sqlite \"select \'\n# + Cell \'|| line || char(10) || source || char(10) from history;\" > temp.py', shell=True)
            with open('temp.py') as f:
                sessions = f.read().split(SESSION_START)
            sessions = map(lambda sess: sess.strip(), sessions)
            sessions = filter(lambda sess: len(sess) > 0, sessions)
            sessions = map(lambda sess: SESSION_START + sess, sessions)
            sessions = filter(lambda sess: len(sess.split('\n# + Cell')) >= 10, sessions)
            sessions = list(sessions)
            if len(sessions) == 0:
                raise ValueError('not enough stuff')
            nb_sessions = []
            for sess in sessions:
                try:
                    with open('temp.py', 'w') as f:
                        f.write(sess)
                    subprocess.check_call(['jupytext', '--to', 'ipynb', 'temp.py', '--output', 'temp.ipynb'])
                    with open('temp.ipynb') as f:
                        nb_sessions.append(json.loads(f.read()))
                except:
                    continue
                if len(nb_sessions) >= MAX_SESSIONS:
                    break
            if len(nb_sessions) == 0:
                raise ValueError('not enough stuff')
            entry['sessions'] = nb_sessions
            new_entries.append(entry)
        except KeyboardInterrupt:
            break
        except:
            continue
        successes += 1
        if MAX_REPOS > 0 and successes >= MAX_REPOS:
            break
    with open('out.json', 'w') as f:
        f.write(json.dumps(new_entries, indent=2))
    subprocess.check_call(['rm', 'temp.sqlite'])
    subprocess.check_call(['rm', 'temp.py'])
    subprocess.check_call(['rm', 'temp.ipynb'])


if __name__ == '__main__':
    sys.exit(main())
