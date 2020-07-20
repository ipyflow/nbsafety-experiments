#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import sqlite3
import subprocess
import sys
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


FILTER_PATTERNS = [
    '%get_ipython().magic(%run%',
    '%get_ipython().magic(%load%',
    '%get_ipython().run_line_magic(%run%',
    '%get_ipython().run_line_magic(%load%',
    '%pd.read_csv(%data%',
    '%keras%',
    '%subprocess%',
    '%shutil%',
    '%pyspark%',
    '%pyscopg2%',
    '%sqlite3%',
    '%mongo%',
    '%mysql%',
    '%cuda%',
    '%requests%',  # TODO: check
    '%grader%',
    '%threading%',
    '%from magic import%',
    '%import magic%',
    '%plotly%',
    '%正常NST%'
]


def format_filter_pattern(patt):
    return f"""
             SELECT DISTINCT trace, session
             FROM cell_execs
             WHERE source LIKE {repr(patt)}
    """.strip()


def main(args, conn):
    curse = conn.cursor()
    ret = 0
    try:
        newline = '\n'
        results = curse.execute(f"""
    SELECT trace, session
    FROM cell_execs
    GROUP BY trace, session
    HAVING max(counter) >= {args.min_cells} 
        EXCEPT
    SELECT *
    FROM (
             SELECT trace, session
             FROM bad_sessions
             UNION
             {(
                newline + 'UNION' + newline
              ).join(format_filter_pattern(patt) for patt in FILTER_PATTERNS)}
         )
        """)
        results = list(results)
    finally:
        curse.close()
    for idx, (trace, session) in enumerate(results):
        logger.info('Running trace %d session, %d (%d of %d total)', trace, session, idx + 1, len(results))
        session_ret = subprocess.call(f'./replay-session.py -- -t {trace} -s {session} --nbsafety', shell=True)
        if session_ret != 0:
            logger.warning('trace %d, session %d got nonzero return code %d', trace, session, session_ret)
        ret += session_ret
    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--min-cells', type=int, default=50)
    args = parser.parse_args()
    ret = 0
    conn = sqlite3.connect('./data/traces.sqlite')
    try:
        ret = main(args, conn)
    except:
        logger.error(traceback.format_exc())
        ret = 1
    finally:
        conn.close()
        sys.exit(ret)
