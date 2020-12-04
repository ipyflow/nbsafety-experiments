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
    '%xls%',
    '%keras%',
    '%subprocess%',
    '%readline%',
    '%shutil%',
    '%pyspark%',
    '%pyscopg2%',
    '%sqlite3%',
    '%mongo%',
    '%mysql%',
    '%cuda%',
    '%requests%',  # TODO: check
    '%codecs%',  # TODO: check
    '%grader%',
    '%threading%',
    '%from magic import%',
    '%import magic%',
    '%plotly%',
    '%正常NST%',
    '%os.system%',
    '%os.walk%',
    '%glob%',
    '%nmap%',
    '%pygoogle%',
    '%pymc3%',
    '%read_login%',
    '%Exscript%',
    '%import pwn%',
    '%from pwn import%',
    '%turtle%',
    '%weights2.tsv%',
    '%all.lsa.s21.list%',
    '%pwcKyotoAll.en%',
    '%update_chis.sh%',
    '%DR1_clean_band_RFI_Pr_Data.csv%',
    '%MeerKAT_anntenna_pair_length.csv%',
    '%pyiron%',
]


def format_filter_pattern(patt):
    return f"""
             SELECT DISTINCT trace, session
             FROM cell_execs
             WHERE source LIKE {repr(patt)}
    """.strip()


def main(args, conn):
    conn.execute("PRAGMA read_uncommitted = true;")
    ret = 0
    newline = '\n'
    results = conn.execute(f"""
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
        {'UNION SELECT trace, session FROM replay_stats WHERE version = ' + str(args.version) if args.skip_already_replayed else ''}
     )
    """).fetchall()
    for idx, (trace, session) in enumerate(results):
        logger.info('Running trace %d session, %d (%d of %d total)', trace, session, idx + 1, len(results))
        session_ret = subprocess.call(f'./replay-session.py -- -t {trace} -s {session} -v {args.version} --nbsafety', shell=True)
        if session_ret != 0:
            logger.warning('trace %d, session %d got nonzero return code %d', trace, session, session_ret)
        ret += session_ret
    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--min-cells', type=int, default=50)
    parser.add_argument('-v', '--version', type=int, required=True)
    parser.add_argument('--skip-already-replayed', action='store_true')
    args = parser.parse_args()
    ret = 0
    conn = sqlite3.connect('./data/traces.sqlite', timeout=30, isolation_level=None)
    try:
        ret = main(args, conn)
    except:
        logger.error(traceback.format_exc())
        ret = 1
    finally:
        conn.close()
        sys.exit(ret)
