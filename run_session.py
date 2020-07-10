#!/usr/bin/env ipython3
import argparse
import ast
import black
import contextlib
import logging
import re
import sqlite3
import sys

from IPython import get_ipython

try:
    from cfuzzyset import cFuzzySet as FuzzySet
except ImportError:
    from fuzzyset import FuzzySet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CELL_ID_BY_SOURCE = {}
MATCHING_CELL_THRESHOLD = 0.5
EXECUTED_CELLS = FuzzySet()


def make_cell_counter():
    current = 0

    def _counter():
        nonlocal current
        ret = current
        current += 1
        return ret
    return _counter


get_new_cell_id = make_cell_counter()


@contextlib.contextmanager
def redirect_std_streams():
    with open('/dev/null', 'w') as devnull:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        old_stdout.flush()
        old_stderr.flush()
        sys.stdout, sys.stderr = devnull, devnull
        yield
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout, sys.stderr = old_stdout, old_stderr


def get_cell_id_for_source(source):
    match = EXECUTED_CELLS.get(source)
    if match is None:
        score, old_source = -1, None
    else:
        score, old_source = match[0]
    if score >= MATCHING_CELL_THRESHOLD:
        cell_id = CELL_ID_BY_SOURCE[old_source]
    else:
        cell_id = get_new_cell_id()
    CELL_ID_BY_SOURCE[source] = cell_id
    EXECUTED_CELLS.add(source)
    return cell_id


class ExceptionWrapTransformer(ast.NodeTransformer):
    def visit(self, node):
        try_stmt = ast.Try()
        try_stmt.body = node.body
        handler = ast.ExceptHandler()
        handler.name = 'e'
        handler.type = ast.Name('Exception', ctx=ast.Load())
        handler.body = ast.parse("logger.warning('An exception occurred: %s', e)").body
        try_stmt.handlers = [handler]
        try_stmt.orelse = []
        try_stmt.finalbody = []
        node.body = [try_stmt]
        return node


def main(args, conn):
    curse = conn.cursor()
    cell_submissions = curse.execute(f"""
SELECT source FROM cell_execs
WHERE trace = {args.trace} AND session = {args.session}
ORDER BY counter ASC
    """)
    cell_submissions = list(map(lambda t: t[0], cell_submissions))
    curse.close()
    if args.use_nbsafety:
        from nbsafety.safety import DependencySafety
        safety = DependencySafety(cell_magic_name='_NBSAFETY_STATE')
    else:
        safety = None
    get_ipython().ast_transformers.append(ExceptionWrapTransformer())
    for cell_source in cell_submissions:
        lines = cell_source.split('\n')
        new_lines = []
        for line in lines:
            if line.startswith('get_ipython()'):
                if 'pylab' not in line and 'matplotlib' not in line and 'time' not in line:
                    continue
            new_lines.append(line)
        cell_source = '\n'.join(new_lines).strip()
        if cell_source == '':
            continue
        cell_id = get_cell_id_for_source(cell_source)
        logger.info('About to run cell %d', cell_id)

        try:
            cell_source = black.format_file_contents(cell_source, fast=False, mode=black.FileMode())
        except:  # noqa
            pass
        cell_source = re.sub('from sklearn.cross_validation', 'from sklearn.model_selection', cell_source)
        cell_source = re.sub('from sklearn.externals import joblib', 'import joblib', cell_source)
        if safety is None:
            get_ipython().run_cell(cell_source, silent=True)
        else:
            safety.set_active_cell(cell_id)
            get_ipython().run_cell_magic(safety.cell_magic_name, None, cell_source)
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--trace', help='Which trace the session to run is in', required=True)
    parser.add_argument('-s', '--session', help='Which session to run', required=True)
    parser.add_argument('--use-nbsafety', '--nbsafety', action='store_true', help='Whether to use nbsafety')
    args = parser.parse_args()
    conn = sqlite3.connect('./data/traces.sqlite')
    ret = 0
    try:
        with redirect_std_streams():
            ret = main(args, conn)
    except Exception as e:
        logger.error('Exception occurred in outer context: %s', e)
        ret = 1
    finally:
        conn.close()
        sys.exit(ret)
