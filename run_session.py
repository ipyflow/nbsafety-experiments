#!/usr/bin/env ipython3
# -*- coding: utf-8 -*-
import argparse
import ast
import black
import collections
import contextlib
import logging
import sqlite3
import sys

from IPython import get_ipython

try:
    from cfuzzyset import cFuzzySet as FuzzySet
except ImportError:
    from fuzzyset import FuzzySet

from ast_utils import ExceptionWrapTransformer, FilenameExtractTransformer, GatherImports
from resolvers import PipResolver

logger = logging.getLogger(__name__)

CELL_ID_BY_SOURCE = {}
MATCHING_CELL_THRESHOLD = 0.5
EXECUTED_CELLS = FuzzySet()


def setup_logging(log_to_stderr=True):
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    info_handler = logging.FileHandler('session.info.log', mode='w')
    info_handler.setLevel(logging.INFO)
    warning_handler = logging.FileHandler('session.warnings.log', mode='w')
    warning_handler.setLevel(logging.WARN)
    error_handler = logging.FileHandler('session.errors.log', mode='w')
    error_handler.setLevel(logging.ERROR)
    handlers = [info_handler, warning_handler, error_handler]
    if log_to_stderr:
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(formatter)
        logging.root.addHandler(stderr_handler)
        logger.addHandler(stderr_handler)
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logging.root.addHandler(handler)


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
def redirect_std_streams_to(redirect_fname):
    with open(redirect_fname, 'w') as devnull:
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


def resolve_packages(cell_submissions):
    import_gatherer = GatherImports()
    for cell_source in cell_submissions:
        try:
            import_gatherer.visit(ast.parse(cell_source))
        except SyntaxError:
            continue
    success_packages = []
    failed_packages = []
    imports_by_pkg = collections.defaultdict(list)
    for import_stmt, pkg_names in import_gatherer.import_stmts:
        for pkg in pkg_names:
            imports_by_pkg[pkg].append(import_stmt)
    for pkg, import_stmts in imports_by_pkg.items():
        logger.info('resolving package %s...', pkg)
        resolver = PipResolver(pkg, import_stmts)
        if resolver.resolve():
            success_packages.append(pkg)
        else:
            failed_packages.append(pkg)
    for pkg in success_packages:
        logger.info('resolving package %s succeeded', pkg)
    for pkg in failed_packages:
        logger.info('resolving package %s failed', pkg)


def resolve_files(cell_submissions):
    filename_extractor = FilenameExtractTransformer()
    for cell_source in cell_submissions:
        try:
            filename_extractor.visit(ast.parse(cell_source))
        except SyntaxError:
            continue
    return filename_extractor


def main(args, conn):
    curse = conn.cursor()
    cell_submissions = curse.execute(f"""
SELECT source FROM cell_execs
WHERE trace = {args.trace} AND session = {args.session}
ORDER BY counter ASC
    """)
    cell_submissions = list(map(lambda t: t[0], cell_submissions))
    curse.close()

    filename_extractor = resolve_files(cell_submissions)
    if args.just_log_files:
        for fname in filename_extractor.file_names:
            logger.info(fname)
        return 0

    resolve_packages(cell_submissions)
    if args.just_log_imports:
        return 0

    if args.use_nbsafety:
        import nbsafety.safety
        safety = nbsafety.safety.NotebookSafety(cell_magic_name='_NBSAFETY_STATE', skip_unsafe=False)
    else:
        safety = None
    get_ipython().ast_transformers.extend([ExceptionWrapTransformer(), filename_extractor])
    session_had_safety_errors = False
    exec_count = 0
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
        exec_count += 1
        cell_id = get_cell_id_for_source(cell_source)
        logger.info('About to run cell %d (cell counter %d)', cell_id, exec_count)
        try:
            cell_source = black.format_file_contents(cell_source, fast=False, mode=black.FileMode())
        except:  # noqa
            pass
        if safety is None:
            get_ipython().run_cell(cell_source, silent=True)
        else:
            safety.set_active_cell(cell_id)
            get_ipython().run_cell_magic(safety.cell_magic_name, None, cell_source)
            session_had_safety_errors = session_had_safety_errors or safety.test_and_clear_detected_flag()
    if args.use_nbsafety:
        if session_had_safety_errors:
            logger.error('Session had safety errors!')
        else:
            logger.error('No safety errors detected in session.')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--trace', help='Which trace the session to run is in', required=True)
    parser.add_argument('-s', '--session', help='Which session to run', required=True)
    parser.add_argument('--use-nbsafety', '--nbsafety', action='store_true', help='Whether to use nbsafety')
    parser.add_argument('--log-to-stderr', '--stderr', action='store_true', help='Whether to log to stderr')
    parser.add_argument('--just-log-files', action='store_true', help='If true, just log paths of files w/out running')
    parser.add_argument('--just-log-imports', action='store_true', help='If true, just log imports w/out running')
    args = parser.parse_args()
    setup_logging(log_to_stderr=args.log_to_stderr)
    conn = sqlite3.connect('./data/traces.sqlite')
    ret = 0
    try:
        with redirect_std_streams_to('/dev/null'):
            ret = main(args, conn)
    except Exception as e:
        logger.error('Exception occurred in outer context: %s', e)
        ret = 1
    finally:
        conn.close()
        sys.exit(ret)
