#!/usr/bin/env ipython3
# -*- coding: utf-8 -*-
import argparse
import ast
import black
import collections
import contextlib
import logging
import numpy
import numpy as np
import os
import shutil
import subprocess
import sqlite3
import sys

from IPython import get_ipython

try:
    from cfuzzyset import cFuzzySet as FuzzySet
except ImportError:
    from fuzzyset import FuzzySet

from ast_utils import FilenameExtractTransformer, GatherImports
from replay_stats_group import ReplayStatsGroup
from resolvers import PipResolver
from timeout import timeout

logger = logging.getLogger(__name__)

CELL_ID_BY_SOURCE = {}
MATCHING_CELL_THRESHOLD = 0.8
EXECUTED_CELLS = FuzzySet()


@timeout(15)
def timeout_run_cell(cell_id, cell_source, safety=None):
    if safety is None:
        get_ipython().run_cell(cell_source, silent=True)
        return False
    else:
        safety.set_active_cell(cell_id)
        get_ipython().run_cell_magic(safety.cell_magic_name, None, cell_source)
        return safety.test_and_clear_detected_flag()


def setup_logging(log_to_stderr=True):
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    info_handler = logging.FileHandler('session.info.log', mode='w')
    info_handler.setLevel(logging.INFO)
    warning_handler = logging.FileHandler('session.warnings.log', mode='w')
    warning_handler.setLevel(logging.WARNING)
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

    def _counter(increment=True):
        nonlocal current
        ret = current
        if increment:
            current += 1
        return ret
    return _counter


get_new_cell_id = make_cell_counter()


os_path_join = os.path.join


def my_path_joiner(a, *p):
    p = tuple(p)
    if len(p) > 0:
        fname = p[-1].split('/')[-1]
    else:
        fname = a.split('/')[-1]
    return os_path_join('data', 'transient', fname)


np_load = np.load
np_save = np.save
np_savez = np.savez


def my_np_load(fname, *args, **kwargs):
    if 'data/transient' not in fname:
        fname = os_path_join('data', 'transient', fname)
    return np_load(fname, *args, **kwargs)


def my_np_save(fname, *args, **kwargs):
    if 'data/transient' not in fname:
        fname = os_path_join('data', 'transient', fname)
    return np_save(fname, *args, **kwargs)


def my_np_savez(fname, *args, **kwargs):
    if 'data/transient' not in fname:
        fname = os_path_join('data', 'transient', fname)
    return np_savez(fname, *args, **kwargs)


np.load = my_np_load
numpy.load = my_np_load
np.save = my_np_save
numpy.save = my_np_save
np.savez = my_np_savez
numpy.savez = my_np_savez


def input(*args, **kwargs):
    pass


def raw_input(*args, **kwargs):
    pass


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


# these are accessed in ipython context and so need to be defined here
num_exceptions = 0
exception_counts = collections.Counter()
should_test_prediction = True


def main(args, conn):
    global num_exceptions
    global should_test_prediction
    curse = conn.cursor()
    cell_submissions = curse.execute(f"""
SELECT source FROM cell_execs
WHERE trace = {args.trace} AND session = {args.session}
ORDER BY counter ASC
    """)
    cell_submissions = list(map(lambda t: t[0], cell_submissions))
    curse.close()

    with open('session.py', 'w') as f:
        for idx, cell in enumerate(cell_submissions):
            f.write(f'# + Cell {idx + 1}\n')
            f.write(cell)
            f.write('\n\n')

    with open('/dev/null', 'w') as devnull:
        subprocess.call('2to3 session.py -w', shell=True, stdout=devnull, stderr=subprocess.STDOUT)

    if args.write_session_file:
        shutil.copy('session.py', f'session.{args.trace}.{args.session}.py')

    with open('session.py') as f:
        cell_submissions = f.read().split('# + Cell ')
        cell_submissions = map(lambda cell: cell.strip(), cell_submissions)
        cell_submissions = filter(lambda cell: len(cell) > 0, cell_submissions)
        cell_submissions = map(lambda cell: '# + Cell ' + cell, cell_submissions)
        cell_submissions = list(cell_submissions)

    filename_extractor = resolve_files(cell_submissions)
    if args.just_log_files:
        for fname in filename_extractor.file_names:
            logger.info(fname)
        return 0

    resolve_packages(cell_submissions)
    if args.just_log_imports:
        return 0

    live_stats = ReplayStatsGroup('live_cells')
    new_live_stats = ReplayStatsGroup('new_live_cells')
    new_or_refresher_stats = ReplayStatsGroup('new_or_refresher_cells')
    refresher_stats = ReplayStatsGroup('refresher_cells')
    stale_stats = ReplayStatsGroup('stale_cells')
    all_stats_groups = [live_stats, new_live_stats, new_or_refresher_stats, refresher_stats, stale_stats]

    prev_cell_id = None
    live_cells = None
    stale_cells = None
    refresher_cells = None
    prev_live_cells = set()

    get_ipython().run_line_magic('matplotlib', 'inline')
    get_ipython().run_cell('import numpy as np', silent=True)
    get_ipython().run_cell('import pandas as pd', silent=True)
    if args.use_nbsafety:
        import nbsafety.safety
        safety = nbsafety.safety.NotebookSafety(cell_magic_name='_NBSAFETY_STATE', skip_unsafe=False)
        # get_ipython().run_line_magic('safety', 'trace_messages enable')
    else:
        safety = None
    # get_ipython().ast_transformers.extend([ExceptionWrapTransformer(), filename_extractor])
    get_ipython().ast_transformers.extend([filename_extractor])
    num_safety_errors = 0
    exec_count_orig = 0
    exec_count_replay = 0
    exec_count_replay_successes = 0
    notebook_state = {}
    for cell_source in cell_submissions:
        lines = cell_source.split('\n')
        new_lines = []
        exec_count_orig += 1
        for line in lines:
            if line.startswith('get_ipython()'):
                if 'pylab' not in line and ('time' not in line or 'timedelta' in line):
                    continue
            new_lines.append('    ' + line)
        cell_source = '\n'.join(new_lines)
        if cell_source.strip() == '':
            continue
        cell_id = get_cell_id_for_source(cell_source)
        cell_source = f"""
try:
{cell_source}
except Exception as e:
    exception_counts[e.__class__.__name__] += 1
    num_exceptions += 1
    should_test_prediction = False
    import traceback
    logger.error('An exception occurred: %s', e)
    logger.error('%s', e.__class__.__name__)
    logger.warning(traceback.format_exc())""".strip()
        logger.info('About to run cell %d (cell counter %d)', cell_id, exec_count_orig)
        try:
            cell_source = black.format_file_contents(cell_source, fast=False, mode=black.FileMode())
        except:  # noqa
            pass

        if 'os.path.join' in cell_source and 'IMDb' not in cell_source:
            os.path.join = my_path_joiner
        this_cell_had_safety_errors = False
        should_test_prediction = True
        try:
            exec_count_replay += 1
            this_cell_had_safety_errors = timeout_run_cell(cell_id, cell_source, safety=safety)
            num_safety_errors += this_cell_had_safety_errors
        except Exception as outer_e:
            exception_counts[outer_e.__class__.__name__] += 1
            num_exceptions += 1
            should_test_prediction = False
        finally:
            exec_count_replay_successes += should_test_prediction
            os.path.join = os_path_join

        if safety is not None and prev_cell_id is not None and cell_id != prev_cell_id and cell_id in notebook_state:
            if should_test_prediction and not this_cell_had_safety_errors:
                assert live_cells is not None
                assert stale_cells is not None
                assert refresher_cells is not None

                num_available_cells = len(notebook_state)
                live_stats.update(cell_id, live_cells, num_available_cells)
                new_live_cells = live_cells - prev_live_cells
                new_live_stats.update(cell_id, new_live_cells, num_available_cells)
                refresher_stats.update(cell_id, refresher_cells, num_available_cells)
                new_or_refresher_stats.update(cell_id, refresher_cells | new_live_cells, num_available_cells)
                stale_stats.update(cell_id, stale_cells, num_available_cells)

        prev_live_cells = live_cells
        assert cell_id is not None
        notebook_state[cell_id] = cell_source
        if safety is not None:
            precheck = safety.multicell_precheck(notebook_state)
            live_cells = set(precheck['stale_output_cells'])
            stale_cells = set(precheck['stale_input_cells'])
            refresher_cells = set(precheck['refresher_links'].keys())
        prev_cell_id = cell_id


    if num_safety_errors > 0:
        logger.error('Session had %d safety errors!', num_safety_errors)
    else:
        logger.error('No safety errors detected in session.')

    if args.no_stats_logging:
        return 0

    upsert_row = dict(
        version=args.version,
        trace=args.trace,
        session=args.session,
        num_cell_execs=exec_count_replay,
        num_successful_cell_execs=exec_count_replay_successes,
        num_cells_created=get_new_cell_id(increment=False),
        num_exceptions=num_exceptions,
        num_safety_errors=num_safety_errors
    )
    for stats_group in all_stats_groups:
        upsert_row.update(stats_group.make_dict())
    curse = conn.cursor()
    try:
        sql = f"""
        INSERT OR REPLACE INTO replay_stats({','.join(upsert_row.keys())})
        VALUES ({','.join(repr(v) for v in upsert_row.values())})
        """
        logger.warning(sql)
        curse.execute(sql)
        sql = f'DELETE FROM replay_exception_stats WHERE trace={args.trace} AND session={args.session}'
        logger.warning(sql)
        curse.execute(sql)
        for exc_name, exc_count in exception_counts.items():
            upsert_row = dict(
                trace=args.trace,
                session=args.session,
                exception=exc_name,
                count=exc_count
            )
            sql = f"""
            INSERT INTO replay_exception_stats({','.join(upsert_row.keys())})
            VALUES ({','.join(repr(v) for v in upsert_row.values())})
            """
            logger.warning(sql)
            curse.execute(sql)
    finally:
        curse.close()

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', type=int, default=-1)
    parser.add_argument('-t', '--trace', type=int, help='Which trace the session to run is in', required=True)
    parser.add_argument('-s', '--session', type=int, help='Which session to run', required=True)
    parser.add_argument('--use-nbsafety', '--nbsafety', action='store_true', help='Whether to use nbsafety')
    parser.add_argument('--log-to-stderr', '--stderr', action='store_true', help='Whether to log to stderr')
    parser.add_argument('--just-log-files', action='store_true', help='If true, just log paths of files w/out running')
    parser.add_argument('--just-log-imports', action='store_true', help='If true, just log imports w/out running')
    parser.add_argument('--write-session-file', action='store_true', help='If write session to session.py')
    parser.add_argument('--no-stats-logging', action='store_true', help='No writing to db tables if true')
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
        conn.commit()
        conn.close()
        sys.exit(ret)
