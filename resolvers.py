# -*- coding: utf-8 -*-
import ast
import logging
import kaggle
import pathlib
import pickle
import shutil
import subprocess

logger = logging.getLogger(__name__)

PACKAGES_BY_IMPORT = {
    'sklearn': {
        'package': 'scikit-learn',
        'versions': ['0.21.3', '0.20.4', '0.19.2']
    },
    'skimage': {
        'package': 'scikit-image',
    }
}


class UnableToResolveError(Exception):
    pass


class Resolver(object):
    def resolve(self):
        return NotImplemented


class ImportResolver(Resolver):
    def __init__(self, libname, imports_involving_lib):
        self.libname = libname
        self.imports_involving_lib = imports_involving_lib


class PipResolver(ImportResolver):

    # huge hack using pickle to get around not having the actual text source code
    def _try_imports(self):
        unique_imports = set()
        for import_stmt in self.imports_involving_lib:
            mod = ast.Module([import_stmt], [])
            unique_imports.add(pickle.dumps(mod))
        logger.info('total unique imports: %d vs %d non-dedupped', len(unique_imports), len(self.imports_involving_lib))
        pickled_import_dir = pathlib.Path('pickled_imports')
        try:
            shutil.rmtree(pickled_import_dir)
        except:
            pass
        pickled_import_dir.mkdir(exist_ok=True)
        for idx, import_dump in enumerate(unique_imports):
            with open(pickled_import_dir.joinpath(f'pickled.{idx}.dump'), 'wb') as f:
                f.write(import_dump)
        try:
            with open('/dev/null', 'w') as devnull:
                total_failing = subprocess.call(
                    'python try-imports.py', shell=True, stdout=devnull, stderr=subprocess.STDOUT
                )
                logger.info('total failing: %d', total_failing)
        finally:
            shutil.rmtree(pickled_import_dir)
        return total_failing

    def resolve(self):
        if self.libname == 'itertools':
            return True

        if self._try_imports() == 0:
            return True

        def _version_tuple(vstr):
            return tuple(map(int, vstr.split('.')))

        best = (-float('inf'), None, None)
        package = PACKAGES_BY_IMPORT.get(self.libname, {'package': self.libname})
        pypi_package = package['package']
        with open('/dev/null', 'w') as devnull:
            if 'versions' in package:
                for v in package['versions']:
                    try:
                        subprocess.check_call(
                            f'pip install {pypi_package}=={v}',
                            shell=True, stdout=devnull, stderr=subprocess.STDOUT
                        )
                    except:
                        continue
                    best = max(best, (-self._try_imports(), _version_tuple(v), v))
                    if best[0] == 0:  # short-circuit if we find one that fixes all imports
                        return True
                if best[2] is None:
                    logger.error('error: unable to find working package for %s', package)
                    return False
                subprocess.check_call(
                    f'pip install {pypi_package}=={best[2]}',
                    shell=True, stdout=devnull, stderr=subprocess.STDOUT
                )
                logger.warning('warning: %d import(s) still failing', -best[0])
            else:
                subprocess.check_call(
                    f'pip install --upgrade {pypi_package}',
                    shell=True, stdout=devnull, stderr=subprocess.STDOUT
                )
                return self._try_imports() == 0

        return False


class FileResolver(Resolver):
    def __init__(self, file_name):
        self.file_name = file_name


class KaggleResolver(FileResolver):
    # kaggle.api.dataset_list(search='titanic')
    # kaggle.api.dataset_download_files('ibooth1/titanic3')
    pass
