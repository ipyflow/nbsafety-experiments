# -*- coding: utf-8 -*-
import ast
import logging
import kaggle
import pickle
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
        total_failing = 0
        with open('/dev/null', 'w') as devnull:
            for import_stmt in self.imports_involving_lib:
                mod = ast.Module()
                mod.body = [import_stmt]
                total_failing += (subprocess.call(f"""
python -c "import pickle; eval(compile(pickle.loads({pickle.dumps(mod)}), filename='', mode='exec'))"
""".strip(), shell=True, stdout=devnull, stderr=subprocess.STDOUT) != 0)
        return total_failing

    def resolve(self):
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
