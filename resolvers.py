# -*- coding: utf-8 -*-
import ast
import logging
import subprocess

logger = logging.getLogger(__name__)

PACKAGES_BY_IMPORT = {
    'sklearn': {
        'package': 'scikit-learn',
        'versions': ['0.21.3', '0.20.4', '0.19.2']
    }
}


class UnableToResolveError(Exception):
    pass


class Resolver(object):
    def resolve(self):
        return NotImplemented


class ImportResolver(Resolver):
    def __init__(self, libname, failing_imports):
        self.libname = libname
        self.failing_imports = failing_imports


class PipResolver(ImportResolver):

    def _try_imports(self):
        total_failing = 0
        for import_stmt in self.failing_imports:
            mod = ast.Module()
            mod.body = [import_stmt]
            try:
                eval(compile(mod, filename='', mode='exec'))
            except (ImportError, ModuleNotFoundError):
                total_failing += 1
            except Exception as e:
                logger.error('Unexpected exception: %s', e)
                total_failing += 1
        return total_failing

    def resolve(self):
        if self._try_imports() == 0:
            return True
        best = (float('inf'), None)
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
                    best = min(best, (self._try_imports(), v))
                    if best[0] == 0:  # short-circuit if we find one that fixes all imports
                        return True
                if best[1] is None:
                    logger.error('error: unable to find working package for %s', package)
                    return False
                subprocess.check_call(
                    f'pip install {pypi_package}=={best[1]}',
                    shell=True, stdout=devnull, stderr=subprocess.STDOUT
                )
                logger.warning('warning: %d import(s) still failing', best[0])
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
    pass
