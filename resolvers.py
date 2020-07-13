# -*- coding: utf-8 -*-
import ast
import logging
import subprocess

logger = logging.getLogger(__name__)

PACKAGES_BY_IMPORT = {
    'sklearn': [
        {
            'package': 'scikit-learn',
            'versions': ['0.21.3', '0.20.4', '0.19.2', '0.18.2']
        }
    ]
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
        for import_stmt in self.failing_imports:
            mod = ast.Module()
            mod.body = [import_stmt]
            try:
                eval(compile(mod, filename='', mode='exec'))
            except (ImportError, ModuleNotFoundError):
                return False
            except Exception as e:
                logger.error('Unexpected exception: %s', e)
                return False
            return True

    def resolve(self):
        with open('/dev/null', 'w') as devnull:
            for package in PACKAGES_BY_IMPORT.get(self.libname, [{'package': self.libname}]):
                if self._try_imports():
                    return True
                pypi_package = package['package']
                if 'versions' in package:
                    for v in package['versions']:
                        if self._try_imports():
                            return True
                        subprocess.check_call(
                            f'pip install --force {pypi_package}=={v}',
                            shell=True, stdout=devnull, stderr=subprocess.STDOUT
                        )
                else:
                    subprocess.check_call(
                        f'pip install --force {pypi_package}',
                        shell=True, stdout=devnull, stderr=subprocess.STDOUT
                    )
        return self._try_imports()


class FileResolver(Resolver):
    def __init__(self, file_name):
        self.file_name = file_name


class KaggleResolver(FileResolver):
    pass
