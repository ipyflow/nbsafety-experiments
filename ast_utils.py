# -*- coding: utf-8 -*-
import ast
import re
import traceback


PATH_SEP = r'[/\\]'

# usage: .match(s).group(3)
LINUX_PATH_RE = re.compile(r'^{s}?((\w|-|_| |\.)+{s})*((\w|-|_| |\.)+(\.\w\w\w))$'.format(s=PATH_SEP))
# usage: .match(s).group(4)
WINDOWS_PATH_RE = re.compile(r'^(\w:{s}{s}?)?((\w|-|_| |\.)+{s})*((\w|-|_| |\.)+(\.\w\w\w))$'.format(s=PATH_SEP))


def make_matcher(regex, group):
    def _matcher(s):
        match = regex.match(s)
        if match is None:
            return None
        else:
            return match.group(group)
    return _matcher


WINDOWS_MATCHER = make_matcher(WINDOWS_PATH_RE, 4)
LINUX_MATCHER = make_matcher(LINUX_PATH_RE, 3)


class GatherImports(ast.NodeVisitor):
    def __init__(self):
        self.imported_packages = set()
        self.import_stmts = []

    def visit_Import(self, node):
        imported_package_names = set()
        for name in node.names:
            imported_package_names.add(name.name.split('.')[0])
        self.imported_packages |= imported_package_names
        self.import_stmts.append((node, tuple(imported_package_names)))

    def visit_ImportFrom(self, node):
        import_name = node.module.split('.')[0]
        self.import_stmts.append((node, (import_name,)))
        self.imported_packages.add(import_name)


class FilenameExtractTransformer(ast.NodeTransformer):
    def __init__(self):
        self.file_names = set()

    def visit_Str(self, node):
        match = LINUX_MATCHER(node.s)
        if match is not None:
            node.s = match
            self.file_names.add(node.s)
        else:
            match = WINDOWS_MATCHER(node.s)
            if match is not None:
                node.s = match
                self.file_names.add(node.s)
        return node


class ExceptionHandler(object):
    def __init__(self, exc, alias, body):
        if isinstance(exc, str):
            self.exc_name = exc
        elif issubclass(exc, BaseException):
            self.exc_name = exc.__name__
        else:
            raise TypeError('got value %s with invalid type for exc' % exc)
        self.alias = alias
        self.body = body

    def build_ast(self):
        handler = ast.ExceptHandler()
        handler.type = ast.Name(self.exc_name, ctx=ast.Load())
        handler.name = self.alias
        handler.body = ast.parse(self.body).body
        return handler


class ExceptionWrapTransformer(ast.NodeTransformer):
    def __init__(self, handlers=None):
        if handlers is None:
            handlers = []
        self.handlers = handlers

    def visit(self, node):
        try_stmt = ast.Try()
        try_stmt.body = node.body
        default_handler = ExceptionHandler(Exception, 'e', """
logger.error('An exception occurred: %s', e)
logger.warning(traceback.format_exc())
""".strip()).build_ast()
        try_stmt.handlers = self.handlers + [default_handler]
        try_stmt.orelse = []
        try_stmt.finalbody = []
        node.body = [try_stmt]
        return node
