# -*- coding: utf-8 -*-
import ast
import re


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
