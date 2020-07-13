# -*- coding: utf-8 -*-
import ast
import re


LINUX_PATH_RE = re.compile(r'^/?((\w|-|_| |\.)+/)*((\w|-|_| |\.)+(\.\w\w\w))$')  # usage: .match(s).group(3)
WINDOWS_PATH_RE = re.compile(r'^(\w:\\\\)?((\w|-|_| |\.)+\\)*((\w|-|_| |\.)+(\.\w\w\w))$')  # usage: .match(s).group(4)


class GatherImports(ast.NodeVisitor):
    def __init__(self):
        self.imported_packages = set()
        self.import_stmts = []

    def visit_Import(self, node):
        self.import_stmts.append(node)
        for name in node.names:
            self.imported_packages.add(name.name.split('.')[0])

    def visit_ImportFrom(self, node):
        self.import_stmts.append(node)
        self.imported_packages.add(node.module.split('.')[0])


class FilenameExtractTransformer(ast.NodeTransformer):
    def __init__(self):
        self.file_names = set()

    def visit_Str(self, node):
        match = LINUX_PATH_RE.match(node.s)
        if match is not None:
            node.s = match.group(3)
            self.file_names.add(node.s)
        else:
            match = WINDOWS_PATH_RE.match(node.s)
            if match is not None:
                node.s = match.group(4)
                self.file_names.add(node.s)
        return node


class ExceptionWrapTransformer(ast.NodeTransformer):
    def visit(self, node):
        try_stmt = ast.Try()
        try_stmt.body = node.body
        handler = ast.ExceptHandler()
        handler.name = 'e'
        handler.type = ast.Name('Exception', ctx=ast.Load())
        handler.body = []
        handler.body.append(ast.parse("logger.error('An exception occurred: %s', e)").body[0])
        handler.body.append(ast.parse("logger.warning(traceback.format_exc())").body[0])
        try_stmt.handlers = [handler]
        try_stmt.orelse = []
        try_stmt.finalbody = []
        node.body = [try_stmt]
        return node
