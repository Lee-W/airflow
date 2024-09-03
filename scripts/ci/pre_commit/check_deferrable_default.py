#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import ast
import glob
import itertools
import os
import sys
from typing import Iterator

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))

DEFERRABLE_DOC = (
    "https://github.com/apache/airflow/blob/main/docs/apache-airflow/"
    "authoring-and-scheduling/deferring.rst#writing-deferrable-operators"
)


def _is_valid_deferrable_default(default: ast.AST) -> bool:
    """Check whether default is 'conf.getboolean("operators", "default_deferrable", fallback=False)'"""
    return ast.unparse(default) == "conf.getboolean('operators', 'default_deferrable', fallback=False)"


class DefaultDeferrableTransformer(ast.NodeTransformer):
    EXPECTED_DEFAULT = "conf.getboolean('operators', 'default_deferrable', fallback=False)"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, *kwargs)
        self.error_linenos: list[int] = []
        self.error_found: bool = False
        self.fixed: bool = False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        if node.name == "__init__":
            args = node.args
            arguments = reversed([*args.args, *args.posonlyargs, *args.kwonlyargs])
            defaults = reversed([*args.defaults, *args.kw_defaults])
            error_arg_index = -1
            for arg_index, (argument, default) in enumerate(itertools.zip_longest(arguments, defaults)):
                # argument is not deferrable
                if argument is None or argument.arg != "deferrable":
                    continue

                # argument is deferrable, but comes with no default value
                if default is None:
                    self.error_linenos.append(argument.lineno)
                    error_arg_index = arg_index
                    continue

                # argument is deferrable, but the default value is not valid
                if not _is_valid_deferrable_default(default):
                    self.error_linenos.append(default.lineno)
                    error_arg_index = arg_index

            if error_arg_index != -1:
                kw_defaults_len = len(args.kw_defaults)
                defaults_len = len(args.defaults)
                # not fixable, there's arg with no default value after argument "deferrable"
                if error_arg_index > (defaults_len + kw_defaults_len):
                    return node

                expected_default_ast = ast.parse(
                    r"conf.getboolean('operators', 'default_deferrable', fallback=False)", mode="eval"
                ).body

                if error_arg_index > kw_defaults_len - 1:
                    # argument "deferrable" not in keyword only arguments
                    new_index = -(error_arg_index - kw_defaults_len + 1)
                    node.args.defaults[-(1 + new_index)] = expected_default_ast
                else:
                    # argument "deferrable" in keyword only arguments
                    node.args.kw_defaults[-(1 + error_arg_index)] = expected_default_ast

                self.fixed = True
                return node
        return node


def iter_check_deferrable_default_errors(module_filename: str) -> Iterator[str]:
    ast_tree = ast.parse(open(module_filename).read())
    transformer = DefaultDeferrableTransformer()
    modified_tree = ast.fix_missing_locations(transformer.visit(ast_tree))
    if transformer.fixed:
        with open(module_filename, "w") as writer:
            writer.write(ast.unparse(modified_tree))

    yield from (f"{module_filename}:{lineno}" for lineno in transformer.error_linenos)


def main() -> int:
    modules = itertools.chain(
        glob.glob(f"{ROOT_DIR}/airflow/**/sensors/**.py", recursive=True),
        glob.glob(f"{ROOT_DIR}/airflow/**/operators/**.py", recursive=True),
    )

    errors = [error for module in modules for error in iter_check_deferrable_default_errors(module)]
    if errors:
        print("Incorrect deferrable default values detected at:")
        for error in errors:
            print(f"  {error}")
        print(
            """Please set the default value of deferrbale to """
            """"conf.getboolean("operators", "default_deferrable", fallback=False)"\n"""
            f"See: {DEFERRABLE_DOC}\n"
        )

    return len(errors)


if __name__ == "__main__":
    sys.exit(main())
