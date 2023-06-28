#  Copyright 2022 The FeatHub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import re
from typing import Set, Optional, cast

from ply import lex

from feathub.common.exceptions import FeathubException
from feathub.dsl.ast import ExprAST, GetAttrOp, VariableNode

from feathub.dsl.built_in_func import BUILTIN_FUNC_DEF_MAP
from feathub.dsl.expr_lexer_rules import ExprLexerRules, ID_REGEX

lexer = lex.lex(module=ExprLexerRules())


def get_variables(feathub_expr: str) -> Set[str]:
    """
    Get the variables in the given Feathub expression.
    """
    lexer.input(feathub_expr)
    variables = set()
    while True:
        tok = lexer.token()
        if not tok:
            break
        if tok.type == "ID" and tok.value not in BUILTIN_FUNC_DEF_MAP:
            variables.add(tok.value)
    return variables


def is_id(expr: str) -> bool:
    """
    Returns whether the feature expression is a single ID.
    """
    return bool(re.match(ID_REGEX, expr))


def get_join_table_name(ast: ExprAST) -> Optional[str]:
    """
    Returns the name of the table to be joined with if the ExprAST
    refers to features in another table.
    """
    table_name = None
    if isinstance(ast, GetAttrOp):
        table_name = cast(ast.left_child, VariableNode).var_name

    for child in ast.get_children():
        tmp_table_name = get_join_table_name(child)
        if tmp_table_name is None:
            continue
        elif table_name is None:
            table_name = tmp_table_name
        elif table_name != tmp_table_name:
            raise FeathubException(
                f"Variables to be joined should be from at most one other table, "
                f"while at least two tables are found: {table_name} and "
                f"{tmp_table_name}"
            )

    return table_name
