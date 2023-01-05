#  Copyright 2022 The Feathub Authors
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
import logging

from feathub.dsl.expr_parser import ExprParser
from feathub.processors.spark.ast_evaluator.spark_ast_evaluator import SparkAstEvaluator

_parser = ExprParser()
_ast_evaluator = SparkAstEvaluator()

logger = logging.getLogger(__file__)


def to_spark_sql_expr(feathub_expr: str) -> str:
    logger.debug(f"Parsing Feathub expr: {feathub_expr}")
    ast = _parser.parse(feathub_expr)
    spark_sql_expr = _ast_evaluator.eval(ast, {})
    logger.debug(f"Result Spark Sql expr: {spark_sql_expr}")
    return spark_sql_expr
