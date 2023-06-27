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
import json
import typing
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Sequence

from feathub.common.exceptions import FeathubException, FeathubExpressionException
from feathub.common.types import (
    DType,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    MapType,
    get_type_by_name,
    from_python_type,
    VectorType,
)
from feathub.dsl.built_in_func import get_builtin_func_def

TYPE_PRECISION_RANK: List[DType] = [Float64, Float32, Int64, Int32]


def _get_higher_precision_type(*dtype: DType) -> Optional[DType]:
    res_type = dtype[0]

    for t in dtype[1:]:
        if res_type not in TYPE_PRECISION_RANK or t not in TYPE_PRECISION_RANK:
            raise FeathubExpressionException(f"Illegal mixing of types: {dtype}")

        if TYPE_PRECISION_RANK.index(res_type) > TYPE_PRECISION_RANK.index(t):
            res_type = t

    return res_type


class ExprAST(ABC):
    def __init__(self, node_type: str) -> None:
        self.node_type = node_type

    @abstractmethod
    def get_children(self) -> Sequence["ExprAST"]:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this node.
        """
        pass

    @abstractmethod
    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        pass

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)


class AbstractUnaryOp(ExprAST, ABC):
    def __init__(self, node_type: str, child: ExprAST):
        super().__init__(node_type)
        self.child = child

    def get_children(self) -> Sequence["ExprAST"]:
        return [self.child]


class AbstractBinaryOp(ExprAST, ABC):
    def __init__(self, node_type: str, left_child: ExprAST, right_child: ExprAST):
        super().__init__(node_type)
        self.left_child = left_child
        self.right_child = right_child

    def get_children(self) -> Sequence["ExprAST"]:
        return [self.left_child, self.right_child]


class BinaryOp(AbstractBinaryOp):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(
            node_type="BinaryOp", left_child=left_child, right_child=right_child
        )
        self.op_type = op_type

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        left_type = self.left_child.eval_dtype(variable_types)
        right_type = self.right_child.eval_dtype(variable_types)

        return _get_higher_precision_type(left_type, right_type)

    def to_json(self) -> Dict:
        return {
            "node_type": "BinaryOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class CompareOp(AbstractBinaryOp):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(
            node_type="CompareOp", left_child=left_child, right_child=right_child
        )
        self.op_type = op_type

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return Bool

    def to_json(self) -> Dict:
        return {
            "node_type": "CompareOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class UminusOp(AbstractUnaryOp):
    def __init__(self, child: ExprAST) -> None:
        super().__init__(node_type="UminusOp", child=child)

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return self.child.eval_dtype(variable_types)

    def to_json(self) -> Dict:
        return {
            "node_type": "UminusOp",
            "child": self.child,
        }


class LogicalOp(AbstractBinaryOp):
    def __init__(self, op_type: str, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(
            node_type="LogicalOp", left_child=left_child, right_child=right_child
        )
        self.op_type = op_type.upper()

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return Bool

    def to_json(self) -> Dict:
        return {
            "node_type": "LogicalOp",
            "op_type": self.op_type,
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class CastOp(AbstractUnaryOp):
    def __init__(
        self, child: ExprAST, type_name: str, exception_on_failure: bool = True
    ):
        super().__init__(node_type="CastOp", child=child)
        self.type_name = type_name
        self.exception_on_failure = exception_on_failure

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return get_type_by_name(self.type_name)

    def to_json(self) -> Dict:
        return {
            "node_type": "CastOp",
            "child": self.child.to_json(),
            "type_name": self.type_name,
            "exception_on_failure": self.exception_on_failure,
        }


class ValueNode(ExprAST):
    def __init__(self, value: Any) -> None:
        super().__init__(node_type="ValueNode")
        self.value = value

    def get_children(self) -> Sequence["ExprAST"]:
        return []

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return from_python_type(type(self.value))

    def to_json(self) -> Dict:
        return {
            "node_type": "ValueNode",
            "value": self.value,
        }


class VariableNode(ExprAST):
    def __init__(self, var_name: str) -> None:
        super().__init__(node_type="VariableNode")
        self.var_name = var_name

    def get_children(self) -> Sequence["ExprAST"]:
        return []

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        if self.var_name not in variable_types:
            raise RuntimeError(f"Type of variable {self.var_name} is not given.")
        return variable_types.get(self.var_name)

    def to_json(self) -> Dict:
        return {
            "node_type": "VariableNode",
            "var_name": self.var_name,
        }


class ArgListNode(ExprAST):
    def __init__(self, values: List[ExprAST]) -> None:
        super().__init__(node_type="ArgListNode")
        self.values = values

    def get_children(self) -> Sequence["ExprAST"]:
        return self.values

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        raise NotImplementedError("This method should not be called.")

    def to_json(self) -> Dict:
        return {
            "node_type": "ArgListNode",
            "values": [value.to_json() for value in self.values],
        }


class FuncCallOp(ExprAST):
    def __init__(self, func_name: str, args: ArgListNode) -> None:
        super().__init__(node_type="FuncCallOp")
        self.func_name = func_name.upper()
        self.args = args

    def get_children(self) -> Sequence["ExprAST"]:
        return [self.args]

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        arg_types = [arg.eval_dtype(variable_types) for arg in self.args.values]
        builtin_func_def = get_builtin_func_def(self.func_name)
        return builtin_func_def.get_result_type(arg_types)

    def to_json(self) -> Dict:
        return {
            "node_type": "FuncCallOp",
            "func_name": self.func_name,
            "args": self.args.to_json(),
        }


class GroupNode(AbstractUnaryOp):
    def __init__(self, child: ExprAST) -> None:
        super().__init__("GroupNode", child=child)

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return self.child.eval_dtype(variable_types)

    def to_json(self) -> Dict:
        return {"node_type": "GroupNode", "child": self.child}


class NullNode(ExprAST):
    def __init__(self) -> None:
        super().__init__(node_type="NullNode")

    def get_children(self) -> Sequence["ExprAST"]:
        return []

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        raise NotImplementedError("This method should not be called.")

    def to_json(self) -> Dict:
        return {"node_type": "NullNode"}


class IsOp(AbstractBinaryOp):
    def __init__(
        self, left_child: ExprAST, right_child: ExprAST, is_not: bool = False
    ) -> None:
        super().__init__(
            node_type="IsOp", left_child=left_child, right_child=right_child
        )
        if not isinstance(right_child, NullNode):
            raise FeathubException("IS/IS NOT can only be concatenated with NULL.")
        self.is_not = is_not

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        return Bool

    def to_json(self) -> Dict:
        return {
            "node_type": "IsOp",
            "left_child": self.left_child.to_json(),
            "is_not": self.is_not,
        }


class GetItemOp(AbstractBinaryOp):
    def __init__(self, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(
            node_type="GetItemOp", left_child=left_child, right_child=right_child
        )

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        collection_type = self.left_child.eval_dtype(variable_types)
        key_type = self.right_child.eval_dtype(variable_types)

        if isinstance(collection_type, MapType):
            if key_type != collection_type.key_dtype:
                raise FeathubExpressionException(
                    f"Map key type {collection_type.key_dtype} does not match "
                    f"with expected {key_type}."
                )
            return collection_type.value_dtype

        # TODO: Support parsing expression based on data types.
        if isinstance(collection_type, VectorType):
            # Suppose parsing an expression "a[b]" into Flink SQL. If a is a map, it
            # should be parsed into "a[b]". If a is a list, it should be parsed into
            # "a[b + 1]". The parse result depends on the data type, but Feathub
            # has not uniform AbstractAstEvaluator#eval and ExprAST#eval_dtype, so
            # the parsing process cannot get type information yet.
            raise FeathubException(
                "Getting element from list by index is not supported yet."
            )

        raise FeathubExpressionException(f"{key_type} is not subscriptable.")

    def to_json(self) -> Dict:
        return {
            "node_type": "GetItemOp",
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class GetAttrOp(AbstractBinaryOp):
    def __init__(self, left_child: ExprAST, right_child: ExprAST) -> None:
        super().__init__(
            node_type="GetAttrOp", left_child=left_child, right_child=right_child
        )
        if not isinstance(left_child, VariableNode):
            raise FeathubExpressionException(
                f"The left child of GetAttrOp should be VariableNode instead "
                f"of {left_child}"
            )

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        table_name = typing.cast(VariableNode, self.left_child).var_name
        prefix_index = len(table_name) + 1
        variable_types = {
            key[prefix_index:]: value
            for key, value in variable_types.items()
            if key.startswith(table_name)
        }
        return self.right_child.eval_dtype(variable_types)

    def to_json(self) -> Dict:
        return {
            "node_type": "GetAttrOp",
            "left_child": self.left_child.to_json(),
            "right_child": self.right_child.to_json(),
        }


class CaseOp(ExprAST):
    def __init__(
        self,
        conditions: List[ExprAST],
        results: List[ExprAST],
        default: ExprAST,
    ) -> None:
        super().__init__(node_type="CaseOp")

        if len(conditions) != len(results):
            raise FeathubException(
                "The number of conditions and results does not match."
            )

        if not conditions:
            raise FeathubException("Cannot create CaseOp without cases.")

        self.conditions = conditions
        self.results = results
        self.default = default

    def get_children(self) -> Sequence["ExprAST"]:
        return self.conditions + self.results + [self.default]

    def eval_dtype(self, variable_types: Dict[str, DType]) -> DType:
        result_types = set(
            [result_expr.eval_dtype(variable_types) for result_expr in self.results]
        )

        if not isinstance(self.default, NullNode):
            result_types.add(self.default.eval_dtype(variable_types))

        if len(result_types) == 1:
            return result_types.pop()

        return _get_higher_precision_type(*result_types)

    def to_json(self) -> Dict:
        return {
            "node_type": "CaseOp",
            "conditions": [x.to_json() for x in self.conditions],
            "results": [x.to_json() for x in self.results],
            "default": self.default.to_json(),
        }

    @staticmethod
    def new_builder() -> "CaseOp.Builder":
        return CaseOp.Builder()

    class Builder:
        def __init__(self) -> None:
            self.conditions: List[ExprAST] = []
            self.results: List[ExprAST] = []
            self.default_value: ExprAST = NullNode()

        def case(self, condition: ExprAST, result: ExprAST) -> "CaseOp.Builder":
            self.conditions.append(condition)
            self.results.append(result)
            return self

        def default(self, result: ExprAST) -> "CaseOp.Builder":
            self.default_value = result
            return self

        def build(self) -> "CaseOp":
            return CaseOp(self.conditions, self.results, self.default_value)
