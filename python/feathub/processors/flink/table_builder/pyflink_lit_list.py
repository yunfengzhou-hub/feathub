from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import (
    expressions as native_flink_expr,
    StreamTableEnvironment,
)
from pyflink.table.types import DataTypes

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    table = t_env.from_elements([(1, ), (2, ), (3, )])
    # table = table.add_or_replace_columns(
    #     native_flink_expr.lit([], DataTypes.ARRAY(DataTypes.INT()).not_null())
    # )
    table = table.add_or_replace_columns(
        native_flink_expr.lit(get_gateway().new_array(get_gateway().jvm.java.lang.Integer, 0))
    )
    table.execute().print()
