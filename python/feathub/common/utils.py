# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
from datetime import datetime, timezone
from string import Template
from typing import Union, Any, Optional, cast

from feathub.common import types
from feathub.common.protobuf import featureValue_pb2


def to_java_date_format(python_format: str) -> str:
    """
    :param python_format: A datetime format string accepted by datetime::strptime().
    :return: A datetime format string accepted by  java.text.SimpleDateFormat.
    """

    # TODO: Currently cannot handle case such as "%Y-%m-%dT%H:%M:%S", which should be
    #  converted to "yyyy-MM-dd'T'HH:mm:ss".
    mapping = {
        "Y": "yyyy",
        "m": "MM",
        "d": "dd",
        "H": "HH",
        "M": "mm",
        "S": "ss",
        "f": "SSS",
        "z": "X",
    }
    return Template(python_format.replace("%", "$")).substitute(**mapping)


def to_unix_timestamp(
    time: Union[datetime, str], format: str = "%Y-%m-%d %H:%M:%S"
) -> float:
    """
    Returns POSIX timestamp corresponding to date_string, parsed according to format.
    Uses UTC timezone if it is not explicitly specified in the given date.
    """
    if isinstance(time, str):
        time = datetime.strptime(time, format)
    if time.tzinfo is None:
        time = time.replace(tzinfo=timezone.utc)
    return time.timestamp()


def append_and_sort_unix_time_column(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: str
) -> str:
    unix_time_column = "_unix_time"

    if unix_time_column in df:
        raise RuntimeError(f"The dataframe has column with name {unix_time_column}.")

    df[unix_time_column] = df.apply(
        lambda row: to_unix_timestamp(row[timestamp_field], timestamp_format),
        axis=1,
    )
    df.sort_values(
        by=[unix_time_column],
        ascending=True,
        inplace=True,
        ignore_index=True,
    )

    return unix_time_column


def serialize_object_with_protobuf(
    input_object: Optional[Any], value_type: types.DType
) -> Optional[Any]:
    """
    Serializes an object into byte array with protobuf.

    :param input_object: The object to be serialized.
    :param value_type: The type of the object.
    """
    if input_object is None:
        return None

    pb_value = featureValue_pb2.FeatureValue()
    if value_type == types.Bytes:
        pb_value.bytes_value = input_object
    elif value_type == types.String:
        pb_value.string_value = input_object
    elif value_type == types.Bool:
        pb_value.boolean_value = input_object
    elif value_type == types.Int32:
        pb_value.int_value = input_object
    elif value_type == types.Int64:
        pb_value.long_value = input_object
    elif value_type == types.Float32:
        pb_value.float_value = input_object
    elif value_type == types.Float64:
        pb_value.double_value = input_object
    elif value_type == types.Timestamp:
        timestamp_object = cast(datetime, input_object)
        pb_value.double_value = to_unix_timestamp(timestamp_object, "")
    elif isinstance(value_type, types.MapType):
        map_type: types.MapType = cast(types.MapType, value_type)
        map_object: dict = cast(dict, input_object)
        for key, value in map_object.items():
            pb_value.bytes_array_value.value.append(
                serialize_object_with_protobuf(key, map_type.key_dtype)
            )
            pb_value.bytes_array_value.value.append(
                serialize_object_with_protobuf(value, map_type.value_dtype)
            )
    elif isinstance(value_type, types.VectorType):
        vector_type: types.VectorType = cast(types.VectorType, value_type)
        vector_object: list = cast(list, input_object)
        for element_object in vector_object:
            pb_value.bytes_array_value.value.append(
                serialize_object_with_protobuf(element_object, vector_type.dtype)
            )
    else:
        raise TypeError(f"Unsupported data type {value_type}")
    return pb_value.SerializeToString()


def deserialize_protobuf_object(
    pb_byte_array: Optional[bytes], value_type: types.DType
) -> Optional[Any]:
    """
    Deserializes an object from byte array with protobuf.

    :param pb_byte_array: The protobuf byte array to be deserialized.
    :param value_type: The type of the object.
    """
    if pb_byte_array is None:
        return None

    pb_value = featureValue_pb2.FeatureValue()
    pb_value.ParseFromString(pb_byte_array)
    if value_type == types.Bytes:
        return pb_value.bytes_value
    elif value_type == types.String:
        return pb_value.string_value
    elif value_type == types.Bool:
        return pb_value.boolean_value
    elif value_type == types.Int32:
        return pb_value.int_value
    elif value_type == types.Int64:
        return pb_value.long_value
    elif value_type == types.Float32:
        return pb_value.float_value
    elif value_type == types.Float64:
        return pb_value.double_value
    elif value_type == types.Timestamp:
        return datetime.utcfromtimestamp(pb_value.double_value)
    elif isinstance(value_type, types.MapType):
        map_type: types.MapType = cast(types.MapType, value_type)
        raw_value = pb_value.bytes_array_value.value
        map_object = dict()
        for i in range(0, len(raw_value), 2):
            key_object = deserialize_protobuf_object(raw_value[i], map_type.key_dtype)
            value_object = deserialize_protobuf_object(
                raw_value[i + 1], map_type.value_dtype
            )
            map_object[key_object] = value_object
        return map_object
    elif isinstance(value_type, types.VectorType):
        vector_type: types.VectorType = cast(types.VectorType, value_type)
        raw_value = pb_value.bytes_array_value.value
        vector_object = list()
        for i in range(len(raw_value)):
            element_object = deserialize_protobuf_object(
                raw_value[i], vector_type.dtype
            )
            vector_object.append(element_object)
        return vector_object
    else:
        raise TypeError(f"Unsupported data type {value_type}")
