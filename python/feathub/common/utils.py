# Copyright 2022 The FeatHub Authors
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
import sys
from datetime import datetime, timezone, tzinfo
from string import Template
from typing import Union, Dict, Any, Type, TYPE_CHECKING
from urllib.parse import urlparse

from feathub.common.exceptions import FeathubException

if TYPE_CHECKING:
    from feathub.table.schema import Schema
    from feathub.table.table_descriptor import TableDescriptor


def to_java_date_format(python_format: str) -> str:
    """
    :param python_format: A datetime format string accepted by datetime::strptime().
    :return: A datetime format string accepted by java.text.SimpleDateFormat.
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
    time: Union[int, datetime, str],
    format: str = "%Y-%m-%d %H:%M:%S",
    tz: tzinfo = timezone.utc,
) -> float:
    """
    Returns POSIX timestamp corresponding to date_string, parsed according to format.
    Uses the timezone specified in tz if it is not explicitly specified in the given
    date.
    """
    if isinstance(time, str):
        time = datetime.strptime(time, format)
    elif isinstance(time, int):
        if format == "epoch":
            time = datetime.fromtimestamp(time, tz=tz)
        elif format == "epoch_millis":
            time = datetime.fromtimestamp(time / 1000, tz=tz)
        else:
            raise FeathubException(
                f"Unknown type {type(time)} of timestamp with timestamp "
                f"format {format}."
            )
    if time.tzinfo is None:
        time = time.replace(tzinfo=tz)
    return time.timestamp()


def get_table_schema(table: "TableDescriptor") -> "Schema":
    """
    Return the schema of the table.
    """
    from feathub.table.schema import Schema

    schema_builder = Schema.new_builder()
    for f in table.get_output_features():
        schema_builder.column(f.name, f.dtype)
    return schema_builder.build()


def is_local_file_or_dir(url: str) -> bool:
    """
    Check whether a url represents a local file or directory.
    """
    url_parsed = urlparse(url)
    return url_parsed.scheme in ("file", "")


def append_metadata_to_json(func: Any, __class__: Type) -> Any:
    """
    Decorates to_json methods, additionally saving the following
    metadata to each json dict.

    - "type": The full module and class name of the generator class.
    - "version": The version of the used json format. Currently version
                 value can only be 1.

    :param func: The to_json method to be wrapped.
    :param __class__: The host class for the to_json method.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        json_dict = func(*args, **kwargs)
        if "type" in json_dict or "version" in json_dict:
            raise FeathubException(
                f"f{__class__.__name__}#to_json should not contain metadata keys."
            )
        json_dict["type"] = __class__.__module__ + "." + __class__.__name__
        json_dict["version"] = 1
        return json_dict

    return wrapper


def from_json(json_dict: Dict) -> Any:
    """
    Converts a json dict to Python object. The input json dict must be generated
    by a to_json method decorated by append_metadata_to_json.
    """

    if json_dict["version"] != 1:
        raise FeathubException(
            f"Unsupported json format version {json_dict['version']}."
        )

    delimiter_index = str(json_dict["type"]).rindex(".")
    module_name = json_dict["type"][:delimiter_index]

    # avoid contradict requirements for code format between black and flake8
    class_name_start_index = delimiter_index + 1
    class_name = json_dict["type"][class_name_start_index:]

    return getattr(sys.modules[module_name], class_name).from_json(json_dict)
