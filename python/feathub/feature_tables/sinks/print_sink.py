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
from typing import Dict

from feathub.feature_tables.sinks.sink import Sink


class PrintSink(Sink):
    """
    PrintSink prints the table row by row to stdout.
    """

    def __init__(self) -> None:
        super().__init__(name="", system_name="print", table_uri={})

    def to_json(self) -> Dict:
        return {}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PrintSink":
        return PrintSink()
