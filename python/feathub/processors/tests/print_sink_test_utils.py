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
from abc import abstractmethod

from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.processors.processor import Processor
from feathub.processors.tests.processor_test_utils import ProcessorTestBase
from feathub.registries.registry import Registry


class PrintSinkTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify PrintSink.
    """

    __test__ = False

    @abstractmethod
    def get_processor(self, registry: Registry) -> Processor:
        pass

    def test_print_sink(self):
        source = self._create_file_source(self.input_data.copy(), schema=self.schema)

        sink = PrintSink()

        self.processor.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()
