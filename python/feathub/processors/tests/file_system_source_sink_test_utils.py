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

from feathub.feathub_client import FeathubClient
from feathub.processors.tests.processor_test_utils import ProcessorTestBase


class FileSystemSourceSinkTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify FileSystemSource and FileSystemSink.
    """

    __test__ = False

    @abstractmethod
    def get_client(self) -> FeathubClient:
        pass

    # TODO: unify the structure of files written out by different processors.

    # TODO: Add test case to verify allow_overwrite.
