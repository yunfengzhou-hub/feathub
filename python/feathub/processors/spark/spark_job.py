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
from concurrent.futures import Future, Executor
from typing import Optional

from feathub.processors.processor_job import ProcessorJob


class SparkJob(ProcessorJob):
    """Represent a Spark job."""

    def __init__(
        self,
        job_future: Future,
        executor: Executor,
    ) -> None:
        super().__init__()
        self._job_future = job_future
        self._executor = executor

    def cancel(self) -> Future:
        return self._executor.submit(self._cancel)

    def _cancel(self) -> None:
        self._job_future.cancel()

    def wait(self, timeout_ms: Optional[int] = None) -> None:
        timeout_sec = None if timeout_ms is None else timeout_ms / 1000
        self._job_future.result(timeout=timeout_sec)
