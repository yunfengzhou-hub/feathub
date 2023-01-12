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
import glob
import os
import shutil
import tempfile

import pandas as pd

from feathub.common.exceptions import FeathubException
from feathub.common.types import String, Int64, Float64
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.tests.datagen_source_test_base import DataGenSourceTestBase
from feathub.processors.tests.expression_transform_test_base import (
    ExpressionTransformTestBase,
)
from feathub.processors.tests.file_system_source_sink_test_base import (
    FileSystemSourceSinkTestBase,
)
from feathub.processors.tests.get_features_test_base import GetFeaturesTestBase
from feathub.processors.tests.join_transform_test_base import JoinTransformTestBase
from feathub.processors.tests.kafka_source_sink_test_base import (
    KafkaSourceSinkTestBase,
)
from feathub.processors.tests.mixed_transform_test_base import MixedTransformTestBase
from feathub.processors.tests.over_window_transform_test_base import (
    OverWindowTransformTestBase,
)
from feathub.processors.tests.print_sink_test_base import PrintSinkTestBase
from feathub.processors.tests.processor_test_base import ProcessorTestBase
from feathub.processors.tests.redis_source_sink_test_base import (
    RedisSourceSinkTestBase,
)
from feathub.processors.tests.sliding_window_transform_test_base import (
    SlidingWindowTransformTestBase,
)
from feathub.table.schema import Schema


class FlinkProcessorTestBase(
    ProcessorTestBase,
):
    __test__ = False

    client = None

    @classmethod
    def setUpClass(cls) -> None:
        # Due to the resource leak in PyFlink StreamExecutionEnvironment and
        # StreamTableEnvironment https://issues.apache.org/jira/browse/FLINK-30258.
        # We want to share env and t_env across all the tests in one class to mitigate
        # the leak.
        # TODO: After the ticket is resolved, we should clean up the resource in
        #  StreamExecutionEnvironment and StreamTableEnvironment after every test to
        #  fully avoid resource leak.
        cls.client = cls._get_local_client(
            {
                "type": "flink",
                "flink": {
                    "deployment_mode": "cli",
                },
            }
        )

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.input_data, self.schema = self._create_input_data_and_schema()

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    def get_client(self) -> FeathubClient:
        return self.client


class FlinkProcessorTest(
    FlinkProcessorTestBase,
    DataGenSourceTestBase,
    ExpressionTransformTestBase,
    FileSystemSourceSinkTestBase,
    GetFeaturesTestBase,
    JoinTransformTestBase,
    MixedTransformTestBase,
    OverWindowTransformTestBase,
    PrintSinkTestBase,
    SlidingWindowTransformTestBase,
):
    __test__ = True

    def test_read_write(self) -> None:
        source = self._create_file_source(self.input_data)

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name

        sink = FileSystemSink(sink_path, "csv")

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        files = glob.glob(f"{sink_path}/*")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["name", "cost", "distance", "time"])
            df = df.append(csv)
        df = df.sort_values(by=["time"]).reset_index(drop=True)
        self.assertTrue(self.input_data.equals(df))

    def test_unsupported_file_format(self):
        source = self._create_file_source(self.input_data)
        sink = FileSystemSink("s3://dummy-bucket/path", "csv")
        with self.assertRaisesRegex(
            FeathubException, "Cannot sink files in CSV format to s3"
        ):
            self.client.materialize_features(
                features=source, sink=sink, allow_overwrite=True
            )

    def test_join_transform_with_zoned_timestamp(self):
        # TODO: Add public API on Feathub Client/Processor to configure time zone,
        #  then move this test case to JoinTransformTestBase
        prev_client = self.client
        self.client = self._get_local_client(
            {
                "type": "flink",
                "flink": {
                    "deployment_mode": "cli",
                    "native.table.local-time-zone": "Asia/Shanghai",
                },
            }
        )

        df_1 = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.000"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.004"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.006"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.008"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.010"],
            ],
            columns=["name", "cost", "distance", "time"],
        )
        source = self._create_file_source(
            df_1,
            schema=Schema(
                ["name", "cost", "distance", "time"], [String, Int64, Int64, String]
            ),
            timestamp_format="%Y-%m-%d %H:%M:%S.%f",
        )
        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                Feature(
                    name="distance",
                    dtype=Int64,
                    transform="distance",
                ),
            ],
            keep_source_fields=True,
        )

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 08:00:00.001 +0800"],
                ["Emma", 400.0, "2022-01-01 00:00:00.003 +0000"],
                ["Alex", 200.0, "2022-01-01 08:00:00.005 +0800"],
                ["Emma", 300.0, "2022-01-01 00:00:00.007 +0000"],
                ["Jack", 500.0, "2022-01-01 08:00:00.009 +0800"],
                ["Alex", 450.0, "2022-01-01 00:00:00.011 +0000"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self._create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d %H:%M:%S.%f %z",
            keys=["name"],
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
            ],
            keep_source_fields=False,
        )

        feature_view_3 = DerivedFeatureView(
            name="feature_view_3",
            source=feature_view_2,
            features=[
                Feature(
                    name="derived_cost",
                    dtype=Float64,
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=True,
        )

        [_, built_feature_view_2, built_feature_view_3] = self.client.build_features(
            [source_2, feature_view_2, feature_view_3]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=built_feature_view_3)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertIsNone(feature_view_1.keys)
        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertListEqual(["name"], built_feature_view_3.keys)
        self.assertTrue(expected_result_df.equals(result_df))

        self.client = prev_client

    # TODO: Fix the bug that FlinkProcessor to_pandas does not support none values.
    def test_join_sliding_feature(self):
        pass

    # TODO: Fix the bug that FlinkProcessor to_pandas does not support none values.
    def test_over_window_on_join_field(self):
        pass


class FlinkProcessorKafkaSourceSinkTest(
    FlinkProcessorTestBase, KafkaSourceSinkTestBase
):
    __test__ = True

    @classmethod
    def setUpClass(cls) -> None:
        FlinkProcessorTestBase.setUpClass()
        KafkaSourceSinkTestBase.setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        FlinkProcessorTestBase.tearDownClass()
        KafkaSourceSinkTestBase.tearDownClass()

    def setUp(self) -> None:
        FlinkProcessorTestBase.setUp(self)
        KafkaSourceSinkTestBase.setUp(self)


class FlinkProcessorRedisSourceSinkTest(
    FlinkProcessorTestBase, RedisSourceSinkTestBase
):
    __test__ = True

    @classmethod
    def setUpClass(cls) -> None:
        FlinkProcessorTestBase.setUpClass()
        RedisSourceSinkTestBase.setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        FlinkProcessorTestBase.tearDownClass()
        RedisSourceSinkTestBase.tearDownClass()

    def setUp(self) -> None:
        FlinkProcessorTestBase.setUp(self)
        RedisSourceSinkTestBase.setUp(self)

    # TODO: Fix the bug that in test_redis_sink when column "val"
    #  contains None, all values in this column are saved as None
    #  to Redis.
