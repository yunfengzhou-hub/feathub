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
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime
from typing import Union, Optional, Dict

import pandas as pd
from pyspark.sql import SparkSession

from feathub.dsl.expr_parser import ExprParser
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_views.feature_view import FeatureView
from feathub.processors.processor import Processor
from feathub.processors.spark.ast_evaluator.spark_ast_evaluator import SparkAstEvaluator
from feathub.processors.spark.dataframe_builder.spark_dataframe_builder import (
    SparkDataFrameBuilder,
)
from feathub.processors.spark.spark_job import SparkJob
from feathub.processors.spark.spark_processor_config import (
    SparkProcessorConfig,
    MASTER_CONFIG,
)
from feathub.processors.spark.spark_table import SparkTable
from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class SparkProcessor(Processor):
    """
    The SparkProcessor does feature ETL using Spark as the processing engine.

    In the following we describe the keys accepted by the `config` dict passed to the
    SparkProcessor constructor.

    master: The Spark cluster manager to connect to.
    """

    def __init__(self, props: Dict, registry: Registry):
        """
        Instantiate the SparkProcessor.

        :param props: The processor properties.
        :param registry: An entity registry.
        """
        super().__init__()
        self.registry = registry

        config = SparkProcessorConfig(props)
        spark_builder = SparkSession.builder

        if config.get(MASTER_CONFIG) is not None:
            spark_builder = spark_builder.master(config.get(MASTER_CONFIG))

        self.spark = spark_builder.getOrCreate()
        self.dataframe_builder = SparkDataFrameBuilder(self.spark, self.registry)
        self.executor = ThreadPoolExecutor()

        self.parser = ExprParser()
        self.ast_evaluator = SparkAstEvaluator()

    def get_table(
        self,
        features: Union[str, TableDescriptor],
        keys: Union[pd.DataFrame, TableDescriptor, None] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> SparkTable:
        features = self._resolve_table_descriptor(features)

        return SparkTable(
            spark_processor=self,
            feature=features,
            keys=keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def materialize_features(
        self,
        features: Union[str, TableDescriptor],
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        allow_overwrite: bool = False,
    ) -> SparkJob:
        resolved_features = self._resolve_table_descriptor(features)

        spark_table = self.get_table(
            features=resolved_features,
            keys=resolved_features.keys,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

        return spark_table.execute_insert(
            sink=sink, ttl=ttl, allow_overwrite=allow_overwrite
        )

    def _resolve_table_descriptor(
        self, features: Union[str, TableDescriptor]
    ) -> TableDescriptor:
        if isinstance(features, str):
            features = self.registry.get_features(name=features)
        elif isinstance(features, FeatureView) and features.is_unresolved():
            features = self.registry.get_features(name=features.name)

        return features
