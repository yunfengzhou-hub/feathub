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
import re
from abc import ABC
from datetime import timedelta
from urllib import request

from prometheus_client import (
    CollectorRegistry,
    Gauge,
    push_to_gateway,
    delete_from_gateway,
)
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
)
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.metric_stores.metric import Count, Ratio
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class PrometheusPushGatewayContainer(DockerContainer):
    def __init__(self, image="prom/pushgateway:v1.6.0", **kwargs):
        super(PrometheusPushGatewayContainer, self).__init__(image, **kwargs)
        self.with_exposed_ports(9091)

    @wait_container_is_ready(IOError)
    def _wait_container_ready(self):
        registry = CollectorRegistry()
        g = Gauge("probe", "probe", registry=registry)
        g.set_to_current_time()
        push_to_gateway(self.get_host_url(), job="probe-job", registry=registry)
        delete_from_gateway(self.get_host_url(), "probe-job")

    def start(self):
        super().start()
        self._wait_container_ready()
        return self

    def get_host_url(self):
        return f"{self.get_container_host_ip()}:{self.get_exposed_port(9091)}"


class PrometheusMetricStoreITTest(ABC, FeathubITTestBase):
    container: PrometheusPushGatewayContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.container = PrometheusPushGatewayContainer()
        cls.container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.container.stop()

    def test_prometheus_metric_store(self):
        self.client = self.get_client(
            extra_config={
                "metric_store": {
                    "type": "prometheus",
                    "report_interval": timedelta(seconds=5),
                    "prometheus": {
                        "host_url": self.container.get_host_url(),
                        "delete_on_shutdown": False,
                    },
                }
            }
        )
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
            ),
            metrics=[
                Count(
                    filter_expr="> 0",
                    window_size=timedelta(days=1),
                ),
                Ratio(
                    filter_expr="> 0",
                    window_size=timedelta(days=1),
                )
            ],
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
        )

        self.client.materialize_features(
            features, sink=BlackHoleSink(), allow_overwrite=True
        ).wait()

        url = f"http://{self.container.get_host_url()}/metrics"
        contents: str = request.urlopen(url).read().decode()
        metrics = []
        for content in contents.split("\n"):
            if not content.startswith("feathub"):
                continue
            metric_value = content.split(" ")[-1]
            name_and_tags = content[:-len(metric_value)-1]
            metric_name = name_and_tags.split("{")[0]
            tags = dict()
            for tag_str in re.split(r'(?<="),', name_and_tags[len(metric_name)+1:-1]):
                tag_key = tag_str.split("=")[0]
                tag_value = tag_str[len(tag_key)+2:-1]
                tags[tag_key] = tag_value
            metrics.append((metric_name, tags, metric_value))

        self.assertEqual(len(metrics), 2)
        self.assertEqual(metrics[0][0], "feathub_default_feature_count")
        self.assertEqual(metrics[0][1]["feature_name"], "total_cost")
        self.assertEqual(metrics[0][1]["filter_expr"], "> 0")
        self.assertEqual(metrics[0][1]["table_name"], "")
        self.assertEqual(metrics[0][1]["window_time"], str(timedelta(days=1)))
        # TODO: setup Prometheus server together with Prometheus PushGateway
        #  and verify history metric value.
        self.assertEqual(metrics[0][2], '0')

        self.assertEqual(metrics[1][0], "feathub_default_feature_ratio")
        self.assertEqual(metrics[1][1]["feature_name"], "total_cost")
        self.assertEqual(metrics[1][1]["filter_expr"], "> 0")
        self.assertEqual(metrics[1][1]["table_name"], "")
        self.assertEqual(metrics[1][1]["window_time"], str(timedelta(days=1)))
        self.assertEqual(metrics[1][2], '0')
