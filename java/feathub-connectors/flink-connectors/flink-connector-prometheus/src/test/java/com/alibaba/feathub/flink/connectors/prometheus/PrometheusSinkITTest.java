/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.connectors.prometheus;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Prometheus sink. */
public class PrometheusSinkITTest extends PrometheusTableTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testSink() throws ExecutionException, InterruptedException, IOException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS TINYINT) AS tinyint_val, "
                                + "CAST(metric_val AS SMALLINT) AS smallint_val, "
                                + "CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val, "
                                + "CAST(metric_val AS DECIMAL) AS decimal_val, "
                                + "CAST(metric_val AS FLOAT) AS float_val, "
                                + "CAST(metric_val AS DOUBLE) AS double_val FROM sourceTable");

        final String hostUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .option("hostUrl", hostUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("groupingKey", "k1=v1;k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(hostUrl);
        final HashMap<String, String> expectedLabel = new HashMap<>();
        expectedLabel.put("k1", "\"v1\"");
        expectedLabel.put("k2", "\"v2\"");

        checkMetric(metrics.get("tinyint_val").get(0), "tinyint_val", "99", expectedLabel);
        checkMetric(metrics.get("smallint_val").get(0), "smallint_val", "99", expectedLabel);
        checkMetric(metrics.get("int_val").get(0), "int_val", "99", expectedLabel);
        checkMetric(metrics.get("bigint_val").get(0), "bigint_val", "99", expectedLabel);
        checkMetric(metrics.get("decimal_val").get(0), "decimal_val", "99", expectedLabel);
        checkMetric(metrics.get("float_val").get(0), "float_val", "99", expectedLabel);
        checkMetric(metrics.get("double_val").get(0), "double_val", "99", expectedLabel);
    }

    @Test
    void testSinkWithSameMetricName() throws ExecutionException, InterruptedException, IOException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val FROM sourceTable");

        final String hostUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column("int_val", DataTypes.INT())
                                                .withComment(
                                                        "{\"metric_name\":\"test_gauge\",\"type\":\"int\"}")
                                                .column("bigint_val", DataTypes.BIGINT())
                                                .withComment(
                                                        "{\"metric_name\":\"test_gauge\",\"type\":\"bigint\"}")
                                                .build())
                                .option("hostUrl", hostUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("groupingKey", "k1=v1;k2=v2")
                                .build())
                .await();

        final List<Metric> metrics = getMetrics(hostUrl).get("test_gauge");
        assertThat(metrics)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("type").equals("\"int\"")
                                        && metric.getValue().equals("99"));
        assertThat(metrics)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("type").equals("\"bigint\"")
                                        && metric.getValue().equals("99"));
    }

    @Test
    void testSinkColumnWithComment() throws ExecutionException, InterruptedException, IOException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS TINYINT) AS tinyint_val, "
                                + "CAST(metric_val AS SMALLINT) AS smallint_val, "
                                + "CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val, "
                                + "CAST(metric_val AS DECIMAL) AS decimal_val, "
                                + "CAST(metric_val AS FLOAT) AS float_val, "
                                + "CAST(metric_val AS DOUBLE) AS double_val FROM sourceTable");

        final String hostUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column("tinyint_val", DataTypes.TINYINT())
                                                .withComment(
                                                        "{\"k3\":\"v2\",\"metric_name\":\"tinyint_metric\",\"value_type\":\"tinyint\"}")
                                                .column("smallint_val", DataTypes.SMALLINT())
                                                .withComment(
                                                        "{\"metric_name\":\"smallint_metric\",\"value_type\":\"smallint\"}")
                                                .column("int_val", DataTypes.INT())
                                                .withComment(
                                                        "{\"metric_name\":\"int_metric\",\"value_type\":\"int\"}")
                                                .column("bigint_val", DataTypes.BIGINT())
                                                .withComment(
                                                        "{\"metric_name\":\"bigint_metric\",\"value_type\":\"bigint\"}")
                                                .column("decimal_val", DataTypes.DECIMAL(10, 2))
                                                .withComment(
                                                        "{\"metric_name\":\"decimal_metric\",\"value_type\":\"decimal\"}")
                                                .column("float_val", DataTypes.FLOAT())
                                                .withComment(
                                                        "{\"metric_name\":\"float_metric\",\"value_type\":\"float\"}")
                                                .column("double_val", DataTypes.DOUBLE())
                                                .withComment(
                                                        "{\"metric_name\":\"double_metric\",\"value_type\":\"double\"}")
                                                .build())
                                .option("hostUrl", hostUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("groupingKey", "k1=v1;k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(hostUrl);
        final HashMap<String, String> tableLabels = new HashMap<>();
        tableLabels.put("k1", "\"v1\"");
        tableLabels.put("k2", "\"v2\"");

        System.out.println(metrics.get("tinyint_metric").get(0));
        checkMetric(
                metrics.get("tinyint_metric").get(0),
                "tinyint_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"tinyint\""));
        checkMetric(
                metrics.get("smallint_metric").get(0),
                "smallint_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"smallint\""));
        checkMetric(
                metrics.get("int_metric").get(0),
                "int_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"int\""));
        checkMetric(
                metrics.get("bigint_metric").get(0),
                "bigint_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"bigint\""));
        checkMetric(
                metrics.get("decimal_metric").get(0),
                "decimal_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"decimal\""));
        checkMetric(
                metrics.get("float_metric").get(0),
                "float_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"float\""));
        checkMetric(
                metrics.get("double_metric").get(0),
                "double_metric",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"double\""));
    }

    @Test
    void testSinkMapMetric() throws ExecutionException, InterruptedException, IOException {
        Map<String, Integer> metricMap = new HashMap<>();
        metricMap.put("a", 100);
        metricMap.put("b", 101);
        final Table metricTable =
                tEnv.fromDataStream(
                                env.fromElements(Row.of(metricMap)),
                                Schema.newBuilder()
                                        .column(
                                                "f0",
                                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                                        .build())
                        .as("metric_map");

        final String hostUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column(
                                                        "metric_map",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.INT()))
                                                .withComment(
                                                        "{\"key_label_name\":\"count_map_key\"}")
                                                .build())
                                .option("hostUrl", hostUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("groupingKey", "k1=v1;k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(hostUrl);

        final List<Metric> mapMetric = metrics.get("metric_map");
        assertThat(mapMetric).hasSize(2);
        assertThat(mapMetric)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("count_map_key").equals("\"a\"")
                                        && metric.getValue().equals("100"));
        assertThat(mapMetric)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("count_map_key").equals("\"b\"")
                                        && metric.getValue().equals("101"));
    }

    @Test
    void testSameMetricNameWithDifferentLabelsThrowException()
            throws ExecutionException, InterruptedException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val FROM sourceTable");

        final String hostUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();

        assertThatThrownBy(
                        () ->
                                metricTable
                                        .executeInsert(
                                                TableDescriptor.forConnector("prometheus")
                                                        .schema(
                                                                Schema.newBuilder()
                                                                        .column(
                                                                                "int_val",
                                                                                DataTypes.INT())
                                                                        .withComment(
                                                                                "{\"metric_name\":\"test_gauge\",\"type\":\"int\"}")
                                                                        .column(
                                                                                "bigint_val",
                                                                                DataTypes.BIGINT())
                                                                        .withComment(
                                                                                "{\"metric_name\":\"test_gauge\",\"type2\":\"bigint\",\"type3\":\"a\"}")
                                                                        .build())
                                                        .option("hostUrl", hostUrl)
                                                        .option("deleteOnShutdown", "false")
                                                        .option("jobName", "testSink")
                                                        .option("groupingKey", "k1=v1;k2=v2")
                                                        .build())
                                        .await())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Invalid metric");
    }

    private Map<String, String> getExpectedLabels(
            Map<String, String> tableLabels, String... keyValues) {
        final HashMap<String, String> res = new HashMap<>(tableLabels);
        for (int i = 0; i < keyValues.length; i += 2) {
            res.put(keyValues[i], keyValues[i + 1]);
        }
        return res;
    }

    private void checkMetric(
            Metric metric, String name, String value, Map<String, String> expectedLabels) {
        assertThat(metric.getName()).isEqualTo(name);
        assertThat(metric.getValue()).isEqualTo(value);
        assertThat(metric.getLabels()).containsAllEntriesOf(expectedLabels);
    }

    private static HashMap<String, List<Metric>> getMetrics(String hostUrl) throws IOException {
        URL url = new URL("http://" + hostUrl + "/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;

        final HashMap<String, List<Metric>> metrics = new HashMap<>();
        while ((inputLine = in.readLine()) != null) {
            if (inputLine.startsWith("#")) {
                // Ignore comment line.
                continue;
            }

            final Metric metric = Metric.parseMetric(inputLine);
            metrics.computeIfAbsent(metric.name, key -> new ArrayList<>()).add(metric);
        }
        in.close();

        return metrics;
    }

    private static class Metric {
        private final String name;
        private final String value;
        private final Map<String, String> labels;

        public Metric(String name, String value, Map<String, String> labels) {
            this.name = name;
            this.value = value;
            this.labels = labels;
        }

        public static Metric parseMetric(String metricString) throws IllegalArgumentException {
            final String[] split = metricString.split(" ");

            if (split.length != 2) {
                throw new IllegalArgumentException(
                        String.format("Illegal metric string: %s", metricString));
            }

            final String metricAndLabel = split[0].trim();
            final String value = split[1].trim();
            final Pattern pattern = Pattern.compile("(.*)\\{(.*)\\}");
            final Matcher matcher = pattern.matcher(metricAndLabel);
            if (!matcher.find()) {
                return new Metric(metricAndLabel, value, Collections.emptyMap());
            }
            final String metricName = matcher.group(1);
            final String labelsString = matcher.group(2);
            final HashMap<String, String> labels = new HashMap<>();
            for (String pair : labelsString.split(",")) {
                final String[] splitPair = pair.split("=");
                if (splitPair.length != 2) {
                    throw new IllegalArgumentException(String.format("Illegal label: %s", pair));
                }
                labels.put(splitPair[0], splitPair[1]);
            }
            return new Metric(metricName, split[1], labels);
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Metric metric = (Metric) o;
            return Objects.equals(name, metric.name)
                    && Objects.equals(value, metric.value)
                    && Objects.equals(labels, metric.labels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, labels);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Metric{");
            sb.append("name='").append(name).append('\'');
            sb.append(", value='").append(value).append('\'');
            sb.append(", labels=").append(labels);
            sb.append('}');
            return sb.toString();
        }
    }
}
