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

package com.alibaba.feathub.flink.connectors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.DELETE_ON_SHUTDOWN;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.GROUPING_KEY;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.HOST_URL;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.JOB_NAME;

/**
 * A {@link org.apache.flink.streaming.api.functions.sink.SinkFunction} that writes to a Prometheus
 * PushGateway. Note that the SinkFunction only support writing numeric columns or map columns that
 * map from string to numeric values. Each column can be configured with a json map via the column
 * comment. Below are the configuration of the column:
 *
 * <ul>
 *   <li>metric_name: This specifies the metric name under which the column value is reported. If
 *       this is not given, the name of the column will be used.
 *   <li>key_label_name: Only effective with map column. This specifies the label name of the map
 *       key.
 * </ul>
 *
 * <p>Other configurations are added to the metric labels. The following examples illustrate how to
 * define a Prometheus Sink table:
 *
 * <pre>
 *     TableDescriptor.forConnector("prometheus")
 *         .schema(
 *                 Schema.newBuilder()
 *                         .column(
 *                                 "metric_map",
 *                                 DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
 *                         .withComment("{\"key_label_name\":\"count_map_key\"}")
 *                         .column("metric", DataTypes.DOUBLE())
 *                         .withComment("{\"metric_name\":\"metric\"}")
 *                         .build())
 *         .option("hostUrl", "localhost:9091")
 *         .option("jobName", "exampleJob")
 *         .option("groupingKey", "k1=v1;k2=v2")
 *         .build();
 * </pre>
 */
public class PrometheusSinkFunction extends RichSinkFunction<RowData> {

    private final ReadableConfig config;
    private final String jobName;
    private final Map<String, String> groupingKey;
    private final List<DataType> fieldTypes;
    private final List<MetricMeta> metricMetas;
    private final Map<String, SimpleCollector<Gauge.Child>> nameToCollectors;
    private CollectorRegistry registry;
    private PushGateway pushGateway;

    public PrometheusSinkFunction(ReadableConfig config, ResolvedSchema schema) {
        this.config = config;
        this.fieldTypes = schema.getColumnDataTypes();
        this.nameToCollectors = new HashMap<>();
        this.jobName = config.get(JOB_NAME);
        this.groupingKey = parseGroupingKey(config.get(GROUPING_KEY));

        final List<Column> columns = schema.getColumns();
        this.metricMetas = new ArrayList<>(columns.size());

        Map<String, Set<String>> metricNameToLabelNames = new HashMap<>();
        for (Column column : columns) {
            verifyType(column.getDataType().getLogicalType());
            final MetricMeta metricMeta = MetricMeta.fromColumn(column);
            final Set<String> labelNames = new HashSet<>(Arrays.asList(metricMeta.getLabelNames()));
            if (!metricNameToLabelNames.containsKey(metricMeta.name)) {
                metricNameToLabelNames.put(metricMeta.name, labelNames);
            }
            if (!metricNameToLabelNames.get(metricMeta.name).equals(labelNames)) {
                throw new RuntimeException(
                        String.format(
                                "Invalid metric: %s. Metric with same name should have the same label names.",
                                metricMeta.name));
            }
            metricMetas.add(metricMeta);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        registry = new CollectorRegistry();
        pushGateway = new PushGateway(config.get(HOST_URL));

        for (final MetricMeta metricMeta : metricMetas) {
            final String metricName = metricMeta.name;
            final SimpleCollector<Gauge.Child> collector =
                    nameToCollectors.computeIfAbsent(
                            metricName,
                            key ->
                                    Gauge.build()
                                            .name(key)
                                            .help(key)
                                            .labelNames(metricMeta.getLabelNames())
                                            .register(registry));
            metricMeta.setCollector(collector);
        }
    }

    private static void verifyType(LogicalType logicalType) {
        if (logicalType.is(LogicalTypeRoot.MAP)) {
            final MapType mapType = (MapType) logicalType;
            if (mapType.getKeyType().is(LogicalTypeRoot.VARCHAR)
                    && mapType.getValueType().is(LogicalTypeFamily.NUMERIC)) {
                return;
            }
        } else if (logicalType.is(LogicalTypeFamily.NUMERIC)) {
            return;
        }
        throw new RuntimeException(
                "PrometheusSink only support numeric data type or map data type from string to numeric.");
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        for (int i = 0; i < metricMetas.size(); i++) {
            setMetric(row, i, fieldTypes.get(i).getLogicalType());
        }
        pushGateway.pushAdd(registry, jobName, groupingKey);
    }

    public void setMetric(RowData row, int index, LogicalType type) throws IOException {
        switch (type.getTypeRoot()) {
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                metricMetas.get(index).getChild().set(getDoubleValue(row, index, type));
                break;
            case MAP:
                final MapData mapData = row.getMap(index);
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();

                for (int j = 0; j < keyArray.size(); j++) {
                    final String keyLabel = keyArray.getString(j).toString();
                    metricMetas
                            .get(index)
                            .getChild(keyLabel)
                            .set(getDoubleValue(valueArray, j, ((MapType) type).getValueType()));
                }
                break;
            default:
                throw new IOException(
                        String.format("Unsupported data type: %s", fieldTypes.get(index)));
        }
    }

    private double getDoubleValue(RowData row, int index, LogicalType type) throws IOException {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                final DecimalData decimal = row.getDecimal(index, precision, scale);
                return decimal.toBigDecimal().doubleValue();
            case TINYINT:
                return row.getByte(index);
            case SMALLINT:
                return row.getShort(index);
            case INTEGER:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            default:
                throw new IOException(
                        String.format("Unsupported data type: %s", fieldTypes.get(index)));
        }
    }

    private double getDoubleValue(ArrayData array, int index, LogicalType type) throws IOException {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                final DecimalData decimal = array.getDecimal(index, precision, scale);
                return decimal.toBigDecimal().doubleValue();
            case TINYINT:
                return array.getByte(index);
            case SMALLINT:
                return array.getShort(index);
            case INTEGER:
                return array.getInt(index);
            case BIGINT:
                return array.getLong(index);
            case FLOAT:
                return array.getFloat(index);
            case DOUBLE:
                return array.getDouble(index);
            default:
                throw new IOException(
                        String.format("Unsupported data type: %s", fieldTypes.get(index)));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (config.get(DELETE_ON_SHUTDOWN)) {
            pushGateway.delete(jobName);
        }
    }

    static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    throw new RuntimeException(
                            String.format("Invalid prometheusPushGateway groupingKey:%s", kv));
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)) {
                    throw new RuntimeException(
                            String.format("GroupingKey %s must not be empty", labelKey));
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }

    private static class MetricMeta implements Serializable {
        public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        public static final String METRIC_NAME = "metric_name";
        public static final String KEY_LABEL_NAME = "key_label_name";

        private final String name;
        private final List<String> labelNames;
        private final List<String> labels;

        private final Map<String, Gauge.Child> children;

        private transient Gauge.Child child;

        private transient SimpleCollector<Gauge.Child> collector;

        MetricMeta(String name, List<String> labelNames, List<String> labels, String keyLabelName) {
            this.name = name;
            this.labelNames = labelNames;
            this.labels = labels;
            this.children = new HashMap<>();

            if (keyLabelName != null) {
                this.labelNames.add(keyLabelName);
            }
        }

        static MetricMeta fromColumn(Column column) {
            final TypeReference<HashMap<String, String>> typeReference =
                    new TypeReference<HashMap<String, String>>() {};
            Map<String, String> meta;
            try {
                meta = OBJECT_MAPPER.readValue(column.getComment().orElse("{}"), typeReference);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            String name = column.getName();
            if (meta.containsKey(METRIC_NAME)) {
                name = meta.remove(METRIC_NAME);
            }

            return new MetricMeta(
                    name,
                    new ArrayList<>(meta.keySet()),
                    new ArrayList<>(meta.values()),
                    meta.remove(KEY_LABEL_NAME));
        }

        void setCollector(SimpleCollector<Gauge.Child> collector) {
            this.collector = collector;
        }

        Gauge.Child getChild(String keyLabel) {
            return children.computeIfAbsent(
                    keyLabel,
                    label -> {
                        final ArrayList<String> allLabels = new ArrayList<>(this.labels);
                        allLabels.add(label);
                        return collector.labels(allLabels.toArray(new String[0]));
                    });
        }

        Gauge.Child getChild() {
            if (child == null) {
                child = collector.labels(labels.toArray(new String[0]));
            }
            return child;
        }

        String[] getLabelNames() {
            return labelNames.toArray(new String[0]);
        }
    }
}
