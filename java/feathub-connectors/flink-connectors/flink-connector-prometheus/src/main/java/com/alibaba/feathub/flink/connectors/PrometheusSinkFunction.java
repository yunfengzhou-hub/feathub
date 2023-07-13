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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.StringUtils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.DELETE_ON_SHUTDOWN;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.EXTRA_LABELS;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.JOB_NAME;
import static com.alibaba.feathub.flink.connectors.PrometheusConfigs.SERVER_URL;

/**
 * A {@link org.apache.flink.streaming.api.functions.sink.SinkFunction} that writes data to a
 * Prometheus PushGateway. This SinkFunction supports numeric columns, list columns with numeric
 * values, and map columns that map from strings to numeric values. Users can specify sets of labels
 * for each column via the column comments. Label sets are separated by ';', labels within a set are
 * separated by ',', and label names and values are separated by '='. For example:
 * k1=v11,k2=v21;k1=v12,k2=v22.
 *
 * <p>The behavior of the Prometheus Sink for different column types is as follows:
 *
 * <ul>
 *   <li>Numeric Column: Each column is emitted as a separate metric. Users can specify zero or one
 *       label set for the metric to be emitted.
 *   <li>List Column: Each element in the list is emitted as a separate metric. Users must specify
 *       the same number of label sets as the number of elements in the list. Each label set
 *       represents the corresponding element's labels. Additionally, all label sets must have the
 *       same set of keys.
 *   <li>Map Column: Each entry in the map is emitted as a separate metric. Users must specify
 *       exactly one label set, which must have at least one label with the value "null". The label
 *       with value "null" will be replaced with the map's key when emitting the metric.
 * </ul>
 *
 * <p>Below is an example illustrating how to define a Prometheus Sink table:
 *
 * <pre>
 *     TableDescriptor.forConnector("prometheus")
 *         .schema(
 *             Schema.newBuilder()
 *                 .column(
 *                     "metric_map",
 *                     DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
 *                 .withComment("feature=a,count_map_key=null")
 *                 .column(
 *                     "metric_array",
 *                     DataTypes.ARRAY(DataTypes.INT()))
 *                 .withComment("feature=a,type=int;feature=b,type=int")
 *                 .column("metric", DataTypes.DOUBLE())
 *                 .withComment("type=double")
 *                 .build())
 *         .option("serverUrl", "localhost:9091")
 *         .option("jobName", "exampleJob")
 *         .option("extraLabels", "k1=v1,k2=v2")
 *         .build();
 * </pre>
 */
public class PrometheusSinkFunction extends RichSinkFunction<RowData> {

    private final String jobName;
    private final Map<String, String> extraLabels;
    private final List<ColumnInfo> columnInfos;
    private final String serverUrl;
    private final Boolean deleteOnShutdown;
    private CollectorRegistry registry;
    private PushGateway pushGateway;
    private List<MetricUpdater<?>> metricUpdaters;

    public PrometheusSinkFunction(ReadableConfig config, ResolvedSchema schema) {
        this.jobName = config.get(JOB_NAME);
        this.extraLabels = parseLabels(config.get(EXTRA_LABELS));
        this.serverUrl = config.get(SERVER_URL);
        this.deleteOnShutdown = config.get(DELETE_ON_SHUTDOWN);

        this.columnInfos = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            this.columnInfos.add(ColumnInfo.fromColumn(column));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        registry = new CollectorRegistry();
        pushGateway = new PushGateway(serverUrl);

        metricUpdaters = new ArrayList<>();
        for (final ColumnInfo columnInfo : this.columnInfos) {
            metricUpdaters.add(columnInfo.getMetricWrapper(registry));
        }
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        for (int i = 0; i < columnInfos.size(); i++) {
            setMetric(row, i);
        }
        pushGateway.pushAdd(registry, jobName, extraLabels);
    }

    public void setMetric(RowData row, int index) throws IOException {
        final LogicalType type = columnInfos.get(index).dataType.getLogicalType();
        switch (type.getTypeRoot()) {
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                ((SimpleMetricUpdater) metricUpdaters.get(index))
                        .update(getDoubleValue(row, index));
                break;
            case MAP:
                final MapData mapData = row.getMap(index);
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                Map<String, Double> mapMetric = new HashMap<>();
                for (int j = 0; j < keyArray.size(); j++) {
                    final String keyLabel = keyArray.getString(j).toString();
                    mapMetric.put(
                            keyLabel,
                            getDoubleValue(valueArray, j, ((MapType) type).getValueType()));
                }
                ((MapMetricUpdater) metricUpdaters.get(index)).update(mapMetric);
                break;
            case ARRAY:
                final ArrayData arrayData = row.getArray(index);
                List<Double> listMetric = new ArrayList<>();
                for (int j = 0; j < arrayData.size(); ++j) {
                    listMetric.add(
                            getDoubleValue(arrayData, j, ((ArrayType) type).getElementType()));
                }
                ((ListMetricUpdater) metricUpdaters.get(index)).update(listMetric);
                break;
            default:
                throw new IOException(String.format("Unsupported data type: %s", type));
        }
    }

    private double getDoubleValue(RowData row, int index) throws IOException {
        final LogicalType type = columnInfos.get(index).dataType.getLogicalType();
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
                throw new IOException(String.format("Unsupported data type: %s", type));
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
                throw new IOException(String.format("Unsupported data type: %s", type));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (deleteOnShutdown) {
            pushGateway.delete(jobName);
        }
    }

    private static Map<String, String> parseLabels(final String labelsConfig) {
        if (!labelsConfig.isEmpty()) {
            Map<String, String> labelNames = new HashMap<>();
            String[] kvs = labelsConfig.split(",");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    throw new RuntimeException(
                            String.format("Invalid prometheusPushGateway labels:%s", kv));
                }

                String labelName = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelName)) {
                    throw new RuntimeException(
                            String.format("Invalid labelName:%s must not be empty", labelName));
                }
                labelNames.put(labelName, labelValue);
            }

            return labelNames;
        }

        return Collections.emptyMap();
    }

    private static class ColumnInfo implements Serializable {
        private final String name;
        private final String comment;
        private final DataType dataType;

        private ColumnInfo(String name, String comment, DataType dataType) {
            this.name = name;
            this.comment = comment;
            this.dataType = dataType;
        }

        private static ColumnInfo fromColumn(Column column) {
            verifyType(column.getDataType().getLogicalType());
            return new ColumnInfo(
                    column.getName(), column.getComment().orElse(""), column.getDataType());
        }

        private static void verifyType(LogicalType logicalType) {
            if (logicalType.is(LogicalTypeRoot.MAP)) {
                final MapType mapType = (MapType) logicalType;
                if (mapType.getKeyType().is(LogicalTypeRoot.VARCHAR)
                        && mapType.getValueType().is(LogicalTypeFamily.NUMERIC)) {
                    return;
                }
            } else if (logicalType.is(LogicalTypeRoot.ARRAY)) {
                if (((ArrayType) logicalType).getElementType().is(LogicalTypeFamily.NUMERIC)) {
                    return;
                }
            } else if (logicalType.is(LogicalTypeFamily.NUMERIC)) {
                return;
            }
            throw new RuntimeException(
                    "PrometheusSink only support numeric data type, array data type of numeric type, "
                            + "and map data type from string type to numeric type.");
        }

        private MetricUpdater<?> getMetricWrapper(CollectorRegistry registry) {
            final List<Map<String, String>> labels = getLabels();
            switch (dataType.getLogicalType().getTypeRoot()) {
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    if (labels.size() > 1) {
                        throw new RuntimeException(
                                "There can be at most one set of labels for numeric type metric.");
                    }
                    return new SimpleMetricUpdater(
                            name,
                            labels.isEmpty() ? Collections.emptyMap() : labels.get(0),
                            registry);
                case MAP:
                    if (labels.size() != 1) {
                        throw new RuntimeException(
                                "There can be exactly one set of labels for map type metric.");
                    }
                    return new MapMetricUpdater(name, labels.get(0), registry);
                case ARRAY:
                    return new ListMetricUpdater(name, labels, registry);
                default:
                    throw new RuntimeException(
                            String.format("Unsupported data type: %s", dataType));
            }
        }

        private List<Map<String, String>> getLabels() {
            List<Map<String, String>> labels = new ArrayList<>();
            final String[] labelStrings = comment.split(";");
            for (String labelString : labelStrings) {
                labels.add(parseLabels(labelString));
            }

            return labels;
        }
    }
}
