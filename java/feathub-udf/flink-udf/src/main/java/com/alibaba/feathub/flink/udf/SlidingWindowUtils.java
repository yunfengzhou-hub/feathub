/*
 * Copyright 2022 The Feathub Authors
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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.feathub.flink.udf.processfunction.PostSlidingWindowZeroValuedRowExpiredRowHandler;
import com.alibaba.feathub.flink.udf.processfunction.SlidingWindowKeyedCoProcessFunction;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/** Utility methods to apply sliding windows. */
public class SlidingWindowUtils {
    /**
     * Apply sliding window with the given step size to the given {@link Table} and {@link
     * AggregationFieldsDescriptor}. The {@link AggregationFieldsDescriptor} describes how to
     * compute each field. It includes the field name and data type of the input and output, the
     * size of the sliding window under which the aggregation performs, and the aggregation
     * function, e.g. SUM, AVG, etc.
     *
     * <p>The {@link SlidingWindowKeyedCoProcessFunction} is optimized to reduce the state usage
     * when computing aggregations under different sliding window sizes.
     *
     * @param tEnv The StreamTableEnvironment of the table.
     * @param table The input table.
     * @param keyFieldNames The names of the group by keys for the sliding window.
     * @param rowTimeFieldName The name of the row time field.
     * @param stepSizeMs The step size of the sliding window in milliseconds.
     * @param aggregationFieldsDescriptor The descriptor of the aggregation field in the sliding
     *     window.
     * @param zeroValuedRow If the zeroValuedRow is not null, the sliding window will output a row
     *     with default value when the window is empty. The zeroValuedRow contains zero values of
     *     the all the fields, except row time field and key fields.
     * @param skipSameWindowOutput Whether to output if the sliding window output the same result.
     */
    public static Table applySlidingWindow(
            StreamTableEnvironment tEnv,
            Table table,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            Row zeroValuedRow,
            boolean skipSameWindowOutput) {
        ResolvedSchema schema = table.getResolvedSchema();
        DataStream<Row> stream =
                tEnv.toChangelogStream(
                        table,
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        ChangelogMode.all());

        final OutputTag<Row> outputTag = new OutputTag<Row>("late-data-side-output") {};

        TimeZone timeZone = TimeZone.getTimeZone(tEnv.getConfig().getLocalTimeZone());

        long offset = getModdedOffset(stepSizeMs, -timeZone.getRawOffset());
        SingleOutputStreamOperator<Row> outputStream =
                stream.keyBy(new RowKeySelector(keyFieldNames))
                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.milliseconds(stepSizeMs), Time.milliseconds(offset)))
                        .sideOutputLateData(outputTag)
                        .apply(
                                new RowWindowFunction(
                                        aggregationFieldsDescriptor,
                                        keyFieldNames,
                                        rowTimeFieldName));

        DataStream<Row> lateDataStream =
                outputStream
                        .getSideOutput(outputTag)
                        .map(
                                new LateDataToPreAggResultMapFunction(
                                        aggregationFieldsDescriptor,
                                        keyFieldNames,
                                        rowTimeFieldName,
                                        stepSizeMs,
                                        offset));

        DataStream<Row> outputStreamWithLateData = outputStream.union(lateDataStream);

        Map<String, DataType> dataTypeMap = new HashMap<>();
        for (String key : keyFieldNames) {
            dataTypeMap.put(key, getDataType(schema, key));
        }
        dataTypeMap.put(rowTimeFieldName, getDataType(schema, rowTimeFieldName));
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            dataTypeMap.put(descriptor.outFieldName, descriptor.outDataType);
        }

        return applySlidingWindowKeyedProcessFunction(
                tEnv,
                outputStreamWithLateData,
                dataTypeMap,
                keyFieldNames,
                rowTimeFieldName,
                stepSizeMs,
                aggregationFieldsDescriptor,
                zeroValuedRow,
                skipSameWindowOutput);
    }

    private static Table applySlidingWindowKeyedProcessFunction(
            StreamTableEnvironment tEnv,
            DataStream<Row> rowDataStream,
            Map<String, DataType> dataTypeMap,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            Row zeroValuedRow,
            boolean skipSameWindowOutput) {
        final TypeSerializer<Row> rowTypeSerializer =
                rowDataStream.getType().createSerializer(new ExecutionConfig());

        final List<DataTypes.Field> resultTableFields =
                getResultTableFields(
                        dataTypeMap, aggregationFieldsDescriptor, rowTimeFieldName, keyFieldNames);
        final List<String> resultTableFieldNames =
                resultTableFields.stream()
                        .map(DataTypes.AbstractField::getName)
                        .collect(Collectors.toList());
        final List<DataType> resultTableFieldDataTypes =
                resultTableFields.stream()
                        .map(DataTypes.Field::getDataType)
                        .collect(Collectors.toList());
        final ExternalTypeInfo<Row> resultRowTypeInfo =
                ExternalTypeInfo.of(DataTypes.ROW(resultTableFields));

        PostSlidingWindowZeroValuedRowExpiredRowHandler expiredRowHandler = null;
        if (zeroValuedRow != null) {
            expiredRowHandler =
                    new PostSlidingWindowZeroValuedRowExpiredRowHandler(
                            updateZeroValuedRow(
                                    zeroValuedRow,
                                    resultTableFieldNames,
                                    resultTableFieldDataTypes),
                            rowTimeFieldName,
                            keyFieldNames);
        }
        rowDataStream =
                rowDataStream
                        .keyBy(new RowKeySelector(keyFieldNames))
                        //                        .connect(lateDataStream.keyBy(new
                        // RowKeySelector(keyFieldNames)))
                        .process(
                                new SlidingWindowKeyedCoProcessFunction(
                                        aggregationFieldsDescriptor,
                                        rowTypeSerializer,
                                        resultRowTypeInfo.createSerializer(null),
                                        keyFieldNames,
                                        rowTimeFieldName,
                                        stepSizeMs,
                                        expiredRowHandler,
                                        skipSameWindowOutput))
                        .returns(resultRowTypeInfo);

        Table table =
                tEnv.fromDataStream(
                        rowDataStream,
                        getResultTableSchema(
                                dataTypeMap,
                                aggregationFieldsDescriptor,
                                rowTimeFieldName,
                                keyFieldNames));
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            table =
                    table.addOrReplaceColumns(
                            $(aggregationFieldDescriptor.outFieldName)
                                    .cast(aggregationFieldDescriptor.outDataType)
                                    .as(aggregationFieldDescriptor.outFieldName));
        }
        return table;
    }

    public static Row updateZeroValuedRow(
            Row zeroValuedRow, List<String> fieldNames, List<DataType> fieldDataType) {
        for (String fieldName : Objects.requireNonNull(zeroValuedRow.getFieldNames(true))) {
            final Object zeroValue = zeroValuedRow.getFieldAs(fieldName);
            if (zeroValue == null) {
                continue;
            }

            final int idx = fieldNames.indexOf(fieldName);
            if (idx == -1) {
                throw new RuntimeException(
                        String.format(
                                "The given default value of field %s doesn't exist.", fieldName));
            }
            final DataType dataType = fieldDataType.get(idx);
            final LogicalTypeRoot zeroValueType = dataType.getLogicalType().getTypeRoot();

            // Integer value pass as Integer type with PY4J from python to Java if the value is less
            // than Integer.MAX_VALUE. Floating point value pass as Double from python to Java.
            // Therefore, we need to cast to the corresponding data type of the column.
            switch (zeroValueType) {
                case INTEGER:
                case DOUBLE:
                    break;
                case BIGINT:
                    if (zeroValue instanceof Number) {
                        final Number numberValue = (Number) zeroValue;
                        zeroValuedRow.setField(fieldName, numberValue.longValue());
                        break;
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for BIGINT column.",
                                        zeroValue.getClass().getName()));
                    }
                case FLOAT:
                    if (zeroValue instanceof Number) {
                        final Number numberValue = (Number) zeroValue;
                        zeroValuedRow.setField(fieldName, numberValue.floatValue());
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unknown default value type %s for FLOAT column.",
                                        zeroValue.getClass().getName()));
                    }
                    break;
                default:
                    throw new RuntimeException(
                            String.format("Unknown default value type %s", zeroValueType));
            }
        }
        return zeroValuedRow;
    }

    private static List<DataTypes.Field> getResultTableFields(
            Map<String, DataType> dataTypeMap,
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        List<DataTypes.Field> keyFields =
                Arrays.stream(keyFieldNames)
                        .map(fieldName -> DataTypes.FIELD(fieldName, dataTypeMap.get(fieldName)))
                        .collect(Collectors.toList());
        List<DataTypes.Field> aggFieldDataTypes =
                aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                        .map(d -> DataTypes.FIELD(d.outFieldName, d.preAggFunc.getResultDatatype()))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> fields = new LinkedList<>();
        fields.addAll(keyFields);
        fields.addAll(aggFieldDataTypes);
        fields.add(DataTypes.FIELD(rowTimeFieldName, dataTypeMap.get(rowTimeFieldName)));
        return fields;
    }

    private static long getModdedOffset(long stepSizeMs, long offsetMs) {
        offsetMs %= stepSizeMs;
        if (offsetMs < 0) {
            offsetMs += stepSizeMs;
        }
        return offsetMs;
    }

    private static class RowKeySelector implements KeySelector<Row, Row> {
        private final String[] keyFieldNames;

        private RowKeySelector(String[] keyFieldNames) {
            this.keyFieldNames = keyFieldNames;
        }

        @Override
        public Row getKey(Row row) {
            return Row.of(Arrays.stream(keyFieldNames).map(row::getField).toArray());
        }
    }

    private static class RowWindowFunction implements WindowFunction<Row, Row, Row, TimeWindow> {
        private final AggregationFieldsDescriptor fieldsDescriptor;
        private final String[] keyFieldNames;
        private final String rowTimeFieldName;

        private RowWindowFunction(
                AggregationFieldsDescriptor fieldsDescriptor,
                String[] keyFieldNames,
                String rowTimeFieldName) {
            this.fieldsDescriptor = fieldsDescriptor;
            this.keyFieldNames = keyFieldNames;
            this.rowTimeFieldName = rowTimeFieldName;
        }

        @Override
        public void apply(
                Row keys, TimeWindow window, Iterable<Row> iterable, Collector<Row> collector) {
            Row acc = Row.withNames();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor fieldDescriptor :
                    fieldsDescriptor.getAggFieldDescriptors()) {
                acc.setField(
                        fieldDescriptor.outFieldName,
                        fieldDescriptor.preAggFunc.createAccumulator());
            }

            for (Row row : iterable) {
                long timestamp = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();
                for (AggregationFieldsDescriptor.AggregationFieldDescriptor fieldDescriptor :
                        fieldsDescriptor.getAggFieldDescriptors()) {
                    fieldDescriptor.preAggFunc.add(
                            acc.getField(fieldDescriptor.outFieldName),
                            row.getField(fieldDescriptor.inFieldName),
                            timestamp);
                }
            }

            for (AggregationFieldsDescriptor.AggregationFieldDescriptor fieldDescriptor :
                    fieldsDescriptor.getAggFieldDescriptors()) {
                Object accumulator = acc.getField(fieldDescriptor.outFieldName);
                acc.setField(
                        fieldDescriptor.outFieldName,
                        fieldDescriptor.preAggFunc.getResult(accumulator));
                //                fieldDescriptor.preAggFunc.add(
                //                        acc.getField(fieldDescriptor.outFieldName),
                //                        row.getField(fieldDescriptor.inFieldName),
                //                        timestamp);
            }

            acc.setField(rowTimeFieldName, window.getEnd() - 1);

            for (int i = 0; i < keyFieldNames.length; i++) {
                acc.setField(keyFieldNames[i], keys.getField(i));
            }

            collector.collect(acc);
        }
    }

    private static class LateDataToPreAggResultMapFunction implements MapFunction<Row, Row> {
        private final AggregationFieldsDescriptor fieldsDescriptor;
        private final String[] keyFieldNames;
        private final String rowTimeFieldName;
        private final long step_size;
        private final long offset;

        private LateDataToPreAggResultMapFunction(
                AggregationFieldsDescriptor fieldsDescriptor,
                String[] keyFieldNames,
                String rowTimeFieldName,
                long step_size,
                long offset) {
            this.fieldsDescriptor = fieldsDescriptor;
            this.keyFieldNames = keyFieldNames;
            this.rowTimeFieldName = rowTimeFieldName;
            this.step_size = step_size;
            this.offset = offset;
        }

        @Override
        public Row map(Row row) throws Exception {
            long timestamp = ((Instant) row.getFieldAs(rowTimeFieldName)).toEpochMilli();
            Row result = Row.withNames();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor fieldDescriptor :
                    fieldsDescriptor.getAggFieldDescriptors()) {
                result.setField(
                        fieldDescriptor.outFieldName,
                        fieldDescriptor.preAggFunc.getResult(
                                row.getField(fieldDescriptor.inFieldName), timestamp));
            }

            result.setField(rowTimeFieldName, getWindowTime(timestamp));

            for (int i = 0; i < keyFieldNames.length; i++) {
                result.setField(keyFieldNames[i], row.getField(i));
            }
            return result;
        }

        private long getWindowTime(long timestamp) {
            long start =
                    TimeWindow.getWindowStartWithOffset(
                            timestamp, this.offset % this.step_size, this.step_size);
            return start + step_size - 1;
        }
    }

    private static DataType getDataType(ResolvedSchema resolvedSchema, String fieldName) {
        return resolvedSchema
                .getColumn(fieldName)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        String.format("Cannot find column %s.", fieldName)))
                .getDataType();
    }

    private static Schema getResultTableSchema(
            Map<String, DataType> dataTypeMap,
            AggregationFieldsDescriptor descriptor,
            String rowTimeFieldName,
            String[] keyFieldNames) {
        final Schema.Builder builder = Schema.newBuilder();

        for (String keyFieldName : keyFieldNames) {
            builder.column(keyFieldName, dataTypeMap.get(keyFieldName).notNull());
        }

        if (keyFieldNames.length > 0) {
            builder.primaryKey(keyFieldNames);
        }

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggregationFieldDescriptor :
                descriptor.getAggFieldDescriptors()) {
            builder.column(
                    aggregationFieldDescriptor.outFieldName,
                    aggregationFieldDescriptor.preAggFunc.getResultDatatype());
        }

        builder.column(rowTimeFieldName, dataTypeMap.get(rowTimeFieldName));

        // Records are ordered by row time after sliding window.
        builder.watermark(
                rowTimeFieldName,
                String.format("`%s` - INTERVAL '0.001' SECONDS", rowTimeFieldName));
        return builder.build();
    }
}
