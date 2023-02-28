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

package com.alibaba.feathub.flink.udf.aggregation.avg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

/** Aggregation function that calculates the average of rows with sum and count. */
public class AvgAggFunc implements AggFunc<Row, Double, AvgAggFunc.AvgAccumulator> {

    private final DataType valueType;

    public AvgAggFunc(DataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public void add(AvgAccumulator accumulator, Row value, long timestamp) {
        if (accumulator.avgRow == null) {
            accumulator.avgRow = value;
            return;
        }

        mergeRow(accumulator, value);
    }

    @Override
    public void retract(AvgAccumulator accumulator, Row value) {
        Object sum1 = accumulator.avgRow.getField(0);
        Object sum2 = value.getField(0);
        Long cnt1 = accumulator.avgRow.getFieldAs(1);
        Long cnt2 = value.getFieldAs(1);

        if (sum1 == null || sum2 == null || cnt1 == null || cnt2 == null) {
            throw new RuntimeException("sum and count cannot be null.");
        }

        if (cnt1.equals(cnt2)) {
            accumulator.avgRow = null;
            return;
        }

        if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            accumulator.avgRow = Row.of((Integer) sum1 - (Integer) sum2, cnt1 - cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            accumulator.avgRow = Row.of((Long) sum1 - (Long) sum2, cnt1 - cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            accumulator.avgRow = Row.of((Float) sum1 - (Float) sum2, cnt1 - cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            accumulator.avgRow = Row.of((Double) sum1 - (Double) sum2, cnt1 - cnt2);
        } else {
            throw new RuntimeException(
                    String.format("Unsupported type for AvgAggregationFunction %s.", valueType));
        }
    }

    private void mergeRow(AvgAccumulator accumulator, Row value) {
        Object sum1 = accumulator.avgRow.getField(0);
        Object sum2 = value.getField(0);
        Long cnt1 = accumulator.avgRow.getFieldAs(1);
        Long cnt2 = value.getFieldAs(1);

        if (sum1 == null || sum2 == null || cnt1 == null || cnt2 == null) {
            throw new RuntimeException("sum and count cannot be null.");
        }

        if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            accumulator.avgRow = Row.of((Integer) sum1 + (Integer) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            accumulator.avgRow = Row.of((Long) sum1 + (Long) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            accumulator.avgRow = Row.of((Float) sum1 + (Float) sum2, cnt1 + cnt2);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            accumulator.avgRow = Row.of((Double) sum1 + (Double) sum2, cnt1 + cnt2);
        } else {
            throw new RuntimeException(
                    String.format("Unsupported type for AvgAggregationFunction %s.", valueType));
        }
    }

    @Override
    public Double getResult(AvgAccumulator accumulator) {
        if (accumulator.avgRow == null) {
            return null;
        }
        Object sum = accumulator.avgRow.getField(0);
        Long cnt = accumulator.avgRow.getFieldAs(1);

        if (sum == null || cnt == null) {
            throw new RuntimeException("sum and count cannot be null.");
        }

        if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            return ((Integer) sum) * 1.0 / cnt;
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            return ((Long) sum) * 1.0 / cnt;
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            return (double) (((Float) sum) / cnt);
        } else if (valueType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            return ((Double) sum) / cnt;
        } else {
            throw new RuntimeException(
                    String.format("Unsupported type for AvgAggregationFunction %s.", valueType));
        }
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.DOUBLE();
    }

    @Override
    public AvgAccumulator createAccumulator() {
        return new AvgAccumulator();
    }

    @Override
    public TypeInformation<AvgAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(AvgAccumulator.class);
    }

    /** Accumulator for {@link AvgAggFunc}. */
    public static class AvgAccumulator {
        public Row avgRow = null;
    }
}
