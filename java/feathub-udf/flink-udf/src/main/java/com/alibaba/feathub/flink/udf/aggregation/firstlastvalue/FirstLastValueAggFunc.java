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

package com.alibaba.feathub.flink.udf.aggregation.firstlastvalue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.AbstractRawDataAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>It assumes that the values are ordered by time.
 */
public class FirstLastValueAggFunc<T>
        implements AggFunc<Row, T, AbstractRawDataAggFunc.RawDataAccumulator<T>> {
    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public void add(
            AbstractRawDataAggFunc.RawDataAccumulator<T> accumulator, Row row, long timestamp) {
        timestamp = row.getFieldAs("timestamp");
        T value = row.getFieldAs("value");

        if (accumulator.rawDatas.isEmpty() || timestamp >= accumulator.rawDatas.getLast().f1) {
            accumulator.rawDatas.add(Tuple2.of(value, timestamp));
            return;
        }

        int index = -1;
        for (Tuple2<T, Long> tuple2 : accumulator.rawDatas) {
            index++;
            if (tuple2.f1 > timestamp) {
                accumulator.rawDatas.add(index, Tuple2.of(value, timestamp));
                return;
            }
        }

        throw new RuntimeException(
                String.format(
                        "Cannot add value with timestamp %s to RawDataAccumulator.", timestamp));
    }

    @Override
    public void retract(
            AbstractRawDataAggFunc.RawDataAccumulator<T> accumulator, Row row, long timestamp) {
        timestamp = row.getFieldAs("timestamp");
        T value = row.getFieldAs("value");

        Tuple2<T, Long> tupleToRetract = Tuple2.of(value, timestamp);
        int index = -1;
        for (Tuple2<T, Long> tuple2 : accumulator.rawDatas) {
            index++;
            if (tuple2.equals(tupleToRetract)) {
                accumulator.rawDatas.remove(index);
                return;
            }
        }

        throw new RuntimeException(
                "Value and timestamp "
                        + tupleToRetract
                        + " cannot be found in RawDataAccumulator.");
    }

    @Override
    public T getResult(AbstractRawDataAggFunc.RawDataAccumulator<T> accumulator) {
        if (accumulator.rawDatas.isEmpty()) {
            return null;
        }
        if (isFirstValue) {
            return accumulator.rawDatas.getFirst().f0;
        } else {
            return accumulator.rawDatas.getLast().f0;
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public AbstractRawDataAggFunc.RawDataAccumulator<T> createAccumulator() {
        return new AbstractRawDataAggFunc.RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(AbstractRawDataAggFunc.RawDataAccumulator.class);
    }
}
