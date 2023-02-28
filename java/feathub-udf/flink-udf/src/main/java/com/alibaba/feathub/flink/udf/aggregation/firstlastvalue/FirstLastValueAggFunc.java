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
import org.apache.flink.util.Preconditions;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

import java.util.LinkedList;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>It assumes that the values are ordered by time.
 */
public class FirstLastValueAggFunc<T>
        implements AggFunc<T, T, FirstLastValueAggFunc.RawDataAccumulator<T>> {

    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public RawDataAccumulator<T> add(RawDataAccumulator<T> acc, T value, long timestamp) {
        if (acc.rawDataList.isEmpty() || timestamp >= acc.rawDataList.getLast().f1) {
            acc.rawDataList.add(Tuple2.of(value, timestamp));
            return acc;
        }

        int index = -1;
        for (Tuple2<T, Long> tuple2 : acc.rawDataList) {
            index++;
            if (tuple2.f1 > timestamp) {
                break;
            }
        }

        acc.rawDataList.add(index, Tuple2.of(value, timestamp));
        return acc;
    }

    @Override
    public RawDataAccumulator<T> merge(RawDataAccumulator<T> target, RawDataAccumulator<T> source) {
        if (source.rawDataList.isEmpty()) {
            return target;
        }

        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return target;
        }

        if (target.rawDataList.getLast().f1 > source.rawDataList.getLast().f1) {
            LinkedList<Tuple2<T, Long>> tmpList = target.rawDataList;
            target.rawDataList = new LinkedList<>();
            target.rawDataList.addAll(source.rawDataList);
            target.rawDataList.addAll(tmpList);
        } else {
            target.rawDataList.addAll(source.rawDataList);
        }

        return target;
    }

    @Override
    public RawDataAccumulator<T> retract(RawDataAccumulator<T> accumulator, T value) {
        Preconditions.checkState(
                accumulator.rawDataList.getFirst().f0.equals(value),
                "Value must be retracted by the ordered as they added to the FirstLastValueAggFuncBase.");
        accumulator.rawDataList.removeFirst();
        return accumulator;
    }

    @Override
    public RawDataAccumulator<T> retractAccumulator(
            RawDataAccumulator<T> target, RawDataAccumulator<T> source) {
        for (Tuple2<T, Long> value : source.rawDataList) {
            Preconditions.checkState(
                    target.rawDataList.getFirst().equals(value),
                    "Value must be retracted by the order as they added to the AggFuncWithLimit.");
            target.rawDataList.removeFirst();
        }
        return target;
    }

    @Override
    public T getResult(RawDataAccumulator<T> accumulator) {
        if (accumulator.rawDataList.isEmpty()) {
            return null;
        }
        if (isFirstValue) {
            return accumulator.rawDataList.getFirst().f0;
        } else {
            return accumulator.rawDataList.getLast().f0;
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public RawDataAccumulator<T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }

    /** Accumulator that collects raw data and their timestamps. */
    public static class RawDataAccumulator<T> {
        public LinkedList<Tuple2<T, Long>> rawDataList = new LinkedList<>();
    }
}
