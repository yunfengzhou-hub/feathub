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

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.DataType;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>Implementation is optimized based on the assumption that no retraction is required.
 */
public class FirstLastValueAggFuncWithoutRetract<T>
        implements AggFunc<T, T, AbstractRawDataAggFunc.RawDataAccumulator<T>> {
    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFuncWithoutRetract(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public T getResult(AbstractRawDataAggFunc.RawDataAccumulator<T> acc) {
        if (acc.rawDataList.isEmpty()) {
            return null;
        }
        return acc.rawDataList.getLast().f0;
    }

    @Override
    public void add(AbstractRawDataAggFunc.RawDataAccumulator<T> acc, T value, long timestamp) {
        if (acc.rawDataList.isEmpty()
                || (!isFirstValue && timestamp > acc.rawDataList.getLast().f1)
                || (isFirstValue && timestamp < acc.rawDataList.getLast().f1)) {
            acc.rawDataList.clear();
            acc.rawDataList.add(Tuple2.of(value, timestamp));
        }
    }

    @Override
    public void merge(
            AbstractRawDataAggFunc.RawDataAccumulator<T> target,
            AbstractRawDataAggFunc.RawDataAccumulator<T> source) {
        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return;
        }

        if (source.rawDataList.isEmpty()) {
            return;
        }

        long timestamp0 = target.rawDataList.getLast().f1;
        long timestamp1 = source.rawDataList.getLast().f1;

        if ((isFirstValue && timestamp0 > timestamp1)
                || (!isFirstValue && timestamp0 < timestamp1)) {
            target.rawDataList.clear();
            target.rawDataList.addAll(source.rawDataList);
        }
    }

    @Override
    public void retract(AbstractRawDataAggFunc.RawDataAccumulator<T> accumulator, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void retractAccumulator(
            AbstractRawDataAggFunc.RawDataAccumulator<T> target,
            AbstractRawDataAggFunc.RawDataAccumulator<T> source) {
        throw new UnsupportedOperationException();
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
