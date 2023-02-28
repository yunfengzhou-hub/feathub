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
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithoutRetract;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>Implementation is optimized based on the assumption that no retraction is required.
 */
public class FirstLastValueAggFuncWithoutRetract<T>
        extends AggFuncWithoutRetract<T, T, Tuple2<T, Long>> {
    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFuncWithoutRetract(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public T getResult(Tuple2<T, Long> acc) {
        return acc.f0;
    }

    @Override
    public Tuple2<T, Long> createAccumulator() {
        return Tuple2.of(null, Long.MIN_VALUE);
    }

    @Override
    public Tuple2<T, Long> add(Tuple2<T, Long> acc, T value, long timestamp) {
        Preconditions.checkState(
                timestamp > acc.f1,
                "The value to the FirstLastValuePreAggFunc must be ordered by timestamp.");

        if (!isFirstValue || acc.f1 == Long.MIN_VALUE) {
            acc.f0 = value;
            acc.f1 = timestamp;
        }
        return acc;
    }

    @Override
    public Tuple2<T, Long> merge(Tuple2<T, Long> target, Tuple2<T, Long> source) {
        if (source.f1 == Long.MIN_VALUE) {
            return target;
        }

        if (target.f1 == Long.MIN_VALUE) {
            target.f0 = source.f0;
            target.f1 = source.f1;
            return target;
        }

        if ((isFirstValue && target.f1 > source.f1) || (!isFirstValue && target.f1 < source.f1)) {
            target.f0 = source.f0;
            target.f1 = source.f1;
        }

        return target;
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public TypeInformation getResultTypeInformation() {
        return ExternalTypeInfo.of(inDataType);
    }

    @Override
    public TypeInformation<Tuple2<T, Long>> getAccumulatorTypeInformation() {
        return Types.TUPLE(getResultTypeInformation(), Types.LONG);
    }
}
