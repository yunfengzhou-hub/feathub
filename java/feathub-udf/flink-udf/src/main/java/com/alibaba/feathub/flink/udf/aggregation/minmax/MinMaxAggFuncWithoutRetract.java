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

package com.alibaba.feathub.flink.udf.aggregation.minmax;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFuncWithoutRetract;

/**
 * Aggregate function that get the min or max.
 *
 * <p>Implementation is optimized based on the assumption that no retraction is required.
 */
public class MinMaxAggFuncWithoutRetract<T extends Comparable<T>>
        extends AggFuncWithoutRetract<T, T, T> {

    private final DataType inDataType;
    private final boolean isMin;

    public MinMaxAggFuncWithoutRetract(DataType inDataType, boolean isMin) {
        this.inDataType = inDataType;
        this.isMin = isMin;
    }

    @Override
    public T add(T accumulator, T value, long timestamp) {
        if (accumulator == null
                || (isMin && accumulator.compareTo(value) > 0)
                || (!isMin && accumulator.compareTo(value) < 0)) {
            return value;
        } else {
            return accumulator;
        }
    }

    @Override
    public T merge(T target, T source) {
        return add(target, source, -1L);
    }

    @Override
    public T getResult(T accumulator) {
        return accumulator;
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
    public T createAccumulator() {
        return null;
    }

    @Override
    public TypeInformation<T> getAccumulatorTypeInformation() {
        return ExternalTypeInfo.of(inDataType);
    }
}
