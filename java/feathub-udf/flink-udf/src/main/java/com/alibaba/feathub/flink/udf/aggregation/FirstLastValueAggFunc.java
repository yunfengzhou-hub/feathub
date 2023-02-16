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

import org.apache.flink.table.types.DataType;

/**
 * Aggregate function that get the first value or last value.
 *
 * <p>It assumes that the values are ordered by time.
 */
public class FirstLastValueAggFunc<T> extends AbstractRawDataAggFunc<T, T> {
    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValueAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public T getResult(RawDataAccumulator accumulator) {
        if (accumulator.rawDatas.isEmpty()) {
            return null;
        }
        if (isFirstValue) {
            return (T) accumulator.rawDatas.getFirst().f0;
        } else {
            return (T) accumulator.rawDatas.getLast().f0;
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }
}
