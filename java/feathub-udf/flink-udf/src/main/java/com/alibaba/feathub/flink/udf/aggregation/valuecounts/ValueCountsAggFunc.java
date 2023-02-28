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

package com.alibaba.feathub.flink.udf.aggregation.valuecounts;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

import java.util.HashMap;
import java.util.Map;

/** Aggregate function that merge value counts. */
public class ValueCountsAggFunc
        implements AggFunc<
                Map<Object, Long>, Map<Object, Long>, ValueCountsAggFunc.ValueCountsAccumulator> {
    private final DataType inDataType;

    public ValueCountsAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void add(ValueCountsAccumulator accumulator, Map<Object, Long> value, long timestamp) {
        for (Map.Entry<Object, Long> entry : value.entrySet()) {
            final Object key = entry.getKey();
            accumulator.valueCounts.put(
                    key, accumulator.valueCounts.getOrDefault(key, 0L) + entry.getValue());
        }
    }

    @Override
    public void retract(ValueCountsAccumulator accumulator, Map<Object, Long> value) {
        for (Map.Entry<Object, Long> entry : value.entrySet()) {
            final Object key = entry.getKey();
            long newCnt = accumulator.valueCounts.get(key) - entry.getValue();
            if (newCnt == 0) {
                accumulator.valueCounts.remove(key);
                return;
            }
            accumulator.valueCounts.put(key, newCnt);
        }
    }

    @Override
    public Map<Object, Long> getResult(ValueCountsAccumulator accumulator) {
        if (accumulator.valueCounts.isEmpty()) {
            return null;
        }
        return accumulator.valueCounts;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.MAP(inDataType, DataTypes.BIGINT());
    }

    @Override
    public ValueCountsAccumulator createAccumulator() {
        return new ValueCountsAccumulator();
    }

    @Override
    public TypeInformation<ValueCountsAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(ValueCountsAccumulator.class);
    }

    /** Accumulator for {@link ValueCountsAggFunc}. */
    public static class ValueCountsAccumulator {
        public final Map<Object, Long> valueCounts = new HashMap<>();
    }
}
