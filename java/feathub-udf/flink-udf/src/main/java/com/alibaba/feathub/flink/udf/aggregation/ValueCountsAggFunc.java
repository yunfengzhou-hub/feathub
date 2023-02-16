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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** Aggregate function that merge value counts. */
public class ValueCountsAggFunc
        implements AggFunc<Object, Map<Object, Long>, ValueCountsAggFunc.ValueCountsAccumulator> {
    private final DataType inDataType;

    public ValueCountsAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void add(ValueCountsAccumulator accumulator, Object value, long timestamp) {
        accumulator.valueCounts.put(value, accumulator.valueCounts.getOrDefault(value, 0L) + 1);
    }

    @Override
    public void retract(ValueCountsAccumulator accumulator, Object value, long timestamp) {
        long newCnt = accumulator.valueCounts.get(value) - 1;
        if (newCnt == 0) {
            accumulator.valueCounts.remove(value);
            return;
        }
        accumulator.valueCounts.put(value, newCnt);
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
    public void mergeAccumulator(ValueCountsAccumulator target, ValueCountsAccumulator source) {
        for (Map.Entry<Object, Long> entry : source.valueCounts.entrySet()) {
            target.valueCounts.put(
                    entry.getKey(),
                    target.valueCounts.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
    }

    @Override
    public void retractAccumulator(ValueCountsAccumulator target, ValueCountsAccumulator source) {
        for (Map.Entry<Object, Long> entry : source.valueCounts.entrySet()) {
            long newCnt = target.valueCounts.get(entry.getKey()) - entry.getValue();
            if (newCnt == 0) {
                target.valueCounts.remove(entry.getKey());
            } else {
                target.valueCounts.put(entry.getKey(), newCnt);
            }
        }
    }

    @Override
    public TypeInformation<ValueCountsAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(ValueCountsAccumulator.class);
    }

    /** Accumulator for {@link ValueCountsAccumulator}. */
    public static class ValueCountsAccumulator {
        public final Map<Object, Long> valueCounts = new HashMap<>();
    }
}
