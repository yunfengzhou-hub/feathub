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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

import java.util.Map;
import java.util.TreeMap;

/** Aggregate function that get the min or max. */
public class MinMaxAggFunc<T extends Comparable<T>> implements AggFunc<T, T, TreeMap<T, Long>> {

    private final DataType inDataType;
    private final boolean isMin;

    public MinMaxAggFunc(DataType inDataType, boolean isMin) {
        this.inDataType = inDataType;
        this.isMin = isMin;
    }

    @Override
    public TreeMap<T, Long> add(TreeMap<T, Long> accumulator, T value, long timestamp) {
        accumulator.put(value, accumulator.getOrDefault(value, 0L) + 1);
        return accumulator;
    }

    @Override
    public TreeMap<T, Long> merge(TreeMap<T, Long> target, TreeMap<T, Long> source) {
        for (Map.Entry<T, Long> entry : source.entrySet()) {
            target.put(entry.getKey(), target.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
        return target;
    }

    @Override
    public TreeMap<T, Long> retract(TreeMap<T, Long> accumulator, T value) {
        final long newCnt = accumulator.get(value) - 1;
        if (newCnt == 0) {
            accumulator.remove(value);
        } else {
            accumulator.put(value, newCnt);
        }
        return accumulator;
    }

    @Override
    public TreeMap<T, Long> retractAccumulator(TreeMap<T, Long> target, TreeMap<T, Long> source) {
        for (Map.Entry<T, Long> entry : source.entrySet()) {
            final long newCnt = target.get(entry.getKey()) - entry.getValue();
            if (newCnt == 0) {
                target.remove(entry.getKey());
                continue;
            }
            target.put(entry.getKey(), newCnt);
        }
        return target;
    }

    @Override
    public T getResult(TreeMap<T, Long> accumulator) {
        if (accumulator.isEmpty()) {
            return null;
        }

        if (isMin) {
            return accumulator.firstKey();
        } else {
            return accumulator.lastKey();
        }
    }

    @Override
    public DataType getResultDatatype() {
        return inDataType;
    }

    @Override
    public TreeMap<T, Long> createAccumulator() {
        return new TreeMap<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.MAP(ExternalTypeInfo.of(inDataType), Types.LONG);
    }
}
