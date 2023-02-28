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
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

import java.util.HashMap;
import java.util.Map;

/** The {@link PreAggFunc} for {@link ValueCountsAggFunc}. */
public class ValueCountsPreAggFunc
        implements PreAggFunc<Object, Map<Object, Long>, Map<Object, Long>> {
    private final DataType inDataType;

    public ValueCountsPreAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public Map<Object, Long> getResult(Map<Object, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Map<Object, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public void add(Map<Object, Long> acc, Object value, long timestamp) {
        acc.put(value, acc.getOrDefault(value, 0L) + 1);
    }

    @Override
    public void merge(Map<Object, Long> target, Map<Object, Long> source) {
        for (Map.Entry<Object, Long> entry : source.entrySet()) {
            target.put(entry.getKey(), target.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
    }

    @Override
    public TypeInformation getResultTypeInformation() {
        return Types.MAP(ExternalTypeInfo.of(inDataType), Types.LONG);
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return getResultTypeInformation();
    }
}
