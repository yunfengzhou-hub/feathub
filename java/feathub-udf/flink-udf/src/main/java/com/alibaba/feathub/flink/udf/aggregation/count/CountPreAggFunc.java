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

package com.alibaba.feathub.flink.udf.aggregation.count;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

/** The {@link PreAggFunc} for {@link CountAggFunc}. */
public class CountPreAggFunc implements PreAggFunc<Object, Long, CountAccumulator> {
    @Override
    public void add(CountAccumulator accumulator, Object value, long timestamp) {
        accumulator.cnt += 1;
    }

    @Override
    public void merge(CountAccumulator target, CountAccumulator source) {
        target.cnt += source.cnt;
    }

    @Override
    public Long getResult(CountAccumulator accumulator) {
        return accumulator.cnt;
    }

    @Override
    public TypeInformation<Long> getResultTypeInformation() {
        return Types.LONG;
    }

    @Override
    public TypeInformation<CountAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(CountAccumulator.class);
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }
}
