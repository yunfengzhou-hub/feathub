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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;

/** Aggregation function that counts the number of values. */
public class CountAggFunc implements AggFunc<Long, Long, CountAccumulator> {

    @Override
    public void add(CountAccumulator accumulator, Long value, long timestamp) {
        accumulator.cnt += value;
    }

    @Override
    public void retract(CountAccumulator accumulator, Long value) {
        accumulator.cnt -= value;
    }

    @Override
    public Long getResult(CountAccumulator accumulator) {
        return accumulator.cnt;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.BIGINT();
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    @Override
    public TypeInformation<CountAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(CountAccumulator.class);
    }
}
