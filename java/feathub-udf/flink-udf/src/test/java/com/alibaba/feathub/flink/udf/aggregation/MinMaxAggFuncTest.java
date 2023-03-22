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

import org.apache.flink.table.api.DataTypes;

import com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFunc;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MinMaxAggFunc}. */
class MinMaxAggFuncTest {
    @Test
    void testMax() {
        final MinMaxAggFunc<Integer> aggFunc = new MinMaxAggFunc<>(DataTypes.INT(), false);
        final TreeMap<Integer, Long> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 1, 0);
        aggFunc.add(accumulator, 3, 0);
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 4, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(4);
        aggFunc.retract(accumulator, 4);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
    }

    @Test
    void testMin() {
        final MinMaxAggFunc<Integer> aggFunc = new MinMaxAggFunc<>(DataTypes.INT(), true);
        final TreeMap<Integer, Long> accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 1, 0);
        aggFunc.add(accumulator, 3, 0);
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 4, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(0);
        aggFunc.retract(accumulator, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(1);
    }
}
