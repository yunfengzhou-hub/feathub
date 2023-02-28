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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;

import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValueAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValuePreAggFunc;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FirstLastValueAggFunc} and {@link FirstLastValuePreAggFunc}. */
class FirstLastValueAggFuncTest {
    @Test
    void tesFirstValuePreAggFunc() {
        final FirstLastValuePreAggFunc<Integer> preAggFunc =
                new FirstLastValuePreAggFunc<>(DataTypes.INT(), true);
        Tuple2<Integer, Long> accumulator = preAggFunc.createAccumulator();
        preAggFunc.add(accumulator, 0, 0);
        preAggFunc.add(accumulator, 1, 1);
        preAggFunc.add(accumulator, 2, 2);
        preAggFunc.add(accumulator, 3, 3);
        assertThat(preAggFunc.getResult(accumulator)).isEqualTo(0);
    }

    @Test
    void tesFirstValue() {
        final FirstLastValueAggFunc<Integer> aggFunc =
                new FirstLastValueAggFunc<>(DataTypes.INT(), true);
        final FirstLastValueAggFunc.FirstLastValueAccumulator accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 1, 1);
        aggFunc.add(accumulator, 2, 2);
        aggFunc.add(accumulator, 3, 3);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(0);
        aggFunc.retract(accumulator, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(1);
    }

    @Test
    void tesLastValuePreAggFunc() {
        final FirstLastValuePreAggFunc<Integer> preAggFunc =
                new FirstLastValuePreAggFunc<>(DataTypes.INT(), false);
        Tuple2<Integer, Long> accumulator = preAggFunc.createAccumulator();
        preAggFunc.add(accumulator, 0, 0);
        preAggFunc.add(accumulator, 1, 1);
        preAggFunc.add(accumulator, 2, 2);
        preAggFunc.add(accumulator, 3, 3);
        assertThat(preAggFunc.getResult(accumulator)).isEqualTo(3);
    }

    @Test
    void testLastValue() {
        final FirstLastValueAggFunc<Integer> aggFunc =
                new FirstLastValueAggFunc<>(DataTypes.INT(), false);
        final FirstLastValueAggFunc.FirstLastValueAccumulator accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNull();
        aggFunc.add(accumulator, 0, 0);
        aggFunc.add(accumulator, 1, 1);
        aggFunc.add(accumulator, 2, 2);
        aggFunc.add(accumulator, 3, 3);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
        aggFunc.retract(accumulator, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
    }
}
