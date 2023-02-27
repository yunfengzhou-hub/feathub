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

import com.alibaba.feathub.flink.udf.aggregation.avg.AvgAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.DoubleSumAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.FloatSumAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.IntSumAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.LongSumAggFunc;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AvgAggFunc}. */
class AvgAggFuncTest {
    @Test
    void testAvgAggregationFunctions() {
        innerTest(Arrays.array(1, 2, 3, 4), 2.5, 3.0, new AvgAggFunc<>(new IntSumAggFunc()));
        innerTest(Arrays.array(1L, 2L, 3L), 2.0, 2.5, new AvgAggFunc<>(new LongSumAggFunc()));
        innerTest(
                Arrays.array(1.0f, 2.0f, 3.0f), 2.0, 2.5, new AvgAggFunc<>(new FloatSumAggFunc()));
        innerTest(Arrays.array(1.0, 2.0, 3.0), 2.0, 2.5, new AvgAggFunc<>(new DoubleSumAggFunc()));
    }

    private <IN_T extends Number, ACC_T> void innerTest(
            IN_T[] inputs,
            Double expectedResult,
            Double expectedResultAfterRetract,
            AvgAggFunc<IN_T, ACC_T> aggFunc) {
        final Tuple2<LongSumAggFunc.LongSumAccumulator, ACC_T> accumulator =
                aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isNaN();
        for (IN_T input : inputs) {
            aggFunc.add(accumulator, Tuple2.of(1L, input), 0);
        }
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResult);
        aggFunc.retract(accumulator, Tuple2.of(1L, inputs[0]), 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResultAfterRetract);
    }
}
