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
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.avg.AvgAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.avg.AvgPreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAccumulator;
import com.alibaba.feathub.flink.udf.aggregation.sum.SumAggFunc;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AvgAggFunc} and {@link AvgPreAggFunc}. */
class AvgAggFuncTest {
    @Test
    void testAvgPreAggFunc() {
        innerTestPreAggFunc(
                Arrays.array(1, 2),
                Row.of(3, 2),
                Arrays.array(3, 4),
                Row.of(7, 2),
                Row.of(10, 4),
                new AvgPreAggFunc<>(new SumAggFunc.IntSumAggFunc()));
        innerTestPreAggFunc(
                Arrays.array(1L, 2L),
                Row.of(3L, 2),
                Arrays.array(3L, 4L),
                Row.of(7L, 2),
                Row.of(10L, 4),
                new AvgPreAggFunc<>(new SumAggFunc.LongSumAggFunc()));
        innerTestPreAggFunc(
                Arrays.array(1.0f, 2.0f),
                Row.of(3.0f, 2),
                Arrays.array(3.0f, 4.0f),
                Row.of(7.0f, 2),
                Row.of(10.0f, 4),
                new AvgPreAggFunc<>(new SumAggFunc.FloatSumAggFunc()));
        innerTestPreAggFunc(
                Arrays.array(1.0, 2.0),
                Row.of(3.0, 2),
                Arrays.array(3.0, 4.0),
                Row.of(7.0, 2),
                Row.of(10.0, 4),
                new AvgPreAggFunc<>(new SumAggFunc.DoubleSumAggFunc()));
    }

    private <IN_T extends Number, ACC_T> void innerTestPreAggFunc(
            IN_T[] inputs1,
            Row expectedResult1,
            IN_T[] inputs2,
            Row expectedResult2,
            Row expectedMergedResult,
            AvgPreAggFunc<IN_T, ACC_T> preAggFunc) {
        Tuple2<CountAccumulator, ACC_T> acc1 = preAggFunc.createAccumulator();
        for (IN_T input : inputs1) {
            preAggFunc.add(acc1, input, 0);
        }
        assertThat(preAggFunc.getResult(acc1).toString()).isEqualTo(expectedResult1.toString());

        Tuple2<CountAccumulator, ACC_T> acc2 = preAggFunc.createAccumulator();
        for (IN_T input : inputs2) {
            preAggFunc.add(acc2, input, 0);
        }
        assertThat(preAggFunc.getResult(acc2).toString()).isEqualTo(expectedResult2.toString());

        preAggFunc.merge(acc1, acc2);
        assertThat(preAggFunc.getResult(acc1).toString())
                .isEqualTo(expectedMergedResult.toString());
    }

    @Test
    void testAvgAggregationFunctions() {
        innerTest(Arrays.array(1, 2, 3, 4), 2.5, 3.0, new AvgAggFunc(DataTypes.INT()));
        innerTest(Arrays.array(1L, 2L, 3L), 2.0, 2.5, new AvgAggFunc(DataTypes.BIGINT()));
        innerTest(Arrays.array(1.0f, 2.0f, 3.0f), 2.0, 2.5, new AvgAggFunc(DataTypes.FLOAT()));
        innerTest(Arrays.array(1.0, 2.0, 3.0), 2.0, 2.5, new AvgAggFunc(DataTypes.DOUBLE()));
    }

    private void innerTest(
            Object[] inputs,
            Double expectedResult,
            Double expectedResultAfterRetract,
            AvgAggFunc aggFunc) {
        final AvgAggFunc.AvgAccumulator accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(null);
        for (Object input : inputs) {
            aggFunc.add(accumulator, Row.of(input, 1L), 0);
        }
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResult);
        aggFunc.retract(accumulator, Row.of(inputs[0], 1L));
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(expectedResultAfterRetract);
    }
}
