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

import com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFunc;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AggFuncWithLimit} and {@link PreAggFuncWithLimit}. */
public class AggFuncWithLimitTest {
    @Test
    void testPreAggFuncWithLimit() {
        final PreAggFuncWithLimit<Integer> preAggFunc = new PreAggFuncWithLimit<>(DataTypes.INT());

        RawDataAccumulator<Integer> acc1 = preAggFunc.createAccumulator();
        preAggFunc.add(acc1, 1, 1);
        preAggFunc.add(acc1, 2, 2);
        assertThat(preAggFunc.getResult(acc1)).containsExactly(Tuple2.of(1, 1L), Tuple2.of(2, 2L));

        RawDataAccumulator<Integer> acc2 = preAggFunc.createAccumulator();
        preAggFunc.add(acc2, 3, 3);
        preAggFunc.add(acc2, 4, 4);
        assertThat(preAggFunc.getResult(acc2)).containsExactly(Tuple2.of(3, 3L), Tuple2.of(4, 4L));

        preAggFunc.merge(acc1, acc2);
        assertThat(preAggFunc.getResult(acc1))
                .containsExactly(
                        Tuple2.of(1, 1L), Tuple2.of(2, 2L), Tuple2.of(3, 3L), Tuple2.of(4, 4L));
    }

    @Test
    void testAggFuncWithLimit() {
        final MinMaxAggFunc<Integer> internalAggFunc = new MinMaxAggFunc<>(DataTypes.INT(), true);
        final AggFuncWithLimit<
                        Integer,
                        Integer,
                        Integer,
                        MinMaxAggFunc.MinMaxAccumulator,
                        MinMaxAggFunc.MinMaxAccumulator>
                aggFunc = new AggFuncWithLimit<>(internalAggFunc, internalAggFunc, 3);

        RawDataAccumulator<Integer> acc = aggFunc.createAccumulator();

        aggFunc.add(acc, Arrays.asList(Tuple2.of(1, 1L), Tuple2.of(2, 2L)), 2L);
        assertThat(aggFunc.getResult(acc)).isEqualTo(1);

        aggFunc.add(acc, Arrays.asList(Tuple2.of(3, 3L), Tuple2.of(4, 4L)), 4L);
        assertThat(aggFunc.getResult(acc)).isEqualTo(2);

        aggFunc.retract(acc, Arrays.asList(Tuple2.of(1, 1L), Tuple2.of(2, 2L)));
        assertThat(aggFunc.getResult(acc)).isEqualTo(3);
    }
}
