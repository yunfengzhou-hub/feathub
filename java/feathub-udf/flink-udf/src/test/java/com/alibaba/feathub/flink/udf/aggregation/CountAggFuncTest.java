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

import com.alibaba.feathub.flink.udf.aggregation.count.CountAccumulator;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountPreAggFunc;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CountAggFunc} and {@link CountPreAggFunc}. */
class CountAggFuncTest {
    @Test
    void testCountPreAggregationFunction() {
        CountPreAggFunc preAggFunc = new CountPreAggFunc();
        CountAccumulator accumulator = preAggFunc.createAccumulator();
        preAggFunc.add(accumulator, "1L", 0);
        preAggFunc.add(accumulator, "2L", 0);
        assertThat(preAggFunc.getResult(accumulator)).isEqualTo(2);
    }

    @Test
    void testCountAggregationFunction() {
        final CountAggFunc aggFunc = new CountAggFunc();
        final CountAccumulator accumulator = aggFunc.createAccumulator();
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(0);
        aggFunc.add(accumulator, 1L, 0);
        aggFunc.add(accumulator, 2L, 0);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(3);
        aggFunc.retract(accumulator, 1L);
        assertThat(aggFunc.getResult(accumulator)).isEqualTo(2);
    }
}
