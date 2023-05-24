/*
 * Copyright 2022 The FeatHub Authors
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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

/** Test for {@link TimeWindowedCollectListAggFunc}. */
public class TimeWindowedCollectListAggFuncTest extends AbstractTimeWindowedAggFuncTest {

    @Override
    protected DataStreamSource<Row> getData(StreamExecutionEnvironment env) {
        return env.fromElements(
                Row.of(0, 1, Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
                Row.of(0, 2, Instant.ofEpochMilli(3000), ZoneId.systemDefault()),
                Row.of(0, 2, Instant.ofEpochMilli(2000), ZoneId.systemDefault()),
                Row.of(0, 3, Instant.ofEpochMilli(4000), ZoneId.systemDefault()));
    }

    @Test
    void testTimeWindowedCollectListAggFuncInt() {
        List<Row> expected =
                Arrays.asList(
                        Row.of(0, new Integer[] {1}),
                        Row.of(0, new Integer[] {1, 2}),
                        Row.of(0, new Integer[] {2, 2}),
                        Row.of(0, new Integer[] {2, 3}));
        internalTest(DataTypes.INT(), expected);
    }

    @Override
    protected Class<? extends AbstractTimeWindowedAggFunc<?, ?>> getAggFunc() {
        return TimeWindowedCollectListAggFunc.class;
    }
}
