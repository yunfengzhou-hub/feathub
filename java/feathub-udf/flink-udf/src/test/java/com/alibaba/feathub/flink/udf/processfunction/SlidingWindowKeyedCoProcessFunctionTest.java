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

package com.alibaba.feathub.flink.udf.processfunction;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;
import com.alibaba.feathub.flink.udf.SlidingWindowUtils;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlidingWindowKeyedCoProcessFunction}. */
public class SlidingWindowKeyedCoProcessFunctionTest {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table inputTable;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        final DataStream<Row> data =
                env.fromElements(
                        Row.of(0, 1L, Instant.ofEpochMilli(0)),
//                        Row.of(1, 2L, Instant.ofEpochMilli(600)),
//                        Row.of(1, 3L, Instant.ofEpochMilli(1100)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5000)),
                        Row.of(0, 3L, Instant.ofEpochMilli(4000)),
                        Row.of(0, 5L, Instant.ofEpochMilli(6000)));
        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2 - INTERVAL '2' SECOND")
                                        .build())
                        .as("id", "val", "ts");
    }

    @Test
    void testMultiSlidingWindowSizeProcessFunction() {
        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "SUM",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_avg_1",
                                        DataTypes.FLOAT(),
                                        1000L,
                                        "AVG",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_avg_2",
                                        DataTypes.DOUBLE(),
                                        2000L,
                                        "AVG",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_value_counts_2",
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                        2000L,
                                        "VALUE_COUNTS",
                                        null)
                                .build(),
                        null,
                        false);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2L,
                                2.0f,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                3L,
                                5L,
                                3.0f,
                                2.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                1,
                                0L,
                                3L,
                                Float.NaN,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                1L,
                                1L,
                                1.0f,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                0,
                                0L,
                                1L,
                                Float.NaN,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                0,
                                3L,
                                3L,
                                3.0f,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                4L,
                                7L,
                                4.0f,
                                3.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                        put(4L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(5999)),
                        Row.of(
                                0,
                                5L,
                                9L,
                                5.0f,
                                4.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(4L, 1L);
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(6999)),
                        Row.of(
                                0,
                                0L,
                                5L,
                                Float.NaN,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testEnableEmptyWindowOutputDisableSameWindowOutput() {
        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);
        zeroValuedRow.setField("val_avg_2", null);
        zeroValuedRow.setField("val_value_counts_2", null);

        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_avg_2",
                                        DataTypes.DOUBLE(),
                                        2000L,
                                        "AVG",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_value_counts_2",
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                        2000L,
                                        "VALUE_COUNTS",
                                        null)
                                .build(),
                        zeroValuedRow,
                        true);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                5L,
                                2.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                1,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(1, 0L, null, null, Instant.ofEpochMilli(3999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                7L,
                                3.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                        put(4L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(5999)),
                        Row.of(
                                0,
                                9L,
                                4.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(4L, 1L);
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(6999)),
                        Row.of(
                                0,
                                5L,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(8999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testEnableEmptyWindowOutputAndSameWindowOutput() {
        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);
        zeroValuedRow.setField("val_avg_2", null);
        zeroValuedRow.setField("val_value_counts_2", null);

        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_avg_2",
                                        DataTypes.DOUBLE(),
                                        2000L,
                                        "AVG",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_value_counts_2",
                                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                        2000L,
                                        "VALUE_COUNTS",
                                        null)
                                .build(),
                        zeroValuedRow,
                        false);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(
                                1,
                                2L,
                                2.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                1,
                                5L,
                                2.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(2L, 1L);
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(
                                1,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(2999)),
                        Row.of(1, 0L, null, null, Instant.ofEpochMilli(3999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(999)),
                        Row.of(
                                0,
                                1L,
                                1.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(1L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(1999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(2999)),
                        Row.of(
                                0,
                                3L,
                                3.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(4999)),
                        Row.of(
                                0,
                                7L,
                                3.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(3L, 1L);
                                        put(4L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(5999)),
                        Row.of(
                                0,
                                9L,
                                4.5,
                                new HashMap<Long, Long>() {
                                    {
                                        put(4L, 1L);
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(6999)),
                        Row.of(
                                0,
                                5L,
                                5.0,
                                new HashMap<Long, Long>() {
                                    {
                                        put(5L, 1L);
                                    }
                                },
                                Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, null, null, Instant.ofEpochMilli(8999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testMinMax() {
        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_max_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "MAX",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_max_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "MAX",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_min_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "MIN",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_min_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "MIN",
                                        null)
                                .build(),
                        null,
                        false);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 3L, 3L, 2L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 4L, 4L, 3L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 5L, 5L, 4L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, null, 5L, Instant.ofEpochMilli(7999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testFirstValue() {
        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_first_value_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "FIRST_VALUE",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_first_value_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "FIRST_VALUE",
                                        null)
                                .build(),
                        null,
                        false);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 2L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 3L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 4L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, Instant.ofEpochMilli(7999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testLastValue() {
        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_last_value_1",
                                        DataTypes.BIGINT(),
                                        1000L,
                                        "LAST_VALUE",
                                        null)
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_last_value_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "LAST_VALUE",
                                        null)
                                .build(),
                        null,
                        false);

        List<Row> expected =
                java.util.Arrays.asList(
                        Row.of(1, 2L, 2L, Instant.ofEpochMilli(999)),
                        Row.of(1, 3L, 3L, Instant.ofEpochMilli(1999)),
                        Row.of(1, null, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 1L, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, null, 1L, Instant.ofEpochMilli(1999)),
                        Row.of(0, 3L, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 5L, 5L, Instant.ofEpochMilli(6999)),
                        Row.of(0, null, 5L, Instant.ofEpochMilli(7999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testLateData() {
        env.getConfig().setParallelism(1);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        final DataStream<Row> data =
                env.addSource(
                        new SourceFunctionWithInterval(
                                100,
                                Row.of(0, 1L, Instant.ofEpochMilli(0)),
                                Row.of(0, 3L, Instant.ofEpochMilli(3000)),
                                Row.of(0, 2L, Instant.ofEpochMilli(2100)),
                                Row.of(0, 0L, Instant.ofEpochMilli(4000)),
                                Row.of(0, 4L, Instant.ofEpochMilli(5000)),
                                Row.of(0, 5L, Instant.ofEpochMilli(6000))),
                        Types.ROW(Types.INT, Types.LONG, Types.INSTANT));
        //        final DataStream<Row> data =
        //                env.fromElements(
        //                        Row.of(0, 1L, Instant.ofEpochMilli(0)),
        //                        Row.of(0, 3L, Instant.ofEpochMilli(3000)),
        //                        Row.of(0, 2L, Instant.ofEpochMilli(2100)),
        //                        Row.of(0, 0L, Instant.ofEpochMilli(4000)),
        //                        Row.of(0, 4L, Instant.ofEpochMilli(5000)),
        //                        Row.of(0, 5L, Instant.ofEpochMilli(6000)));
        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2")
                                        .build())
                        .as("id", "val", "ts");

        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);

        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM",
                                        null)
                                .build(),
                        zeroValuedRow,
                        true);

        List<Row> expected1 =
                java.util.Arrays.asList(
                        Row.of(0, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 5L, Instant.ofEpochMilli(3999)),
                        Row.of(0, 3L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 9L, Instant.ofEpochMilli(6999)),
                        Row.of(0, 5L, Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(8999)));

        List<Row> expected2 =
                java.util.Arrays.asList(
                        Row.of(0, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 9L, Instant.ofEpochMilli(6999)),
                        Row.of(0, 5L, Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(8999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected1);
        //        assertThat(actual.equals(expected1) || actual.equals(expected2)).isTrue();
    }

    @Test
    void testLateData2() {
        env.getConfig().setParallelism(1);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        final DataStream<Row> data =
                env.addSource(
                        new SourceFunctionWithInterval(
                                100,
                                Row.of(0, 1L, Instant.ofEpochMilli(0)),
                                Row.of(0, 3L, Instant.ofEpochMilli(2500)),
                                Row.of(0, 1L, Instant.ofEpochMilli(3500)),
                                Row.of(0, 2L, Instant.ofEpochMilli(2100)),
                                Row.of(0, 0L, Instant.ofEpochMilli(4000)),
                                Row.of(0, 4L, Instant.ofEpochMilli(5000)),
                                Row.of(0, 5L, Instant.ofEpochMilli(6000))),
                        Types.ROW(Types.INT, Types.LONG, Types.INSTANT));
        //                env.fromElements(
        //                        Row.of(0, 1L, Instant.ofEpochMilli(0)),
        //                        Row.of(0, 3L, Instant.ofEpochMilli(3000)),
        //                        Row.of(0, 0L, Instant.ofEpochMilli(3500)),
        //                        Row.of(0, 2L, Instant.ofEpochMilli(2100)),
        //                        Row.of(0, 0L, Instant.ofEpochMilli(4000)),
        //                        Row.of(0, 4L, Instant.ofEpochMilli(5000)),
        //                        Row.of(0, 5L, Instant.ofEpochMilli(6000)));
        inputTable =
                tEnv.fromDataStream(
                                data,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))
                                        .watermark("f2", "f2")
                                        .build())
                        .as("id", "val", "ts");

        final Row zeroValuedRow = Row.withNames();
        zeroValuedRow.setField("val_sum_2", 0);

        Table table =
                SlidingWindowUtils.applySlidingWindow(
                        tEnv,
                        inputTable,
                        Arrays.array("id"),
                        "ts",
                        1000L,
                        AggregationFieldsDescriptor.builder()
                                .addField(
                                        "val",
                                        DataTypes.BIGINT(),
                                        "val_sum_2",
                                        DataTypes.BIGINT(),
                                        2000L,
                                        "SUM",
                                        null)
                                .build(),
                        zeroValuedRow,
                        true);

        List<Row> expected1 =
                java.util.Arrays.asList(
                        Row.of(0, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 6L, Instant.ofEpochMilli(3999)),
                        Row.of(0, 1L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 9L, Instant.ofEpochMilli(6999)),
                        Row.of(0, 5L, Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(8999)));

        List<Row> expected2 =
                java.util.Arrays.asList(
                        Row.of(0, 1L, Instant.ofEpochMilli(999)),
                        Row.of(0, 3L, Instant.ofEpochMilli(2999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(4999)),
                        Row.of(0, 4L, Instant.ofEpochMilli(5999)),
                        Row.of(0, 9L, Instant.ofEpochMilli(6999)),
                        Row.of(0, 5L, Instant.ofEpochMilli(7999)),
                        Row.of(0, 0L, Instant.ofEpochMilli(8999)));

        List<Row> actual = CollectionUtil.iteratorToList(table.execute().collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected1);
        //        assertThat(actual.equals(expected1) || actual.equals(expected2)).isTrue();
    }

    private static class SourceFunctionWithInterval implements SourceFunction<Row> {
        private final long intervalMs;
        private final List<Row> data;

        private SourceFunctionWithInterval(long intervalMs, Row... data) {
            this.intervalMs = intervalMs;
            this.data = java.util.Arrays.asList(data);
        }

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            for (Row t : data) {
                sourceContext.collect(t);
                Thread.sleep(intervalMs);
            }
        }

        @Override
        public void cancel() {}
    }
}
