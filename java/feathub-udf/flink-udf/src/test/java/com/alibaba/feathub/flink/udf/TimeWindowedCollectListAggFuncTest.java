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

/** JavaDoc. */
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
    void testTimeWindowedValueCountsAggFuncInt() {
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
