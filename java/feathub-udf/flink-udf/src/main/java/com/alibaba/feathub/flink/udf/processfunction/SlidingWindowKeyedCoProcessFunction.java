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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A KeyedProcessFunction that aggregate sliding windows with different sizes. The ProcessFunction
 * should be applied after the input has been aggregated with a tumble window with size equals to
 * the step size of sliding window.
 *
 * <p>One native Flink sliding window operator can only aggregate under one sliding window size. If
 * we need aggregation under different window sizes, we need to apply multiple native Flink window
 * operators with different sizes and join the results together. Each window operator keeps the rows
 * for its window size. This will result in a lots of duplicates row in the state backend. For
 * example, if we want to do aggregation under 1 hour and 2 hours, both sliding window will keep the
 * rows in the last one hour, which are duplicated. With this process function, we only keep the
 * rows for the maximum window size so that we can avoid duplicated rows in state backend.
 *
 * <p>The ProcessFunction assumes that: 1. rows of each key are ordered by the row time 2. row time
 * attribute of rows with the same key are all distinct. The assumptions hold true after applying
 * the tumbling window aggregation to the input with window size that is same as the step size of
 * the {@link SlidingWindowKeyedCoProcessFunction} to be applied.
 */
public class SlidingWindowKeyedCoProcessFunction extends KeyedProcessFunction<Row, Row, Row> {

    private final AggregationFieldsDescriptor aggregationFieldsDescriptor;
    private final TypeSerializer<Row> inputRowTypeSerializer;
    private final TypeSerializer<Row> outputRowTypeSerializer;
    private final String rowTimeFieldName;
    private final String[] keyFieldNames;
    private final long stepSizeMs;
    private SlidingWindowState state;
    private final PostSlidingWindowExpiredRowHandler expiredRowHandler;
    private final boolean skipSameWindowOutput;

    /**
     * The grace period specifies the maximum duration a row can stay in the state after it is
     * retracted from all windows. The grace period can reduce the frequency of pruning row and
     * reduce state accessing.
     */
    private final long pruneRowGracePeriod;

    public SlidingWindowKeyedCoProcessFunction(
            AggregationFieldsDescriptor aggregationFieldsDescriptor,
            TypeSerializer<Row> inputRowTypeSerializer,
            TypeSerializer<Row> outputRowTypeSerializer,
            String[] keyFieldNames,
            String rowTimeFieldName,
            long stepSizeMs,
            PostSlidingWindowExpiredRowHandler expiredRowHandler,
            boolean skipSameWindowOutput) {
        this.aggregationFieldsDescriptor = aggregationFieldsDescriptor;
        this.inputRowTypeSerializer = inputRowTypeSerializer;
        this.outputRowTypeSerializer = outputRowTypeSerializer;
        this.rowTimeFieldName = rowTimeFieldName;
        this.keyFieldNames = keyFieldNames;
        this.stepSizeMs = stepSizeMs;
        this.expiredRowHandler = expiredRowHandler;
        this.skipSameWindowOutput = skipSameWindowOutput;

        // We set the grace period to be 1/10 of the maximum window time, so that the state size is
        // at most 1/10 larger. And we only need to call prune row once every 1/10 of the maximum
        // window time.
        this.pruneRowGracePeriod =
                Math.floorDiv(aggregationFieldsDescriptor.getMaxWindowSizeMs(), 10);
    }

    @Override
    public void open(Configuration parameters) {
        state =
                SlidingWindowState.create(
                        getRuntimeContext(),
                        aggregationFieldsDescriptor,
                        inputRowTypeSerializer,
                        outputRowTypeSerializer);
    }

    @Override
    public void processElement(
            Row row, KeyedProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        final long rowTime = row.getFieldAs(rowTimeFieldName);
        System.out.println("processElement " + row);

        List<Integer> leftIdxList = state.getLeftTimestampIdx();
        List<Long> timestampList = state.getTimestampList();
        List<Integer> affectedAggFieldIndices = new ArrayList<>();
        Row accumulatorStates = state.getAccumulatorStates();
        long maxTriggeredTimer =
                state.maxTriggeredTimer.value() == null
                        ? Long.MIN_VALUE
                        : state.maxTriggeredTimer.value();
        for (AggregationFieldsDescriptor.AggregationFieldDescriptor aggFieldDescriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            int aggFieldIdx = aggregationFieldsDescriptor.getAggFieldIdx(aggFieldDescriptor);
            int leftIdx = leftIdxList.get(aggFieldIdx);
            if (leftIdx < timestampList.size() && timestampList.get(leftIdx) > rowTime) {
                continue;
            }
            if (rowTime + aggFieldDescriptor.windowSizeMs <= maxTriggeredTimer) {
                continue;
            }
            Object accumulatorState = accumulatorStates.getField(aggFieldIdx);
            Object preAggResult = row.getField(aggFieldDescriptor.outFieldName);
            aggFieldDescriptor.aggFunc.add(accumulatorState, preAggResult, rowTime);
            accumulatorStates.setField(aggFieldIdx, accumulatorState);
            affectedAggFieldIndices.add(aggFieldIdx);

            if (skipSameWindowOutput) {
                ctx.timerService()
                        .registerEventTimeTimer(rowTime + aggFieldDescriptor.windowSizeMs);
            }
        }
        state.updateAccumulatorStates(accumulatorStates);

        if (skipSameWindowOutput) {
            // Only register timer on the event time of the row and on the row expire time.
            ctx.timerService()
                    .registerEventTimeTimer(Math.max(rowTime, maxTriggeredTimer + stepSizeMs));
            System.out.println("register timer " + Math.max(rowTime, maxTriggeredTimer + stepSizeMs));
        } else {
            Long triggerTime = state.maxRegisteredTimer.value();
            if (triggerTime == null) {
                triggerTime = rowTime;
            } else {
                triggerTime = Math.max(triggerTime + stepSizeMs, rowTime);
            }
            long rowExpireTime = rowTime + aggregationFieldsDescriptor.getMaxWindowSizeMs();

            // TODO: Register at most one timer at a timestamp regardless of the number of the keys
            //  processed by the operator.
            while (triggerTime <= rowExpireTime) {
                ctx.timerService().registerEventTimeTimer(triggerTime);
                System.out.println("register timer " + triggerTime);
                triggerTime += stepSizeMs;
            }
            state.maxRegisteredTimer.update(triggerTime - stepSizeMs);
        }

        state.addRow(rowTime, row, affectedAggFieldIndices);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Row, Row, Row>.OnTimerContext ctx,
            Collector<Row> out)
            throws Exception {
        state.maxTriggeredTimer.update(timestamp);
        System.out.println("onTimer " + timestamp);
        boolean hasRow = false;

        final List<Long> timestampList = state.getTimestampList();
        final int timestampListSize = timestampList.size();

        Row outputRow = Row.withNames();
        for (int i = 0; i < keyFieldNames.length; i++) {
            outputRow.setField(keyFieldNames[i], ctx.getCurrentKey().getField(i));
        }

        //        List<Row> rowsToAdd = state.timestampToRow.get(timestamp);
        final Row accumulatorStates = state.getAccumulatorStates();
        final List<Integer> leftIdxList = state.getLeftTimestampIdx();

        for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                aggregationFieldsDescriptor.getAggFieldDescriptors()) {
            final int aggFieldIdx = aggregationFieldsDescriptor.getAggFieldIdx(descriptor);
            Object accumulatorState = accumulatorStates.getField(aggFieldIdx);
            //            if (rowsToAdd != null) {
            //                for (Row rowToAdd: rowsToAdd) {
            //                    descriptor.aggFunc.add(
            //                            accumulatorState,
            // rowToAdd.getField(descriptor.outFieldName), timestamp);
            //                }
            //            }

            // Advance left idx and retract rows whose rowTime is out of the time window
            // (timestamp - descriptor.windowSizeMs, timestamp]
            int leftIdx = leftIdxList.get(aggFieldIdx);
            for (; leftIdx < timestampListSize; ++leftIdx) {
                long rowTime = timestampList.get(leftIdx);
                if (timestamp - descriptor.windowSizeMs < rowTime) {
                    break;
                }
                for (Row curRow : state.timestampToRow.get(rowTime)) {
                    descriptor.aggFunc.retract(
                            accumulatorState, curRow.getField(descriptor.outFieldName), rowTime);
                }
            }
            if (leftIdx < timestampList.size() && timestampList.get(leftIdx) <= timestamp) {
                // If the row time of the earliest row that is not retracted is less than or
                // equal to the current time, we know there is at least one row in the
                // current window.
                hasRow = true;
            }
            leftIdxList.set(aggFieldIdx, leftIdx);

            outputRow.setField(
                    descriptor.outFieldName, descriptor.aggFunc.getResult(accumulatorState));
        }
        state.updateLeftTimestampIdx(leftIdxList);
        state.updateAccumulatorStates(accumulatorStates);
        outputRow.setField(rowTimeFieldName, Instant.ofEpochMilli(timestamp));

        if (!hasRow) {
            if (expiredRowHandler != null) {
                final Row lastRow =
                        Preconditions.checkNotNull(
                                state.lastOutputRow.value(), "The last row should not be null.");
                expiredRowHandler.handleExpiredRow(out, lastRow, timestamp);
            }

            state.pruneRow(timestamp, aggregationFieldsDescriptor);
            state.lastOutputRow.clear();
            return;
        }

        state.lastOutputRow.update(outputRow);
        out.collect(outputRow);

        final Long earliestRowTime = timestampList.get(0);
        if (earliestRowTime
                <= timestamp
                        - aggregationFieldsDescriptor.getMaxWindowSizeMs()
                        - pruneRowGracePeriod) {
            state.pruneRow(timestamp, aggregationFieldsDescriptor);
        }
    }

    /** The state of {@link SlidingWindowKeyedCoProcessFunction}. */
    private static class SlidingWindowState {

        private final AggregationFieldsDescriptor aggregationFieldsDescriptor;

        /** This MapState maps from row timestamp to the row. */
        private final MapState<Long, List<Row>> timestampToRow;

        /**
         * This ListState keeps all the row timestamp that has been added to the timestampToRow
         * state. Since the operator assume rows are ordered by time, the timestamp list should be
         * ordered as well.
         */
        private final ListState<Long> timestampList;

        /**
         * This ValueState keeps a list of index of the entry in the timestampList for each
         * aggregation field such that all entries before it must have been outside the time window
         * for this field. And it always points to the timestamp of the earliest row that has not
         * been retracted from the aggregation function of that field. This list has the same order
         * as the aggregation fields.
         */
        private final ValueState<List<Integer>> leftTimestampIdxList;

        /**
         * This ValueState keeps the maximum registered timer so that we don't try to register the
         * same timer twice.
         */
        private final ValueState<Long> maxRegisteredTimer;

        /**
         * This ValueState keeps the maximum triggered timer so that we only try to register timers
         * whose timestamps are larger than this one.
         */
        private final ValueState<Long> maxTriggeredTimer;

        /**
         * This ValueState keeps the last output row so that we can handle last output row when it
         * is expired.
         */
        private final ValueState<Row> lastOutputRow;

        /**
         * This ValueState keeps the accumulator state for the given aggregationFieldsDescriptor.
         */
        private final ValueState<Row> accumulatorStates;

        private SlidingWindowState(
                AggregationFieldsDescriptor aggregationFieldsDescriptor,
                MapState<Long, List<Row>> timestampToRow,
                ListState<Long> timestampList,
                ValueState<List<Integer>> leftTimestampIdxList,
                ValueState<Long> maxRegisteredTimer,
                ValueState<Long> maxTriggeredTimer,
                ValueState<Row> lastOutputRow,
                ValueState<Row> accumulatorStates) {
            this.aggregationFieldsDescriptor = aggregationFieldsDescriptor;
            this.timestampToRow = timestampToRow;
            this.timestampList = timestampList;
            this.leftTimestampIdxList = leftTimestampIdxList;
            this.maxRegisteredTimer = maxRegisteredTimer;
            this.maxTriggeredTimer = maxTriggeredTimer;
            this.lastOutputRow = lastOutputRow;
            this.accumulatorStates = accumulatorStates;
        }

        @SuppressWarnings({"rawtypes"})
        public static SlidingWindowState create(
                RuntimeContext context,
                AggregationFieldsDescriptor aggregationFieldsDescriptor,
                TypeSerializer<Row> inputRowTypeSerializer,
                TypeSerializer<Row> outputRowTypeSerializer) {
            final MapState<Long, List<Row>> timestampToRow =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "TimestampToRow",
                                    LongSerializer.INSTANCE,
                                    new ListSerializer<>(inputRowTypeSerializer)));

            final ListState<Long> listState =
                    context.getListState(
                            new ListStateDescriptor<>(
                                    "TimestampListState", LongSerializer.INSTANCE));

            final ValueState<List<Integer>> outFieldNameToLeftTimestampIdx =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "OutFieldNameToLeftTimestampIdx", Types.LIST(Types.INT)));

            final ValueState<Long> maxRegisteredTimer =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "MaxRegisteredTimer", LongSerializer.INSTANCE));

            final ValueState<Long> maxTriggeredTimer =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "maxTriggeredTimer", LongSerializer.INSTANCE));

            final ValueState<Row> lastOutputRow =
                    context.getState(
                            new ValueStateDescriptor<>("LastOutputRow", outputRowTypeSerializer));

            final TypeInformation[] accumulatorTypeInformation =
                    aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                            .map(descriptor -> descriptor.aggFunc.getAccumulatorTypeInformation())
                            .toArray(TypeInformation[]::new);
            ValueState<Row> accumulatorStates =
                    context.getState(
                            new ValueStateDescriptor<>(
                                    "AccumulatorStates", Types.ROW(accumulatorTypeInformation)));

            return new SlidingWindowState(
                    aggregationFieldsDescriptor,
                    timestampToRow,
                    listState,
                    outFieldNameToLeftTimestampIdx,
                    maxRegisteredTimer,
                    maxTriggeredTimer,
                    lastOutputRow,
                    accumulatorStates);
        }

        /**
         * Add the row to the state.
         *
         * @param timestamp The row time of the row.
         * @param row The row to be added
         */
        public void addRow(long timestamp, Row row, List<Integer> affectedAggFieldIndices)
                throws Exception {
            if (timestampToRow.contains(timestamp)) {
                List<Row> rows = timestampToRow.get(timestamp);
                System.out.println(rows.getClass());
                rows.add(row);
                timestampToRow.put(timestamp, rows);
            } else {
                timestampToRow.put(timestamp, new ArrayList<>(Collections.singletonList(row)));

                List<Long> timestamps = getTimestampList();
                if (timestamps.isEmpty() || timestamps.get(timestamps.size() - 1) < timestamp) {
                    timestampList.add(timestamp);
                } else {
                    for (int i = 0; i < timestamps.size(); i++) {
                        if (timestamps.get(i) < timestamp) {
                            continue;
                        }
                        timestamps.add(i, timestamp);
                        List<Integer> leftIdxList = getLeftTimestampIdx();
                        for (AggregationFieldsDescriptor.AggregationFieldDescriptor
                                aggFieldDescriptor :
                                        aggregationFieldsDescriptor.getAggFieldDescriptors()) {
                            int aggFieldIdx =
                                    aggregationFieldsDescriptor.getAggFieldIdx(aggFieldDescriptor);
                            if (!affectedAggFieldIndices.contains(aggFieldIdx)) {
                                leftIdxList.set(aggFieldIdx, leftIdxList.get(aggFieldIdx) + 1);
                            }
                        }
                        break;
                    }
                    timestampList.update(timestamps);
                }
            }
        }

        /**
         * Prune all the row with timestamp that are less than or equals to currentTimestamp -
         * maxWindowSizeMs. It also cleans up the state if the window is empty after prune.
         *
         * @param currentTimestamp The current timestamp.
         * @param aggFieldsDescriptor The aggFieldsDescriptor.
         */
        public void pruneRow(long currentTimestamp, AggregationFieldsDescriptor aggFieldsDescriptor)
                throws Exception {
            long lowerBound = currentTimestamp - aggFieldsDescriptor.getMaxWindowSizeMs();
            List<Long> timestamps = getTimestampList();

            final int originalSize = timestamps.size();
            final Iterator<Long> iterator = timestamps.iterator();

            while (iterator.hasNext()) {
                final long cur = iterator.next();
                if (cur > lowerBound) {
                    break;
                }
                timestampToRow.remove(cur);
                iterator.remove();
            }

            final List<Integer> leftIdxList = getLeftTimestampIdx();
            for (AggregationFieldsDescriptor.AggregationFieldDescriptor descriptor :
                    aggFieldsDescriptor.getAggFieldDescriptors()) {
                final int removedRowCnt = originalSize - timestamps.size();
                final int aggFieldIdx = aggFieldsDescriptor.getAggFieldIdx(descriptor);
                final int leftIdx = leftIdxList.get(aggFieldIdx) - removedRowCnt;
                leftIdxList.set(aggFieldIdx, Math.max(leftIdx, 0));
            }
            updateLeftTimestampIdx(leftIdxList);

            if (timestamps.isEmpty()) {
                timestampList.clear();
                leftTimestampIdxList.clear();
                maxRegisteredTimer.clear();
                lastOutputRow.clear();
                accumulatorStates.clear();
            } else {
                timestampList.update(timestamps);
            }
        }

        public List<Integer> getLeftTimestampIdx() throws Exception {
            List<Integer> leftTimestampIdx = leftTimestampIdxList.value();
            if (leftTimestampIdx == null) {
                leftTimestampIdx =
                        new ArrayList<>(
                                Arrays.asList(
                                        new Integer
                                                [aggregationFieldsDescriptor
                                                        .getAggFieldDescriptors()
                                                        .size()]));
                Collections.fill(leftTimestampIdx, 0);
            }
            return leftTimestampIdx;
        }

        public void updateLeftTimestampIdx(List<Integer> leftTimestampIdx) throws Exception {
            leftTimestampIdxList.update(leftTimestampIdx);
        }

        public List<Long> getTimestampList() throws Exception {
            List<Long> timestamps = new ArrayList<>();
            final Iterable<Long> iter = timestampList.get();
            if (iter != null) {
                iter.forEach(timestamps::add);
            }
            return timestamps;
        }

        public Row getAccumulatorStates() throws IOException {
            Row acc = accumulatorStates.value();
            if (acc == null) {
                final Object[] accumulators =
                        aggregationFieldsDescriptor.getAggFieldDescriptors().stream()
                                .map(descriptor -> descriptor.aggFunc.createAccumulator())
                                .toArray();
                acc = Row.of(accumulators);
            }
            return acc;
        }

        public void updateAccumulatorStates(Row accumulators) throws IOException {
            accumulatorStates.update(accumulators);
        }
    }
}
