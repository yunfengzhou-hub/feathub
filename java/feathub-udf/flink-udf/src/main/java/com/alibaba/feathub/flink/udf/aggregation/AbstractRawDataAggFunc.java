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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.LinkedList;

/** Abstract aggregate function that collects raw data. */
public abstract class AbstractRawDataAggFunc<IN_T, OUT_T>
        implements AggFunc<IN_T, OUT_T, AbstractRawDataAggFunc.RawDataAccumulator<IN_T>> {
    @Override
    public void add(RawDataAccumulator<IN_T> acc, IN_T value, long timestamp) {
        if (acc.rawDataList.isEmpty() || timestamp >= acc.rawDataList.getLast().f1) {
            acc.rawDataList.add(Tuple2.of(value, timestamp));
            return;
        }

        int index = -1;
        for (Tuple2<IN_T, Long> tuple2 : acc.rawDataList) {
            index++;
            if (tuple2.f1 > timestamp) {
                break;
            }
        }

        acc.rawDataList.add(index, Tuple2.of(value, timestamp));
    }

    @Override
    public void merge(RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        if (source.rawDataList.isEmpty()) {
            return;
        }

        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return;
        }

        Iterator<Tuple2<IN_T, Long>> iterator0 = target.rawDataList.iterator();
        Iterator<Tuple2<IN_T, Long>> iterator1 = source.rawDataList.iterator();
        Tuple2<IN_T, Long> tuple0 = getNextOrNull(iterator0);
        Tuple2<IN_T, Long> tuple1 = getNextOrNull(iterator1);
        target.rawDataList = new LinkedList<>();
        while (tuple0 != null && tuple1 != null) {
            if (tuple0.f1 < tuple1.f1) {
                target.rawDataList.add(tuple0);
                tuple0 = getNextOrNull(iterator0);
            } else {
                target.rawDataList.add(tuple1);
                tuple1 = getNextOrNull(iterator1);
            }
        }

        while (tuple0 != null) {
            target.rawDataList.add(tuple0);
            tuple0 = getNextOrNull(iterator0);
        }

        while (tuple1 != null) {
            target.rawDataList.add(tuple1);
            tuple1 = getNextOrNull(iterator1);
        }
    }

    private static <T> T getNextOrNull(Iterator<T> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void retract(RawDataAccumulator<IN_T> accumulator, IN_T value) {
        Preconditions.checkState(
                accumulator.rawDataList.getFirst().f0.equals(value),
                "Value must be retracted by the ordered as they added to the AbstractRawDataAggFuncBase.");
        accumulator.rawDataList.removeFirst();
    }

    @Override
    public void retractAccumulator(
            RawDataAccumulator<IN_T> target, RawDataAccumulator<IN_T> source) {
        for (Tuple2<IN_T, Long> value : source.rawDataList) {
            Preconditions.checkState(
                    target.rawDataList.getFirst().equals(value),
                    "Value must be retracted by the order as they added to the AggFuncWithLimit.");
            target.rawDataList.removeFirst();
        }
    }

    @Override
    public RawDataAccumulator<IN_T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }

    /** Accumulator that collects raw data and their timestamps. */
    public static class RawDataAccumulator<T> {
        public LinkedList<Tuple2<T, Long>> rawDataList = new LinkedList<>();
    }
}
