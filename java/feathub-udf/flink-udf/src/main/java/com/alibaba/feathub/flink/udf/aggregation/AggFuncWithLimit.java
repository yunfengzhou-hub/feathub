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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * Aggregation function decorator that only aggregates up to `limit` number of most recent records.
 */
public class AggFuncWithLimit<IN_T, OUT_T, ACC_T> extends AbstractRawDataAggFunc<IN_T, OUT_T> {
    private final AggFunc<IN_T, OUT_T, ACC_T> aggFunc;
    private final long limit;

    public AggFuncWithLimit(AggFunc<IN_T, OUT_T, ACC_T> aggFunc, long limit) {
        this.aggFunc = aggFunc;
        this.limit = limit;
    }

    //    @Override
    //    public void addPreAggResult(RawDataAccumulator target, RawDataAccumulator source) {
    //        if (source.rawDatas.isEmpty()) {
    //            return;
    //        }
    //
    //        if (target.rawDatas.isEmpty()) {
    //            target.rawDatas.addAll(source.rawDatas);
    //            return;
    //        }
    //
    //        if (target.rawDatas.getLast().f1 > source.rawDatas.getLast().f1) {
    //            LinkedList<Tuple2<Object, Long>> tmpList = target.rawDatas;
    //            target.rawDatas = new LinkedList<>();
    //            target.rawDatas.addAll(source.rawDatas);
    //            target.rawDatas.addAll(tmpList);
    //        } else {
    //            target.rawDatas.addAll(source.rawDatas);
    //        }
    //    }
    //
    //    @Override
    //    public void retractPreAggResult(RawDataAccumulator target, RawDataAccumulator source) {
    //        for (Tuple2<Object, Long> value : source.rawDatas) {
    //            this.retract(target, (IN_T) value.f0, value.f1);
    //        }
    //    }

    @Override
    public OUT_T getResult(RawDataAccumulator<IN_T> accumulator) {
        long numIgnoredData = accumulator.rawDatas.size() - limit;
        long count = 0;
        ACC_T internalAccumulator = aggFunc.createAccumulator();
        for (Tuple2<IN_T, Long> data : accumulator.rawDatas) {
            count++;
            if (count <= numIgnoredData) {
                continue;
            }
            aggFunc.add(internalAccumulator, data.f0, data.f1);
        }

        return aggFunc.getResult(internalAccumulator);
    }

    @Override
    public DataType getResultDatatype() {
        return aggFunc.getResultDatatype();
    }

    public static class PreAggFuncWithLimit<T>
            implements PreAggFunc<T, RawDataAccumulator<T>, RawDataAccumulator<T>> {
        @Override
        public void add(RawDataAccumulator<T> accumulator, T value, long timestamp) {
            if (accumulator.rawDatas.isEmpty() || timestamp >= accumulator.rawDatas.getLast().f1) {
                accumulator.rawDatas.add(Tuple2.of(value, timestamp));
                return;
            }

            int index = -1;
            for (Tuple2<T, Long> tuple2 : accumulator.rawDatas) {
                index++;
                if (tuple2.f1 > timestamp) {
                    accumulator.rawDatas.add(index, Tuple2.of(value, timestamp));
                    return;
                }
            }

            throw new RuntimeException(
                    String.format(
                            "Cannot add value with timestamp %s to RawDataAccumulator.",
                            timestamp));
        }

        @Override
        public RawDataAccumulator<T> getResult(RawDataAccumulator<T> accumulator) {
            return accumulator;
        }

        @Override
        public DataType getResultDatatype() {
            return DataTypes.RAW(
                    RawDataAccumulator.class,
                    Types.POJO(RawDataAccumulator.class).createSerializer(new ExecutionConfig()));
        }

        @Override
        public RawDataAccumulator<T> createAccumulator() {
            return new RawDataAccumulator<>();
        }

        @Override
        public TypeInformation getAccumulatorTypeInformation() {
            return Types.POJO(RawDataAccumulator.class);
        }
    }
}
