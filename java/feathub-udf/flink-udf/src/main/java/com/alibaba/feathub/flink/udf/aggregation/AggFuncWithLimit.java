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
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Aggregation function decorator that only aggregates up to `limit` number of most recent records.
 */
public class AggFuncWithLimit<IN_T, PRE_OUT_T, OUT_T, PRE_ACC_T, ACC_T>
        implements AggFunc<List<Tuple2<IN_T, Long>>, OUT_T, RawDataAccumulator<IN_T>> {
    private final PreAggFunc<IN_T, PRE_OUT_T, PRE_ACC_T> preAggFunc;
    private final AggFunc<PRE_OUT_T, OUT_T, ACC_T> aggFunc;
    private final long limit;

    public AggFuncWithLimit(
            PreAggFunc<IN_T, PRE_OUT_T, PRE_ACC_T> preAggFunc,
            AggFunc<PRE_OUT_T, OUT_T, ACC_T> aggFunc,
            long limit) {
        this.preAggFunc = preAggFunc;
        this.aggFunc = aggFunc;
        this.limit = limit;
    }

    @Override
    public void add(
            RawDataAccumulator<IN_T> target, List<Tuple2<IN_T, Long>> source, long timestamp) {
        for (Tuple2<IN_T, Long> rawData : source) {
            IN_T value = rawData.f0;
            timestamp = rawData.f1;

            if (target.rawDataList.isEmpty() || timestamp >= target.rawDataList.getLast().f1) {
                target.rawDataList.add(Tuple2.of(value, timestamp));
                continue;
            }

            int index = -1;
            for (Tuple2<IN_T, Long> tuple2 : target.rawDataList) {
                index++;
                if (tuple2.f1 > timestamp) {
                    break;
                }
            }
            target.rawDataList.add(index, Tuple2.of(value, timestamp));
        }
    }

    @Override
    public void retract(RawDataAccumulator<IN_T> target, List<Tuple2<IN_T, Long>> source) {
        for (Tuple2<IN_T, Long> value : source) {
            Preconditions.checkState(
                    target.rawDataList.getFirst().equals(value),
                    "Value must be retracted by the order as they added to the AggFuncWithLimit.");
            target.rawDataList.removeFirst();
        }
    }

    @Override
    public OUT_T getResult(RawDataAccumulator<IN_T> accumulator) {
        long numIgnoredData = accumulator.rawDataList.size() - limit;
        long count = 0;
        long timestamp = Long.MIN_VALUE;
        PRE_ACC_T preAcc = preAggFunc.createAccumulator();
        for (Tuple2<IN_T, Long> data : accumulator.rawDataList) {
            count++;
            if (count <= numIgnoredData) {
                continue;
            }
            preAggFunc.add(preAcc, data.f0, data.f1);
            timestamp = data.f1;
        }

        ACC_T acc = aggFunc.createAccumulator();
        aggFunc.add(acc, preAggFunc.getResult(preAcc), timestamp);

        return aggFunc.getResult(acc);
    }

    @Override
    public DataType getResultDatatype() {
        return aggFunc.getResultDatatype();
    }

    @Override
    public RawDataAccumulator<IN_T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }
}
