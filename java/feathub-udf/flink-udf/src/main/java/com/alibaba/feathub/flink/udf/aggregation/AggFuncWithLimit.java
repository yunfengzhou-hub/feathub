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

    @Override
    public OUT_T getResult(RawDataAccumulator accumulator) {
        long numIgnoredData = accumulator.rawDatas.size() - limit;
        long count = 0;
        ACC_T internalAccumulator = aggFunc.createAccumulator();
        for (Tuple2<Object, Long> data : accumulator.rawDatas) {
            count++;
            if (count <= numIgnoredData) {
                continue;
            }
            aggFunc.add(internalAccumulator, (IN_T) data.f0, data.f1);
        }

        return aggFunc.getResult(internalAccumulator);
    }

    @Override
    public DataType getResultDatatype() {
        return aggFunc.getResultDatatype();
    }
}
