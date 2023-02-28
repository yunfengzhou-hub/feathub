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

package com.alibaba.feathub.flink.udf.aggregation.avg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAccumulator;
import com.alibaba.feathub.flink.udf.aggregation.count.CountPreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.SumAggFunc;

/** The {@link PreAggFunc} for {@link AvgAggFunc}. */
public class AvgPreAggFunc<IN_T extends Number, ACC_T>
        implements PreAggFunc<IN_T, Row, Tuple2<CountAccumulator, ACC_T>> {
    private final CountPreAggFunc countPreAggFunc;
    private final SumAggFunc<IN_T, ACC_T> sumPreAggFunc;

    public AvgPreAggFunc(SumAggFunc<IN_T, ACC_T> sumPreAggFunc) {
        this.countPreAggFunc = new CountPreAggFunc();
        this.sumPreAggFunc = sumPreAggFunc;
    }

    @Override
    public void add(Tuple2<CountAccumulator, ACC_T> accumulator, IN_T value, long timestamp) {
        countPreAggFunc.add(accumulator.f0, value, timestamp);
        sumPreAggFunc.add(accumulator.f1, value, timestamp);
    }

    @Override
    public void merge(
            Tuple2<CountAccumulator, ACC_T> target, Tuple2<CountAccumulator, ACC_T> source) {
        countPreAggFunc.merge(target.f0, source.f0);
        sumPreAggFunc.merge(target.f1, source.f1);
    }

    @Override
    public Row getResult(Tuple2<CountAccumulator, ACC_T> accumulator) {
        return Row.of(
                sumPreAggFunc.getResult(accumulator.f1), countPreAggFunc.getResult(accumulator.f0));
    }

    @Override
    public TypeInformation<Row> getResultTypeInformation() {
        return Types.ROW(
                sumPreAggFunc.getResultTypeInformation(),
                countPreAggFunc.getResultTypeInformation());
    }

    @Override
    public TypeInformation<Tuple2<CountAccumulator, ACC_T>> getAccumulatorTypeInformation() {
        return Types.TUPLE(
                countPreAggFunc.getAccumulatorTypeInformation(),
                sumPreAggFunc.getAccumulatorTypeInformation());
    }

    @Override
    public Tuple2<CountAccumulator, ACC_T> createAccumulator() {
        return Tuple2.of(countPreAggFunc.createAccumulator(), sumPreAggFunc.createAccumulator());
    }
}
