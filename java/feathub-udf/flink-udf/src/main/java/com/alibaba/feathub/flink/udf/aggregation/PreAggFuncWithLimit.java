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
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import java.util.LinkedList;
import java.util.List;

/** The function that performs the pre-aggregation process for aggregation functions with limit. */
public class PreAggFuncWithLimit<T>
        implements PreAggFunc<T, List<Tuple2<T, Long>>, RawDataAccumulator<T>> {
    DataType inDataType;

    public PreAggFuncWithLimit(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public List<Tuple2<T, Long>> getResult(RawDataAccumulator<T> acc) {
        return acc.rawDataList;
    }

    @Override
    public RawDataAccumulator<T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public void add(RawDataAccumulator<T> acc, T value, long timestamp) {
        if (acc.rawDataList.isEmpty() || timestamp >= acc.rawDataList.getLast().f1) {
            acc.rawDataList.add(Tuple2.of(value, timestamp));
            return;
        }

        int index = -1;
        for (Tuple2<T, Long> tuple2 : acc.rawDataList) {
            index++;
            if (tuple2.f1 > timestamp) {
                break;
            }
        }

        acc.rawDataList.add(index, Tuple2.of(value, timestamp));
    }

    @Override
    public void merge(RawDataAccumulator<T> target, RawDataAccumulator<T> source) {
        if (source.rawDataList.isEmpty()) {
            return;
        }

        if (target.rawDataList.isEmpty()) {
            target.rawDataList.addAll(source.rawDataList);
            return;
        }

        if (target.rawDataList.getLast().f1 > source.rawDataList.getLast().f1) {
            LinkedList<Tuple2<T, Long>> tmpList = target.rawDataList;
            target.rawDataList = new LinkedList<>();
            target.rawDataList.addAll(source.rawDataList);
            target.rawDataList.addAll(tmpList);
        } else {
            target.rawDataList.addAll(source.rawDataList);
        }
    }

    @Override
    public TypeInformation getResultTypeInformation() {
        return Types.LIST(Types.TUPLE(ExternalTypeInfo.of(inDataType), Types.LONG));
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }
}
