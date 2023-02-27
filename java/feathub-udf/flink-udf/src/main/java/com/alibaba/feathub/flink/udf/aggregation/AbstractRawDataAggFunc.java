package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;

/** Abstract aggregation function that collects raw data. */
public abstract class AbstractRawDataAggFunc<IN_T, OUT_T>
        implements AggFunc<IN_T, OUT_T, AbstractRawDataAggFunc.RawDataAccumulator<IN_T>> {
    @Override
    public void add(RawDataAccumulator<IN_T> accumulator, IN_T value, long timestamp) {
        if (accumulator.rawDatas.isEmpty() || timestamp >= accumulator.rawDatas.getLast().f1) {
            accumulator.rawDatas.add(Tuple2.of(value, timestamp));
            return;
        }

        int index = -1;
        for (Tuple2<IN_T, Long> tuple2 : accumulator.rawDatas) {
            index++;
            if (tuple2.f1 > timestamp) {
                accumulator.rawDatas.add(index, Tuple2.of(value, timestamp));
                return;
            }
        }

        throw new RuntimeException(
                String.format(
                        "Cannot add value with timestamp %s to RawDataAccumulator.", timestamp));
    }

    @Override
    public void retract(RawDataAccumulator<IN_T> accumulator, IN_T value, long timestamp) {
        Tuple2<IN_T, Long> tupleToRetract = Tuple2.of(value, timestamp);
        int index = -1;
        for (Tuple2<IN_T, Long> tuple2 : accumulator.rawDatas) {
            index++;
            if (tuple2.equals(tupleToRetract)) {
                accumulator.rawDatas.remove(index);
                return;
            }
        }

        throw new RuntimeException(
                "Value and timestamp "
                        + tupleToRetract
                        + " cannot be found in RawDataAccumulator.");
    }

    @Override
    public RawDataAccumulator<IN_T> createAccumulator() {
        return new RawDataAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }

    /** Accumulator for {@link AbstractRawDataAggFunc}. */
    public static class RawDataAccumulator<T> {
        public LinkedList<Tuple2<T, Long>> rawDatas = new LinkedList<>();
    }
}
