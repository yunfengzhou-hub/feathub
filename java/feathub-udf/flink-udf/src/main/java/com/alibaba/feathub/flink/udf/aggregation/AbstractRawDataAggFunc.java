package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;

/** Abstract aggregation function that collects raw data. */
public abstract class AbstractRawDataAggFunc<IN_T, OUT_T>
        implements AggFunc<IN_T, OUT_T, AbstractRawDataAggFunc.RawDataAccumulator> {
    @Override
    public void add(RawDataAccumulator accumulator, IN_T value, long timestamp) {
        if (accumulator.rawDatas.isEmpty() || timestamp >= accumulator.rawDatas.getLast().f1) {
            accumulator.rawDatas.add(Tuple2.of(value, timestamp));
            return;
        }

        int index = -1;
        for (Tuple2<Object, Long> tuple2 : accumulator.rawDatas) {
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
    public void retract(RawDataAccumulator accumulator, IN_T value, long timestamp) {
        Tuple2<Object, Long> tupleToRetract = Tuple2.of(value, timestamp);
        int index = -1;
        for (Tuple2<Object, Long> tuple2 : accumulator.rawDatas) {
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
    public RawDataAccumulator createAccumulator() {
        return new RawDataAccumulator();
    }

    @Override
    public void mergeAccumulator(RawDataAccumulator target, RawDataAccumulator source) {
        if (source.rawDatas.isEmpty()) {
            return;
        }

        if (target.rawDatas.isEmpty()) {
            target.rawDatas.addAll(source.rawDatas);
            return;
        }

        if (target.rawDatas.getLast().f1 > source.rawDatas.getLast().f1) {
            LinkedList<Tuple2<Object, Long>> tmpList = target.rawDatas;
            target.rawDatas = new LinkedList<>();
            target.rawDatas.addAll(source.rawDatas);
            target.rawDatas.addAll(tmpList);
        } else {
            target.rawDatas.addAll(source.rawDatas);
        }
    }

    @Override
    public void retractAccumulator(RawDataAccumulator target, RawDataAccumulator source) {
        for (Tuple2<Object, Long> value : source.rawDatas) {
            this.retract(target, (IN_T) value.f0, value.f1);
        }
    }

    @Override
    public TypeInformation<RawDataAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(RawDataAccumulator.class);
    }

    /** Accumulator for {@link AbstractRawDataAggFunc}. */
    public static class RawDataAccumulator {
        public LinkedList<Tuple2<Object, Long>> rawDatas = new LinkedList<>();
    }
}
