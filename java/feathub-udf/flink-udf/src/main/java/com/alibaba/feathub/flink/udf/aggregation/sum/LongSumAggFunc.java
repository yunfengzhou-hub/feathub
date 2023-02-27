package com.alibaba.feathub.flink.udf.aggregation.sum;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of Long values. */
public class LongSumAggFunc extends SumAggFunc<Long, LongSumAggFunc.LongSumAccumulator> {
    @Override
    public void add(LongSumAccumulator accumulator, Long value, long timestamp) {
        accumulator.agg += value;
    }

    @Override
    public void retract(LongSumAccumulator accumulator, Long value, long timestamp) {
        accumulator.agg -= value;
    }

    @Override
    public Long getResult(LongSumAccumulator accumulator) {
        return accumulator.agg;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.BIGINT();
    }

    @Override
    public LongSumAccumulator createAccumulator() {
        return new LongSumAccumulator();
    }

    //        @Override
    //        public void addPreAggResult(LongSumAccumulator target, Long source) {
    //            target.agg += source;
    //        }
    //
    //        @Override
    //        public void retractPreAggResult(LongSumAccumulator target, Long source) {
    //            target.agg -= source;
    //        }

    @Override
    public TypeInformation<LongSumAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(LongSumAccumulator.class);
    }

    /** Accumulator for {@link com.alibaba.feathub.flink.udf.aggregation.sum.LongSumAggFunc}. */
    public static class LongSumAccumulator {
        public long agg = 0L;
    }
}
