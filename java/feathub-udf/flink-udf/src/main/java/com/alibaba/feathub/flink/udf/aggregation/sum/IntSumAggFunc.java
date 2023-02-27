package com.alibaba.feathub.flink.udf.aggregation.sum;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of Integer values. */
public class IntSumAggFunc extends SumAggFunc<Integer, IntSumAggFunc.IntSumAccumulator> {

    @Override
    public void add(IntSumAccumulator accumulator, Integer value, long timestamp) {
        accumulator.agg += value;
    }

    @Override
    public void retract(IntSumAccumulator accumulator, Integer value, long timestamp) {
        accumulator.agg -= value;
    }

    @Override
    public Integer getResult(IntSumAccumulator accumulator) {
        return accumulator.agg;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.INT();
    }

    @Override
    public IntSumAccumulator createAccumulator() {
        return new IntSumAccumulator();
    }

    @Override
    public TypeInformation<IntSumAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(IntSumAccumulator.class);
    }

    /** Accumulator for {@link com.alibaba.feathub.flink.udf.aggregation.sum.IntSumAggFunc}. */
    public static class IntSumAccumulator {
        public int agg = 0;
    }
}
