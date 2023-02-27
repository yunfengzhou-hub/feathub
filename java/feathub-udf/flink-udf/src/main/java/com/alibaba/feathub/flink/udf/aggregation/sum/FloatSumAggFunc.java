package com.alibaba.feathub.flink.udf.aggregation.sum;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of Float values. */
public class FloatSumAggFunc extends SumAggFunc<Float, FloatSumAggFunc.FloatSumAccumulator> {
    @Override
    public void add(FloatSumAccumulator accumulator, Float value, long timestamp) {
        accumulator.agg += value;
    }

    @Override
    public void retract(FloatSumAccumulator accumulator, Float value, long timestamp) {
        accumulator.agg -= value;
    }

    @Override
    public Float getResult(FloatSumAccumulator accumulator) {
        return accumulator.agg;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.FLOAT();
    }

    @Override
    public FloatSumAccumulator createAccumulator() {
        return new FloatSumAccumulator();
    }

    //        @Override
    //        public void addPreAggResult(FloatSumAccumulator target, Float source) {
    //            target.agg += source;
    //        }
    //
    //        @Override
    //        public void retractPreAggResult(FloatSumAccumulator target, Float source) {
    //            target.agg -= source;
    //        }

    @Override
    public TypeInformation<FloatSumAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(FloatSumAccumulator.class);
    }

    /** Accumulator for {@link com.alibaba.feathub.flink.udf.aggregation.sum.FloatSumAggFunc}. */
    public static class FloatSumAccumulator {
        public float agg = 0.0f;
    }
}
