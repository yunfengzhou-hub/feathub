package com.alibaba.feathub.flink.udf.aggregation.sum;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** Aggregation functions that calculates the sum of Double values. */
public class DoubleSumAggFunc extends SumAggFunc<Double, DoubleSumAggFunc.DoubleSumAccumulator> {
    @Override
    public void add(DoubleSumAccumulator accumulator, Double value, long timestamp) {
        accumulator.agg += value;
    }

    @Override
    public void retract(DoubleSumAccumulator accumulator, Double value, long timestamp) {
        accumulator.agg -= value;
    }

    @Override
    public Double getResult(DoubleSumAccumulator accumulator) {
        return accumulator.agg;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.DOUBLE();
    }

    @Override
    public DoubleSumAccumulator createAccumulator() {
        return new DoubleSumAccumulator();
    }

    //        @Override
    //        public void addPreAggResult(DoubleSumAccumulator target, Double source) {
    //            target.agg += source;
    //        }
    //
    //        @Override
    //        public void retractPreAggResult(DoubleSumAccumulator target, Double source) {
    //            target.agg -= source;
    //        }

    @Override
    public TypeInformation<DoubleSumAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(DoubleSumAccumulator.class);
    }

    /** Accumulator for {@link com.alibaba.feathub.flink.udf.aggregation.sum.DoubleSumAggFunc}. */
    public static class DoubleSumAccumulator {
        public double agg = 0.0;
    }
}
