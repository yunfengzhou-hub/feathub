package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;

/**
 * An {@link AggFunc} used in sliding window transform's pre-aggregation process.
 *
 * <p>Unless specially optimized, in the pre-aggregation process, aggregation functions collect
 * input values into partial aggregation results and send them to {@link
 * SlidingWindowAggFuncWrapper}, where the final aggregation result is generated.
 */
public class SlidingWindowPreAggFuncWrapper<IN_T, OUT_T, ACC_T>
        implements AggFunc<IN_T, ACC_T, ACC_T> {
    private final AggFunc<IN_T, OUT_T, ACC_T> aggFunc;

    public SlidingWindowPreAggFuncWrapper(AggFunc<IN_T, OUT_T, ACC_T> aggFunc) {
        this.aggFunc = aggFunc;
    }

    @Override
    public ACC_T add(ACC_T accumulator, IN_T value, long timestamp) {
        return aggFunc.add(accumulator, value, timestamp);
    }

    @Override
    public ACC_T merge(ACC_T target, ACC_T source) {
        return aggFunc.merge(target, source);
    }

    @Override
    public ACC_T retract(ACC_T accumulator, IN_T value) {
        return aggFunc.retract(accumulator, value);
    }

    @Override
    public ACC_T retractAccumulator(ACC_T target, ACC_T source) {
        return aggFunc.retractAccumulator(target, source);
    }

    @Override
    public ACC_T getResult(ACC_T accumulator) {
        return accumulator;
    }

    @Override
    public DataType getResultDatatype() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeInformation<ACC_T> getResultTypeInformation() {
        return aggFunc.getAccumulatorTypeInformation();
    }

    @Override
    public ACC_T createAccumulator() {
        return aggFunc.createAccumulator();
    }

    @Override
    public TypeInformation<ACC_T> getAccumulatorTypeInformation() {
        return aggFunc.getAccumulatorTypeInformation();
    }
}
