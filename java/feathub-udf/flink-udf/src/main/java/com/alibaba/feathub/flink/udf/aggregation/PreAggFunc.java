package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

public interface PreAggFunc<IN_T, OUT_T, ACC_T> extends Serializable {
    /**
     * Aggregate the value with the timestamp.
     *
     * @param value The value.
     * @param timestamp The timestamp of the value.
     */
    void add(ACC_T accumulator, IN_T value, long timestamp);

    /** @return The aggregation result. */
    OUT_T getResult(ACC_T accumulator);

    /** @return The DataType of the aggregation result. */
    DataType getResultDatatype();

    /** @return The new accumulator of the aggregation function. */
    ACC_T createAccumulator();

    /** @return The type info of the accumulator. */
    TypeInformation<ACC_T> getAccumulatorTypeInformation();

    default OUT_T getResult(IN_T value, long timestamp) {
        ACC_T accumulator = createAccumulator();
        add(accumulator, value, timestamp);
        return getResult(accumulator);
    }
}
