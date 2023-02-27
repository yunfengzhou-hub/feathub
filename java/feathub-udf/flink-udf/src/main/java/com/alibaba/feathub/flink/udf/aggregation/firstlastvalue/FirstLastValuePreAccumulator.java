package com.alibaba.feathub.flink.udf.aggregation.firstlastvalue;

public class FirstLastValuePreAccumulator<T> {
    T value;
    Long timestamp = Long.MIN_VALUE;
}
