package com.alibaba.feathub.flink.udf.aggregation.minmax;

import java.util.TreeMap;

/** Accumulator for {@link MinMaxAggFunc}. */
public class MinMaxAccumulator {
    public final TreeMap<Comparable<?>, Long> values = new TreeMap<>();
}
