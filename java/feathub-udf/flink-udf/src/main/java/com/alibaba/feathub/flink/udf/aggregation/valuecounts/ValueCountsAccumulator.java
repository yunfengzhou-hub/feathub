package com.alibaba.feathub.flink.udf.aggregation.valuecounts;

import java.util.HashMap;
import java.util.Map;

/** Accumulator for {@link ValueCountsAccumulator}. */
public class ValueCountsAccumulator {
    public final Map<Object, Long> valueCounts = new HashMap<>();
}
