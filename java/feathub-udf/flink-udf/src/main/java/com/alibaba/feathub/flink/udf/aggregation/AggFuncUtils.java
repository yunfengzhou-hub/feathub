/*
 * Copyright 2022 The Feathub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf.aggregation;

import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.avg.AvgAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValueAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValueAggFuncWithoutRetract;
import com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFuncWithoutRetract;
import com.alibaba.feathub.flink.udf.aggregation.sum.SumAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.valuecounts.ValueCountsAggFunc;

/** Utility of aggregation functions. */
public class AggFuncUtils {
    /**
     * Gets the {@link AggFunc} instance used in sliding window transform's aggregation process.
     *
     * @param aggFuncName The name of the aggregation function.
     * @param inDataType the data type of the input values.
     */
    public static AggFunc<?, ?, ?> getSlidingWindowAggFunc(
            String aggFuncName, DataType inDataType) {
        if ("SUM".equals(aggFuncName)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFuncName)) {
            return new SlidingWindowAggFuncWrapper<>(new AvgAggFunc<>(getSumAggFunc(inDataType)));
        } else if ("FIRST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFunc<>(inDataType, true);
        } else if ("LAST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFunc<>(inDataType, false);
        } else if ("MAX".equals(aggFuncName)) {
            return new MinMaxAggFunc<>(inDataType, false);
        } else if ("MIN".equals(aggFuncName)) {
            return new MinMaxAggFunc<>(inDataType, true);
        } else if ("COUNT".equals(aggFuncName) || "ROW_NUMBER".equals(aggFuncName)) {
            return new SlidingWindowAggFuncWrapper<>(new CountAggFunc());
        } else if ("VALUE_COUNTS".equals(aggFuncName)) {
            return new SlidingWindowAggFuncWrapper<>(new ValueCountsAggFunc(inDataType));
        }

        throw new RuntimeException(
                String.format("Unsupported aggregation function %s", aggFuncName));
    }

    /**
     * Gets the {@link AggFunc} instance used in sliding window transform's pre-aggregation process.
     *
     * <p>In the pre-aggregation process, values would not be retracted from accumulators, so the
     * returned {@link AggFunc}s could be instances of {@link AggFuncWithoutRetract}.
     *
     * @param aggFuncName The name of the aggregation function.
     * @param inDataType the data type of the input values.
     */
    public static AggFunc<?, ?, ?> getSlidingWindowPreAggFunc(
            String aggFuncName, DataType inDataType) {
        if ("SUM".equals(aggFuncName)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFuncName)) {
            return new SlidingWindowPreAggFuncWrapper<>(
                    new AvgAggFunc<>(getSumAggFunc(inDataType)));
        } else if ("FIRST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFuncWithoutRetract<>(inDataType, true);
        } else if ("LAST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFuncWithoutRetract<>(inDataType, false);
        } else if ("MAX".equals(aggFuncName)) {
            return new MinMaxAggFuncWithoutRetract<>(inDataType, false);
        } else if ("MIN".equals(aggFuncName)) {
            return new MinMaxAggFuncWithoutRetract<>(inDataType, true);
        } else if ("COUNT".equals(aggFuncName) || "ROW_NUMBER".equals(aggFuncName)) {
            return new SlidingWindowPreAggFuncWrapper<>(new CountAggFunc());
        } else if ("VALUE_COUNTS".equals(aggFuncName)) {
            return new SlidingWindowPreAggFuncWrapper<>(new ValueCountsAggFunc(inDataType));
        }

        throw new RuntimeException(
                String.format("Unsupported aggregation function %s", aggFuncName));
    }

    /**
     * Get the AggFunc implementation by the given function name and input data type.
     *
     * @param aggFuncName The name of the aggregation function.
     * @param inDataType The input data type of the aggregation function.
     */
    public static AggFunc<?, ?, ?> getAggFunc(String aggFuncName, DataType inDataType) {
        if ("SUM".equals(aggFuncName)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFuncName)) {
            return new AvgAggFunc<>(getSumAggFunc(inDataType));
        } else if ("FIRST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFunc<>(inDataType, true);
        } else if ("LAST_VALUE".equals(aggFuncName)) {
            return new FirstLastValueAggFunc<>(inDataType, false);
        } else if ("MAX".equals(aggFuncName)) {
            return new MinMaxAggFunc<>(inDataType, false);
        } else if ("MIN".equals(aggFuncName)) {
            return new MinMaxAggFunc<>(inDataType, true);
        } else if ("COUNT".equals(aggFuncName) || "ROW_NUMBER".equals(aggFuncName)) {
            return new CountAggFunc();
        } else if ("VALUE_COUNTS".equals(aggFuncName)) {
            return new ValueCountsAggFunc(inDataType);
        }

        throw new RuntimeException(
                String.format("Unsupported aggregation function %s", aggFuncName));
    }

    @SuppressWarnings({"unchecked"})
    public static <IN_T> SumAggFunc<IN_T> getSumAggFunc(DataType inDataType) {
        final Class<?> inClass = inDataType.getConversionClass();
        if (inClass.equals(Integer.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.IntSumAggFunc();
        } else if (inClass.equals(Long.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.LongSumAggFunc();
        } else if (inClass.equals(Float.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.FloatSumAggFunc();
        } else if (inClass.equals(Double.class)) {
            return (SumAggFunc<IN_T>) new SumAggFunc.DoubleSumAggFunc();
        }
        throw new RuntimeException(
                String.format("Unsupported type for AvgAggregationFunction %s.", inDataType));
    }
}
