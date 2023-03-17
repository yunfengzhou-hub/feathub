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
import com.alibaba.feathub.flink.udf.aggregation.avg.AvgPreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountPreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValueAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.firstlastvalue.FirstLastValuePreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.SumAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.valuecounts.ValueCountsAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.valuecounts.ValueCountsPreAggFunc;

/** Utility of aggregation functions. */
public class AggFuncUtils {
    /**
     * Get the PreAggFunc implementation by the given function name and input data type.
     *
     * @param aggFunc The name of the aggregation function.
     * @param inDataType The input data type of the aggregation function.
     */
    public static PreAggFunc<?, ?, ?> getPreAggFunc(
            String aggFunc, DataType inDataType, Long limit) {
        if (limit != null) {
            return new PreAggFuncWithLimit<>(inDataType);
        }

        if ("SUM".equals(aggFunc)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFunc)) {
            return new AvgPreAggFunc<>(getSumAggFunc(inDataType));
        } else if ("FIRST_VALUE".equals(aggFunc)) {
            return new FirstLastValuePreAggFunc<>(inDataType, true);
        } else if ("LAST_VALUE".equals(aggFunc)) {
            return new FirstLastValuePreAggFunc<>(inDataType, false);
        } else if ("MAX".equals(aggFunc)) {
            return new MinMaxAggFunc<>(inDataType, false);
        } else if ("MIN".equals(aggFunc)) {
            return new MinMaxAggFunc<>(inDataType, true);
        } else if ("COUNT".equals(aggFunc) || "ROW_NUMBER".equals(aggFunc)) {
            return new CountPreAggFunc();
        } else if ("VALUE_COUNTS".equals(aggFunc)) {
            return new ValueCountsPreAggFunc(inDataType);
        }

        throw new RuntimeException(String.format("Unsupported aggregation function %s", aggFunc));
    }

    /**
     * Get the AggFunc implementation by the given function name and input data type.
     *
     * @param aggFuncName The name of the aggregation function.
     * @param inDataType The input data type of the aggregation function.
     */
    public static AggFunc<?, ?, ?> getAggFunc(String aggFuncName, DataType inDataType, Long limit) {
        AggFunc<?, ?, ?> aggFunc = getAggFunc(aggFuncName, inDataType);

        if (limit != null) {
            PreAggFunc<?, ?, ?> preAggFunc = getPreAggFunc(aggFuncName, inDataType, null);
            return new AggFuncWithLimit(preAggFunc, aggFunc, limit);
        }

        return aggFunc;
    }

    private static AggFunc<?, ?, ?> getAggFunc(String aggFunc, DataType inDataType) {
        if ("SUM".equals(aggFunc)) {
            return getSumAggFunc(inDataType);
        } else if ("AVG".equals(aggFunc)) {
            return new AvgAggFunc(inDataType);
        } else if ("FIRST_VALUE".equals(aggFunc)) {
            return new FirstLastValueAggFunc<>(inDataType, true);
        } else if ("LAST_VALUE".equals(aggFunc)) {
            return new FirstLastValueAggFunc<>(inDataType, false);
        } else if ("MAX".equals(aggFunc)) {
            return new MinMaxAggFunc<>(inDataType, false);
        } else if ("MIN".equals(aggFunc)) {
            return new MinMaxAggFunc<>(inDataType, true);
        } else if ("COUNT".equals(aggFunc) || "ROW_NUMBER".equals(aggFunc)) {
            return new CountAggFunc();
        } else if ("VALUE_COUNTS".equals(aggFunc)) {
            return new ValueCountsAggFunc(inDataType);
        }

        throw new RuntimeException(String.format("Unsupported aggregation function %s", aggFunc));
    }

    @SuppressWarnings({"unchecked"})
    private static <IN_T> SumAggFunc<IN_T, ?> getSumAggFunc(DataType inDataType) {
        final Class<?> inClass = inDataType.getConversionClass();
        if (inClass.equals(Integer.class)) {
            return (SumAggFunc<IN_T, ?>) new SumAggFunc.IntSumAggFunc();
        } else if (inClass.equals(Long.class)) {
            return (SumAggFunc<IN_T, ?>) new SumAggFunc.LongSumAggFunc();
        } else if (inClass.equals(Float.class)) {
            return (SumAggFunc<IN_T, ?>) new SumAggFunc.FloatSumAggFunc();
        } else if (inClass.equals(Double.class)) {
            return (SumAggFunc<IN_T, ?>) new SumAggFunc.DoubleSumAggFunc();
        }
        throw new RuntimeException(
                String.format("Unsupported type for AvgAggregationFunction %s.", inDataType));
    }
}
