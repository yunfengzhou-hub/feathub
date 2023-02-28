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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

// TODO: Update AbstractTimeWindowedAggFunc to reuse the implementations of AggFunc.
/**
 * Interface of aggregation function. The aggregation function can aggregate any number of records
 * with its timestamp and get the aggregation result.
 */
public interface AggFunc<IN_T, OUT_T, ACC_T> extends Serializable {
    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * <p>For efficiency, the input accumulator may be modified and returned.
     *
     * @param value The value.
     * @param timestamp The timestamp of the value.
     */
    ACC_T add(ACC_T accumulator, IN_T value, long timestamp);

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     *
     * <p>This function may reuse any of the given accumulators as the target for the merge and
     * return that. The assumption is that the given accumulators will not be used any more after
     * having been passed to this function.
     */
    ACC_T merge(ACC_T target, ACC_T source);

    /**
     * Retracts the given input value from the given accumulator, returning the new accumulator
     * value.
     *
     * <p>For efficiency, the input accumulator may be modified and returned.
     *
     * @param value The value to be retracted.
     */
    ACC_T retract(ACC_T accumulator, IN_T value);

    /**
     * Retracts the contents of the source accumulator from the target accumulator, returning an
     * accumulator with the merged state.
     *
     * <p>This function may reuse any of the given accumulators as the target for the merge and
     * return that. The assumption is that the given accumulators will not be used any more after
     * having been passed to this function.
     */
    ACC_T retractAccumulator(ACC_T target, ACC_T source);

    /** @return The aggregation result. */
    OUT_T getResult(ACC_T accumulator);

    /** @return The DataType of the aggregation result. */
    DataType getResultDatatype();

    /** @return The type info of the aggregation result. */
    default TypeInformation<OUT_T> getResultTypeInformation() {
        return ExternalTypeInfo.of(getResultDatatype());
    }

    /** @return The new accumulator of the aggregation function. */
    ACC_T createAccumulator();

    /** @return The type info of the accumulator. */
    TypeInformation<ACC_T> getAccumulatorTypeInformation();
}
