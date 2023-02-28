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

import java.io.Serializable;

/**
 * Interface for functions that perform the pre-aggregation process for corresponding {@link
 * AggFunc}s.
 */
public interface PreAggFunc<IN_T, OUT_T, ACC_T> extends Serializable {
    /**
     * Aggregate the value with the timestamp.
     *
     * @param value The value.
     * @param timestamp The timestamp of the value.
     */
    void add(ACC_T acc, IN_T value, long timestamp);

    /** @return The aggregation result. */
    OUT_T getResult(ACC_T acc);

    /** @return The new accumulator of the aggregation function. */
    ACC_T createAccumulator();

    /** Merges the contents of the source accumulator into the target accumulator. */
    void merge(ACC_T target, ACC_T source);

    /** @return The type info of the aggregation result. */
    TypeInformation<OUT_T> getResultTypeInformation();

    /** @return The type info of the accumulator. */
    TypeInformation<ACC_T> getAccumulatorTypeInformation();
}
