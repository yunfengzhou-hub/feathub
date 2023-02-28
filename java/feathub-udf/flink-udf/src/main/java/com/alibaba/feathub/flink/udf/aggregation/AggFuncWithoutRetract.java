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

/**
 * Parent class of {@link AggFunc} child classes that does not support retractions.
 *
 * <p>Some aggregation functions, like {@link
 * com.alibaba.feathub.flink.udf.aggregation.minmax.MinMaxAggFunc}, have optimized implementations
 * when retraction is disabled, and this class provides a unified parent class for these
 * implementation classes.
 */
public abstract class AggFuncWithoutRetract<IN_T, OUT_T, ACC_T>
        implements AggFunc<IN_T, OUT_T, ACC_T> {
    @Override
    public final ACC_T retract(ACC_T accumulator, IN_T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final ACC_T retractAccumulator(ACC_T target, ACC_T source) {
        throw new UnsupportedOperationException();
    }
}
