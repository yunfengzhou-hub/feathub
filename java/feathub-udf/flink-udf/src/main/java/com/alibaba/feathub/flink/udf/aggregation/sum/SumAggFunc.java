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

package com.alibaba.feathub.flink.udf.aggregation.sum;

import com.alibaba.feathub.flink.udf.aggregation.AggFunc;
import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

/** Aggregation functions that calculates the sum of values. */
public abstract class SumAggFunc<IN_T, ACC_T>
        implements AggFunc<IN_T, IN_T, ACC_T>, PreAggFunc<IN_T, IN_T, ACC_T> {}
