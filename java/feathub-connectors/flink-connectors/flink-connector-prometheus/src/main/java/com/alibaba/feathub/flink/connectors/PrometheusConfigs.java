/*
 * Copyright 2022 The FeatHub Authors
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

package com.alibaba.feathub.flink.connectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

/** Configurations used by Prometheus sink. */
public class PrometheusConfigs {

    public static final ConfigOption<String> HOST_URL =
            ConfigOptions.key("hostUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The PushGateway server host URL including scheme, host name, and port.");

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The job name under which metrics will be pushed.");

    public static final ConfigOption<Boolean> DELETE_ON_SHUTDOWN =
            ConfigOptions.key("deleteOnShutdown")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specifies whether to delete metrics from the PushGateway on shutdown."
                                                    + " Flink will try its best to delete the metrics but this is not guaranteed.")
                                    .build());

    public static final ConfigOption<String> GROUPING_KEY =
            ConfigOptions.key("groupingKey")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Specifies the grouping keys which are the group and global labels of all metrics pushed by this sink. "
                                    + "The label name and value are separated by '=', and labels are separated by ';', "
                                    + "e.g., k1=v1;k2=v2. Please ensure that your grouping key meets the "
                                    + "Prometheus requirements.");
}
