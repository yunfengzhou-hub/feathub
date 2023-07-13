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

import org.apache.flink.util.Preconditions;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.SimpleCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The ListMetricUpdater updates Prometheus gauge metrics with a list of double values. */
public class ListMetricUpdater implements MetricUpdater<List<Double>> {

    private final List<Gauge.Child> children;

    public ListMetricUpdater(
            String name, List<Map<String, String>> labelsSets, CollectorRegistry registry) {
        verifyLabelsSet(labelsSets);
        this.children = new ArrayList<>();

        SimpleCollector<Gauge.Child> collector =
                Gauge.build()
                        .name(name)
                        .help(name)
                        .labelNames(labelsSets.get(0).keySet().toArray(new String[0]))
                        .register(registry);

        for (Map<String, String> labelsSet : labelsSets) {
            children.add(collector.labels(labelsSet.values().toArray(new String[0])));
        }
    }

    private void verifyLabelsSet(List<Map<String, String>> labelsSet) {
        if (labelsSet.isEmpty()) {
            throw new RuntimeException("List metric must has at least one set of labels.");
        }

        if (labelsSet.get(0).isEmpty()) {
            throw new RuntimeException("List metric must has at least one labels.");
        }

        final Set<String> keySet = labelsSet.get(0).keySet();
        for (Map<String, String> labels : labelsSet) {
            if (keySet.equals(labels)) {
                throw new RuntimeException("All labels set must have the same set of label keys.");
            }
        }
    }

    @Override
    public void update(List<Double> values) {
        Preconditions.checkArgument(
                values.size() == children.size(),
                "List metric size must be the same as the labels set size.");
        for (int i = 0; i < values.size(); ++i) {
            children.get(i).set(values.get(i));
        }
    }
}
