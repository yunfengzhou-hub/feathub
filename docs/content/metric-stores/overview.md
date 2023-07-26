# Overview

A MetricStore provides properties to set the metrics of a Feathub job into an external metric service. A metric refers to a statistic of a characteristic of a feature.

## Metric store configurations

Below are common configurations shared by different metric store implementations. The document for each metric store contains its specific configurations.

| Key                 | Required | Default   | Type    | Description                                                  |
| ------------------- | -------- | --------- | ------- | ------------------------------------------------------------ |
| type                | Required | -         | String  | The type of the metric store to use.                         |
| report_interval_sec | Required | -         | Integer | The interval in seconds to report metrics.                   |
| namespace           | Optional | "default" | String  | The namespace to report metrics to the metric store. Metrics within different namespace will not overwrite each other. |

## Defining metrics

Feathub supports defining metrics at feature's granularity. Below is an example of defining a metric for a feature.

```python
f_total_cost = Feature(
    name="total_cost",
    transform=SlidingWindowTransform(
        expr="cost",
        agg_func="SUM",
        group_by_keys=["name"],
        window_size=timedelta(days=2),
        step_size=timedelta(days=1),
    ),
    metrics=[
    	Count(
          window_size=timedelta(days=1),
      )
    ],
)
```

## Metric reporting format

Metrics reported by metric stores will have the following format by default. Some metric stores might override the default format, and please check the document for each metric store for details.

- Metric name: `"{namespace}_{feature_name}_{metric_type}"`
  - namespace: The namespace of the metric store.
  - feature_name: The name of the host feature.
  - metric_type: The type of the metric.
- Metric labels:

  - namespace: The namespace of the metric store.

  - table_name: The name of the sink where the host features would be written to.

  - feature_name: The name of the host feature.

  - other labels the metric itself has exposed.

## Built-in metrics

Below are Feathub's built-in metrics's class names, their parameters and their exposed tags.

For built-in metrics described below, their metric types equal to their class names.

### Count

Count is a metric that shows the number of features. It has the following parameters:

- filter_expr: Optional. If it is not None, it represents a partial FeatHub expression which evaluates to a boolean value. The partial Feathub expression should be a binary operator whose left child is absent and would be filled in with the host feature name. For example, "IS NULL" will be enriched into "{feature_name} IS NULL". Only features that evaluate this expression into True will be considered when computing the metric.
- window_size: The time range to compute the metric. It should be zero or a positive time span. If it is zero, the metric will be computed from all features that have been processed since the Feathub job is created.

It exposes the following tags:

- filter_expr: The value of the filter_expr parameter.
- window_size_sec: The value of the window_size parameter in seconds.

### Ratio

Ratio is a metric that shows the proportion of the number features that meets filter_expr to the number of all features. It has the following parameters:

- filter_expr: A partial FeatHub expression which evaluates to a boolean value. The partial Feathub expression should be a binary operator whose left child is absent and would be filled in with the host feature name. For example, "IS NULL" will be enriched into "{feature_name} IS NULL". Only features that evaluate this expression into True will be considered when computing the metric.
- window_size: The time range to compute the metric. It should be zero or a positive time span. If it is zero, the metric will be computed from all features that have been processed since the Feathub job is created.

It exposes the following tags:

- filter_expr: The value of the filter_expr parameter.
- window_size_sec: The value of the window_size parameter in seconds.

