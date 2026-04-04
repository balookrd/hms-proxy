package io.github.mmalykhin.hmsproxy.observability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public final class PrometheusMetrics {
  private static final double[] REQUEST_DURATION_BUCKETS =
      new double[] {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0};

  private final Counter requestsTotal = new Counter(
      "hms_proxy_requests_total",
      "Total HMS proxy requests by method, routed catalog/backend, and terminal status",
      List.of("method", "catalog", "backend", "status"));
  private final Histogram requestDurationSeconds = new Histogram(
      "hms_proxy_request_duration_seconds",
      "HMS proxy request duration in seconds",
      List.of("method", "catalog", "backend"),
      REQUEST_DURATION_BUCKETS);
  private final Counter backendFailuresTotal = new Counter(
      "hms_proxy_backend_failures_total",
      "Backend invocation failures grouped by backend and exception type",
      List.of("backend", "exception"));
  private final Counter backendFallbackTotal = new Counter(
      "hms_proxy_backend_fallback_total",
      "Compatibility fallbacks returned after backend failures",
      List.of("method", "from_api", "to_api"));
  private final Counter routingAmbiguousTotal = new Counter(
      "hms_proxy_routing_ambiguous_total",
      "Requests safely failed because deterministic routing detected conflicting namespaces",
      List.of());
  private final Counter defaultCatalogRoutedTotal = new Counter(
      "hms_proxy_default_catalog_routed_total",
      "Requests routed to the default catalog because no explicit catalog namespace was provided",
      List.of("method"));
  private final Counter rateLimitedTotal = new Counter(
      "hms_proxy_rate_limited_total",
      "Requests rejected by proxy overload protection grouped by limiting dimension, scope, method family, and catalog",
      List.of("dimension", "scope", "method", "method_family", "catalog"));
  private final Counter filteredObjectsTotal = new Counter(
      "hms_proxy_filtered_objects_total",
      "Metadata objects hidden by selective federation filters grouped by method, catalog, and object type",
      List.of("method", "catalog", "object_type"));
  private final Counter syntheticReadLockEventsTotal = new Counter(
      "hms_proxy_synthetic_read_lock_events_total",
      "Synthetic read-lock shim lifecycle events grouped by operation, catalog, store mode, and result",
      List.of("operation", "catalog", "store_mode", "result"));
  private final Counter syntheticReadLockStoreFailuresTotal = new Counter(
      "hms_proxy_synthetic_read_lock_store_failures_total",
      "Synthetic read-lock store failures grouped by operation, store mode, and exception type",
      List.of("operation", "store_mode", "exception"));
  private final Counter syntheticReadLockHandoffsTotal = new Counter(
      "hms_proxy_synthetic_read_lock_handoffs_total",
      "Synthetic read-lock operations served by a different proxy instance than the original lock owner",
      List.of("operation", "catalog", "store_mode"));
  private final Gauge syntheticReadLocksActive = new Gauge(
      "hms_proxy_synthetic_read_locks_active",
      "Current number of active synthetic read locks visible to this proxy instance",
      List.of("store_mode"));
  private final Gauge syntheticReadLockStoreInfo = new Gauge(
      "hms_proxy_synthetic_read_lock_store_info",
      "Configured synthetic read-lock store mode for this proxy instance",
      List.of("store_mode"));

  public void recordRequest(String method, String catalog, String backend, String status, double durationSeconds) {
    requestsTotal.inc(labels("method", method, "catalog", catalog, "backend", backend, "status", status));
    requestDurationSeconds.observe(labels("method", method, "catalog", catalog, "backend", backend), durationSeconds);
  }

  public void recordBackendFailure(String backend, Throwable error) {
    backendFailuresTotal.inc(labels(
        "backend", backend,
        "exception", error == null ? "unknown" : error.getClass().getSimpleName()));
  }

  public void recordBackendFallback(String method, String fromApi, String toApi) {
    backendFallbackTotal.inc(labels("method", method, "from_api", fromApi, "to_api", toApi));
  }

  public void recordRoutingAmbiguous() {
    routingAmbiguousTotal.inc(Map.of());
  }

  public void recordDefaultCatalogRoute(String method) {
    defaultCatalogRoutedTotal.inc(labels("method", method));
  }

  public void recordRateLimited(
      String dimension,
      String scope,
      String method,
      String methodFamily,
      String catalog
  ) {
    rateLimitedTotal.inc(labels(
        "dimension", dimension,
        "scope", scope,
        "method", method,
        "method_family", methodFamily,
        "catalog", catalog));
  }

  public void recordFilteredObject(String method, String catalog, String objectType) {
    recordFilteredObject(method, catalog, objectType, 1L);
  }

  public void recordFilteredObject(String method, String catalog, String objectType, long count) {
    filteredObjectsTotal.add(labels(
        "method", method,
        "catalog", catalog,
        "object_type", objectType), count);
  }

  public void recordSyntheticReadLockEvent(
      String operation,
      String catalog,
      String storeMode,
      String result
  ) {
    recordSyntheticReadLockEvent(operation, catalog, storeMode, result, 1L);
  }

  public void recordSyntheticReadLockEvent(
      String operation,
      String catalog,
      String storeMode,
      String result,
      long count
  ) {
    syntheticReadLockEventsTotal.add(labels(
        "operation", operation,
        "catalog", catalog,
        "store_mode", storeMode,
        "result", result), count);
  }

  public void recordSyntheticReadLockStoreFailure(String operation, String storeMode, Throwable error) {
    syntheticReadLockStoreFailuresTotal.inc(labels(
        "operation", operation,
        "store_mode", storeMode,
        "exception", error == null ? "unknown" : error.getClass().getSimpleName()));
  }

  public void recordSyntheticReadLockHandoff(String operation, String catalog, String storeMode) {
    syntheticReadLockHandoffsTotal.inc(labels(
        "operation", operation,
        "catalog", catalog,
        "store_mode", storeMode));
  }

  public void recordSyntheticReadLockHandoff(String operation, String catalog, String storeMode, long count) {
    syntheticReadLockHandoffsTotal.add(labels(
        "operation", operation,
        "catalog", catalog,
        "store_mode", storeMode), count);
  }

  public void setSyntheticReadLocksActive(String storeMode, long activeLocks) {
    syntheticReadLocksActive.set(labels("store_mode", storeMode), activeLocks);
  }

  public void setSyntheticReadLockStoreMode(String storeMode) {
    syntheticReadLockStoreInfo.set(labels("store_mode", storeMode), 1.0);
  }

  public String render() {
    StringBuilder builder = new StringBuilder(4096);
    requestsTotal.renderInto(builder);
    requestDurationSeconds.renderInto(builder);
    backendFailuresTotal.renderInto(builder);
    backendFallbackTotal.renderInto(builder);
    routingAmbiguousTotal.renderInto(builder);
    defaultCatalogRoutedTotal.renderInto(builder);
    rateLimitedTotal.renderInto(builder);
    filteredObjectsTotal.renderInto(builder);
    syntheticReadLockEventsTotal.renderInto(builder);
    syntheticReadLockStoreFailuresTotal.renderInto(builder);
    syntheticReadLockHandoffsTotal.renderInto(builder);
    syntheticReadLocksActive.renderInto(builder);
    syntheticReadLockStoreInfo.renderInto(builder);
    return builder.toString();
  }

  private static Map<String, String> labels(String... keyValues) {
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("Labels must be provided as key/value pairs");
    }
    Map<String, String> labels = new LinkedHashMap<>();
    for (int index = 0; index < keyValues.length; index += 2) {
      labels.put(keyValues[index], sanitizeLabelValue(keyValues[index + 1]));
    }
    return labels;
  }

  private static String sanitizeLabelValue(String value) {
    if (value == null || value.isBlank()) {
      return "none";
    }
    return value;
  }

  private abstract static class Metric {
    private final String name;
    private final String help;
    private final List<String> labelNames;

    private Metric(String name, String help, List<String> labelNames) {
      this.name = Objects.requireNonNull(name, "name");
      this.help = Objects.requireNonNull(help, "help");
      this.labelNames = List.copyOf(labelNames);
    }

    protected String name() {
      return name;
    }

    protected List<String> labelNames() {
      return labelNames;
    }

    protected void appendHeader(StringBuilder builder, String type) {
      builder.append("# HELP ").append(name).append(' ').append(help).append('\n');
      builder.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    protected static String formatLabels(List<String> labelNames, LabelValues values) {
      if (labelNames.isEmpty()) {
        return "";
      }
      StringBuilder builder = new StringBuilder("{");
      for (int index = 0; index < labelNames.size(); index++) {
        if (index > 0) {
          builder.append(',');
        }
        builder.append(labelNames.get(index))
            .append("=\"")
            .append(escapeLabelValue(values.values().get(index)))
            .append('"');
      }
      builder.append('}');
      return builder.toString();
    }

    private static String escapeLabelValue(String value) {
      return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
  }

  private static final class Counter extends Metric {
    private final ConcurrentMap<LabelValues, LongAdder> values = new ConcurrentHashMap<>();

    private Counter(String name, String help, List<String> labelNames) {
      super(name, help, labelNames);
    }

    private void inc(Map<String, String> labels) {
      add(labels, 1L);
    }

    private void add(Map<String, String> labels, long value) {
      if (value <= 0) {
        return;
      }
      values.computeIfAbsent(LabelValues.from(labelNames(), labels), ignored -> new LongAdder()).add(value);
    }

    private void renderInto(StringBuilder builder) {
      appendHeader(builder, "counter");
      List<Map.Entry<LabelValues, LongAdder>> entries = new ArrayList<>(values.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      if (entries.isEmpty()) {
        builder.append(name()).append(" 0\n");
        return;
      }
      for (Map.Entry<LabelValues, LongAdder> entry : entries) {
        builder.append(name())
            .append(formatLabels(labelNames(), entry.getKey()))
            .append(' ')
            .append(entry.getValue().sum())
            .append('\n');
      }
    }
  }

  private static final class Gauge extends Metric {
    private final ConcurrentMap<LabelValues, DoubleAdder> values = new ConcurrentHashMap<>();

    private Gauge(String name, String help, List<String> labelNames) {
      super(name, help, labelNames);
    }

    private void set(Map<String, String> labels, double value) {
      LabelValues key = LabelValues.from(labelNames(), labels);
      DoubleAdder gauge = values.computeIfAbsent(key, ignored -> new DoubleAdder());
      synchronized (gauge) {
        double current = gauge.sum();
        if (current != 0.0d) {
          gauge.add(-current);
        }
        gauge.add(value);
      }
    }

    private void renderInto(StringBuilder builder) {
      appendHeader(builder, "gauge");
      List<Map.Entry<LabelValues, DoubleAdder>> entries = new ArrayList<>(values.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      if (entries.isEmpty()) {
        builder.append(name()).append(" 0\n");
        return;
      }
      for (Map.Entry<LabelValues, DoubleAdder> entry : entries) {
        builder.append(name())
            .append(formatLabels(labelNames(), entry.getKey()))
            .append(' ')
            .append(entry.getValue().sum())
            .append('\n');
      }
    }
  }

  private static final class Histogram extends Metric {
    private final ConcurrentMap<LabelValues, HistogramSample> values = new ConcurrentHashMap<>();
    private final double[] buckets;

    private Histogram(String name, String help, List<String> labelNames, double[] buckets) {
      super(name, help, labelNames);
      this.buckets = Arrays.copyOf(buckets, buckets.length);
    }

    private void observe(Map<String, String> labels, double value) {
      values.computeIfAbsent(LabelValues.from(labelNames(), labels), ignored -> new HistogramSample(buckets.length))
          .observe(value, buckets);
    }

    private void renderInto(StringBuilder builder) {
      appendHeader(builder, "histogram");
      List<Map.Entry<LabelValues, HistogramSample>> entries = new ArrayList<>(values.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      if (entries.isEmpty()) {
        builder.append(name()).append("_bucket{le=\"+Inf\"} 0\n");
        builder.append(name()).append("_sum 0.0\n");
        builder.append(name()).append("_count 0\n");
        return;
      }
      for (Map.Entry<LabelValues, HistogramSample> entry : entries) {
        HistogramSample sample = entry.getValue();
        long cumulativeCount = 0L;
        for (int index = 0; index < buckets.length; index++) {
          cumulativeCount += sample.bucket(index);
          builder.append(name())
              .append("_bucket")
              .append(formatHistogramLabels(labelNames(), entry.getKey(), buckets[index]))
              .append(' ')
              .append(cumulativeCount)
              .append('\n');
        }
        builder.append(name())
            .append("_bucket")
            .append(formatHistogramLabels(labelNames(), entry.getKey(), Double.POSITIVE_INFINITY))
            .append(' ')
            .append(sample.count())
            .append('\n');
        builder.append(name())
            .append("_sum")
            .append(formatLabels(labelNames(), entry.getKey()))
            .append(' ')
            .append(sample.sum())
            .append('\n');
        builder.append(name())
            .append("_count")
            .append(formatLabels(labelNames(), entry.getKey()))
            .append(' ')
            .append(sample.count())
            .append('\n');
      }
    }

    private String formatHistogramLabels(List<String> labelNames, LabelValues values, double bucket) {
      List<String> extendedNames = new ArrayList<>(labelNames);
      extendedNames.add("le");
      List<String> extendedValues = new ArrayList<>(values.values());
      extendedValues.add(Double.isInfinite(bucket) ? "+Inf" : Double.toString(bucket));
      return formatLabels(extendedNames, new LabelValues(extendedValues));
    }
  }

  private static final class HistogramSample {
    private final LongAdder[] buckets;
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();

    private HistogramSample(int bucketCount) {
      this.buckets = new LongAdder[bucketCount];
      for (int index = 0; index < bucketCount; index++) {
        buckets[index] = new LongAdder();
      }
    }

    private void observe(double value, double[] bucketBounds) {
      count.increment();
      sum.add(value);
      for (int index = 0; index < bucketBounds.length; index++) {
        if (value <= bucketBounds[index]) {
          buckets[index].increment();
          break;
        }
      }
    }

    private long bucket(int index) {
      return buckets[index].sum();
    }

    private long count() {
      return count.sum();
    }

    private double sum() {
      return sum.sum();
    }
  }

  private record LabelValues(List<String> values) implements Comparable<LabelValues> {
    private LabelValues {
      values = List.copyOf(values);
    }

    private static LabelValues from(List<String> labelNames, Map<String, String> labels) {
      List<String> values = new ArrayList<>(labelNames.size());
      for (String labelName : labelNames) {
        values.add(sanitizeLabelValue(labels.get(labelName)));
      }
      return new LabelValues(values);
    }

    @Override
    public int compareTo(LabelValues other) {
      int length = Math.min(values.size(), other.values.size());
      for (int index = 0; index < length; index++) {
        int compared = values.get(index).compareTo(other.values.get(index));
        if (compared != 0) {
          return compared;
        }
      }
      return Integer.compare(values.size(), other.values.size());
    }
  }
}
