package io.github.mmalykhin.hmsproxy.observability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public final class PrometheusMetrics {
  private static final double[] REQUEST_DURATION_BUCKETS =
      new double[] {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0};

  static final int DEFAULT_MAX_SERIES_PER_METRIC = 5000;
  static final String OVERFLOW_LABEL_VALUE = "overflow";
  static final String UNKNOWN_EXCEPTION_LABEL_VALUE = "other";

  private static final Set<String> KNOWN_EXCEPTION_SIMPLE_NAMES = Set.of(
      "IllegalStateException", "IllegalArgumentException", "NullPointerException",
      "UnsupportedOperationException", "ClassCastException", "ArithmeticException",
      "ArrayIndexOutOfBoundsException", "IndexOutOfBoundsException", "NumberFormatException",
      "ConcurrentModificationException", "RuntimeException",
      "InterruptedException", "TimeoutException", "ExecutionException",
      "CancellationException", "RejectedExecutionException",
      "IOException", "EOFException", "FileNotFoundException", "InterruptedIOException",
      "SocketException", "SocketTimeoutException", "ConnectException", "UnknownHostException",
      "MetaException", "NoSuchObjectException", "AlreadyExistsException",
      "InvalidObjectException", "InvalidOperationException", "UnknownDBException",
      "UnknownTableException", "InvalidPartitionException", "UnknownPartitionException",
      "ConfigValSecurityException", "InvalidInputException", "NoSuchTxnException",
      "TxnAbortedException", "TxnOpenException",
      "TException", "TApplicationException", "TTransportException", "TProtocolException",
      "RateLimitExceededException", "ProxyConfigurationException",
      "KeeperException", "ConnectionLossException", "SessionExpiredException");

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
  private final Counter lockRequestSplitTotal = new Counter(
      "hms_proxy_lock_request_split_total",
      "Lock requests spanning several catalogs, routed to one catalog with the other components dropped",
      List.of("catalog"));
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
  private final Counter backendSessionAcquireTimeoutsTotal = new Counter(
      "hms_proxy_backend_session_acquire_timeouts_total",
      "Backend metastore session acquisitions that timed out waiting for a free pool permit",
      List.of("catalog", "operation"));
  private final Counter adaptiveTimeoutReconnectTotal = new Counter(
      "hms_proxy_adaptive_timeout_reconnect_total",
      "Adaptive socket timeout changes that triggered a backend client reconnect grouped by catalog",
      List.of("catalog"));
  private final Counter adaptiveTimeoutReconnectSkippedTotal = new Counter(
      "hms_proxy_adaptive_timeout_reconnect_skipped_total",
      "Adaptive socket timeout reconnects suppressed by hysteresis or cooldown grouped by catalog and reason",
      List.of("catalog", "reason"));
  private final Counter syntheticReadLockHandoffsTotal = new Counter(
      "hms_proxy_synthetic_read_lock_handoffs_total",
      "Synthetic read-lock operations served by a different proxy instance than the original lock owner",
      List.of("operation", "catalog", "store_mode"));
  private final Gauge impersonationPoolUsers = new Gauge(
      "hms_proxy_impersonation_pool_users",
      "Distinct users currently holding a per-user impersonation session pool, grouped by catalog",
      List.of("catalog"));
  private final Gauge impersonationPoolSessions = new Gauge(
      "hms_proxy_impersonation_pool_sessions",
      "Per-user impersonation backend sessions grouped by catalog and pool state (active or idle)",
      List.of("catalog", "state"));
  private final Counter impersonationSessionAcquireTimeoutsTotal = new Counter(
      "hms_proxy_impersonation_session_acquire_timeouts_total",
      "Per-user impersonation pool borrow attempts that timed out waiting for a free session",
      List.of("catalog"));
  private final Counter impersonationSessionEvictionsTotal = new Counter(
      "hms_proxy_impersonation_session_evictions_total",
      "Per-user impersonation backend sessions discarded grouped by catalog and reason",
      List.of("catalog", "reason"));
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
        "exception", classifyException(error)));
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

  public void recordLockRequestSplit(String catalog) {
    lockRequestSplitTotal.inc(labels("catalog", catalog));
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

  public void recordBackendSessionAcquireTimeout(String catalog, String operation) {
    backendSessionAcquireTimeoutsTotal.inc(labels("catalog", catalog, "operation", operation));
  }

  public void setImpersonationPoolUsers(String catalog, long users) {
    impersonationPoolUsers.set(labels("catalog", catalog), users);
  }

  public void setImpersonationPoolSessions(String catalog, long active, long idle) {
    impersonationPoolSessions.set(labels("catalog", catalog, "state", "active"), active);
    impersonationPoolSessions.set(labels("catalog", catalog, "state", "idle"), idle);
  }

  public void recordImpersonationSessionAcquireTimeout(String catalog) {
    impersonationSessionAcquireTimeoutsTotal.inc(labels("catalog", catalog));
  }

  public void recordImpersonationSessionEviction(String catalog, String reason) {
    impersonationSessionEvictionsTotal.inc(labels("catalog", catalog, "reason", reason));
  }

  public void recordAdaptiveTimeoutReconnect(String catalog) {
    adaptiveTimeoutReconnectTotal.inc(labels("catalog", catalog));
  }

  public void recordAdaptiveTimeoutReconnectSkipped(String catalog, String reason) {
    adaptiveTimeoutReconnectSkippedTotal.inc(labels("catalog", catalog, "reason", reason));
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
        "exception", classifyException(error)));
  }

  static String classifyException(Throwable error) {
    if (error == null) {
      return "unknown";
    }
    String simpleName = error.getClass().getSimpleName();
    if (simpleName == null || simpleName.isEmpty()) {
      return UNKNOWN_EXCEPTION_LABEL_VALUE;
    }
    if (KNOWN_EXCEPTION_SIMPLE_NAMES.contains(simpleName)) {
      return simpleName;
    }
    return UNKNOWN_EXCEPTION_LABEL_VALUE;
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

  // Declared last so every metric field above is already initialized; render order is the
  // exposition order of /metrics.
  private final List<Metric> exposedMetrics = List.of(
      requestsTotal,
      requestDurationSeconds,
      backendFailuresTotal,
      backendFallbackTotal,
      routingAmbiguousTotal,
      defaultCatalogRoutedTotal,
      lockRequestSplitTotal,
      rateLimitedTotal,
      backendSessionAcquireTimeoutsTotal,
      impersonationPoolUsers,
      impersonationPoolSessions,
      impersonationSessionAcquireTimeoutsTotal,
      impersonationSessionEvictionsTotal,
      adaptiveTimeoutReconnectTotal,
      adaptiveTimeoutReconnectSkippedTotal,
      filteredObjectsTotal,
      syntheticReadLockEventsTotal,
      syntheticReadLockStoreFailuresTotal,
      syntheticReadLockHandoffsTotal,
      syntheticReadLocksActive,
      syntheticReadLockStoreInfo);

  public String render() {
    int estimatedSize = 0;
    for (Metric metric : exposedMetrics) {
      estimatedSize += metric.estimatedRenderSize();
    }
    StringBuilder builder = new StringBuilder(estimatedSize);
    for (Metric metric : exposedMetrics) {
      metric.renderInto(builder);
    }
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
    private final int maxSeries;
    private final LabelValues overflowKey;
    private final AtomicInteger admittedSeries = new AtomicInteger();

    private Metric(String name, String help, List<String> labelNames) {
      this(name, help, labelNames, DEFAULT_MAX_SERIES_PER_METRIC);
    }

    private Metric(String name, String help, List<String> labelNames, int maxSeries) {
      this.name = Objects.requireNonNull(name, "name");
      this.help = Objects.requireNonNull(help, "help");
      this.labelNames = List.copyOf(labelNames);
      if (maxSeries < 1) {
        throw new IllegalArgumentException("maxSeries must be positive");
      }
      this.maxSeries = maxSeries;
      this.overflowKey = this.labelNames.isEmpty()
          ? null
          : new LabelValues(Collections.nCopies(this.labelNames.size(), OVERFLOW_LABEL_VALUE));
    }

    protected String name() {
      return name;
    }

    protected List<String> labelNames() {
      return labelNames;
    }

    /**
     * Returns the sample for {@code requested}, creating it only while the metric still has room
     * for a fresh series. Admission reserves a slot with a CAS before the map insert, so racing
     * threads at the cardinality boundary cannot push the metric past {@code maxSeries} distinct
     * label combinations; everything beyond the cap collapses into the single overflow series.
     */
    protected <V> V resolveSeries(
        ConcurrentMap<LabelValues, V> series,
        LabelValues requested,
        Supplier<V> factory
    ) {
      V existing = series.get(requested);
      if (existing != null) {
        return existing;
      }
      if (overflowKey == null) {
        return series.computeIfAbsent(requested, ignored -> factory.get());
      }
      while (true) {
        int admitted = admittedSeries.get();
        if (admitted >= maxSeries) {
          return series.computeIfAbsent(overflowKey, ignored -> factory.get());
        }
        if (admittedSeries.compareAndSet(admitted, admitted + 1)) {
          break;
        }
      }
      boolean[] created = new boolean[1];
      V value = series.computeIfAbsent(requested, ignored -> {
        created[0] = true;
        return factory.get();
      });
      if (!created[0]) {
        admittedSeries.decrementAndGet();
      }
      return value;
    }

    protected void appendHeader(StringBuilder builder, String type) {
      builder.append("# HELP ").append(name).append(' ').append(help).append('\n');
      builder.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    /** Rough exposition size used to size the render buffer up front. */
    abstract int estimatedRenderSize();

    abstract void renderInto(StringBuilder builder);

    protected int estimatedHeaderSize() {
      return 2 * name.length() + help.length() + 24;
    }

    protected int estimatedSampleLineSize() {
      int size = name.length() + 24;
      for (String labelName : labelNames) {
        size += labelName.length() + 24;
      }
      return size;
    }

    protected static void appendLabels(StringBuilder builder, List<String> labelNames, LabelValues values) {
      appendLabels(builder, labelNames, values, null, null);
    }

    protected static void appendLabels(
        StringBuilder builder,
        List<String> labelNames,
        LabelValues values,
        String extraName,
        String extraValue
    ) {
      if (labelNames.isEmpty() && extraName == null) {
        return;
      }
      builder.append('{');
      for (int index = 0; index < labelNames.size(); index++) {
        if (index > 0) {
          builder.append(',');
        }
        builder.append(labelNames.get(index)).append("=\"");
        appendEscapedLabelValue(builder, values.values().get(index));
        builder.append('"');
      }
      if (extraName != null) {
        if (!labelNames.isEmpty()) {
          builder.append(',');
        }
        builder.append(extraName).append("=\"");
        appendEscapedLabelValue(builder, extraValue);
        builder.append('"');
      }
      builder.append('}');
    }

    private static void appendEscapedLabelValue(StringBuilder builder, String value) {
      for (int index = 0; index < value.length(); index++) {
        char current = value.charAt(index);
        switch (current) {
          case '\\' -> builder.append("\\\\");
          case '"' -> builder.append("\\\"");
          case '\n' -> builder.append("\\n");
          default -> builder.append(current);
        }
      }
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
      resolveSeries(values, LabelValues.from(labelNames(), labels), LongAdder::new).add(value);
    }

    @Override
    int estimatedRenderSize() {
      return estimatedHeaderSize() + Math.max(values.size(), 1) * estimatedSampleLineSize();
    }

    @Override
    void renderInto(StringBuilder builder) {
      appendHeader(builder, "counter");
      List<Map.Entry<LabelValues, LongAdder>> entries = new ArrayList<>(values.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      if (entries.isEmpty()) {
        builder.append(name()).append(" 0\n");
        return;
      }
      for (Map.Entry<LabelValues, LongAdder> entry : entries) {
        builder.append(name());
        appendLabels(builder, labelNames(), entry.getKey());
        builder.append(' ').append(entry.getValue().sum()).append('\n');
      }
    }
  }

  private static final class Gauge extends Metric {
    private final ConcurrentMap<LabelValues, AtomicLong> values = new ConcurrentHashMap<>();

    private Gauge(String name, String help, List<String> labelNames) {
      super(name, help, labelNames);
    }

    private void set(Map<String, String> labels, double value) {
      resolveSeries(values, LabelValues.from(labelNames(), labels), AtomicLong::new)
          .set(Double.doubleToRawLongBits(value));
    }

    @Override
    int estimatedRenderSize() {
      return estimatedHeaderSize() + Math.max(values.size(), 1) * estimatedSampleLineSize();
    }

    @Override
    void renderInto(StringBuilder builder) {
      appendHeader(builder, "gauge");
      List<Map.Entry<LabelValues, AtomicLong>> entries = new ArrayList<>(values.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      if (entries.isEmpty()) {
        builder.append(name()).append(" 0\n");
        return;
      }
      for (Map.Entry<LabelValues, AtomicLong> entry : entries) {
        builder.append(name());
        appendLabels(builder, labelNames(), entry.getKey());
        builder.append(' ').append(Double.longBitsToDouble(entry.getValue().get())).append('\n');
      }
    }
  }

  private static final class Histogram extends Metric {
    private static final String POSITIVE_INFINITY_LABEL = "+Inf";

    private final ConcurrentMap<LabelValues, HistogramSample> values = new ConcurrentHashMap<>();
    private final double[] buckets;
    private final String[] bucketLabels;

    private Histogram(String name, String help, List<String> labelNames, double[] buckets) {
      super(name, help, labelNames);
      this.buckets = Arrays.copyOf(buckets, buckets.length);
      this.bucketLabels = new String[this.buckets.length];
      for (int index = 0; index < this.buckets.length; index++) {
        bucketLabels[index] = Double.isInfinite(this.buckets[index])
            ? POSITIVE_INFINITY_LABEL
            : Double.toString(this.buckets[index]);
      }
    }

    private void observe(Map<String, String> labels, double value) {
      resolveSeries(
          values,
          LabelValues.from(labelNames(), labels),
          () -> new HistogramSample(buckets.length)).observe(value, buckets);
    }

    @Override
    int estimatedRenderSize() {
      int linesPerSeries = buckets.length + 3;
      return estimatedHeaderSize()
          + Math.max(values.size(), 1) * linesPerSeries * (estimatedSampleLineSize() + 16);
    }

    @Override
    void renderInto(StringBuilder builder) {
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
          builder.append(name()).append("_bucket");
          appendLabels(builder, labelNames(), entry.getKey(), "le", bucketLabels[index]);
          builder.append(' ').append(cumulativeCount).append('\n');
        }
        builder.append(name()).append("_bucket");
        appendLabels(builder, labelNames(), entry.getKey(), "le", POSITIVE_INFINITY_LABEL);
        builder.append(' ').append(sample.count()).append('\n');
        builder.append(name()).append("_sum");
        appendLabels(builder, labelNames(), entry.getKey());
        builder.append(' ').append(sample.sum()).append('\n');
        builder.append(name()).append("_count");
        appendLabels(builder, labelNames(), entry.getKey());
        builder.append(' ').append(sample.count()).append('\n');
      }
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
