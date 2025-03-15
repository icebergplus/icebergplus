package io.github.icebergplus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

public class MicrometerMetricsReporter implements MetricsReporter {
  private MeterRegistry meterRegistry;
  private String metricPrefix;

  public MicrometerMetricsReporter() {
    // no-op
  }

  public MicrometerMetricsReporter(MeterRegistry registry) {
    this("iceberg.", registry);
  }

  public MicrometerMetricsReporter(String metricPrefix, MeterRegistry registry) {
    this.meterRegistry = registry;
    this.metricPrefix = metricPrefix;
  }

  public void setMeterRegistry(MeterRegistry registry) {
    this.meterRegistry = registry;
  }

  public void setMetricPrefix(String prefix) {
    this.metricPrefix = prefix;
  }

  @Override
  public void report(final MetricsReport report) {
    if (report == null) {
      return;
    }

    if (this.meterRegistry == null) {
      return;
    }

    if (report instanceof CommitReport commitReport) {
      final String prefix = metricPrefix + "commitReport.";
      List<Tag> tags = List.of(Tag.of("tableName", commitReport.tableName()));
      var counters = extractCounters(commitReport.commitMetrics());
      for (var counter : counters.entrySet()) {
        report(prefix + counter.getKey(), counter.getValue(), tags);
      }
      var timers = extractTimers(commitReport.commitMetrics());
      for (var timer: timers.entrySet()) {
        report(prefix + timer.getKey(), timer.getValue());
      }
    } else if (report instanceof ScanReport scanReport) {
      final String prefix = metricPrefix + "scanReport.";
      var counters = extractCounters(scanReport.scanMetrics());
      List<Tag> tags = List.of(Tag.of("tableName", scanReport.tableName()));
      for (var counter : counters.entrySet()) {
        report(prefix + counter.getKey(), counter.getValue(), tags);
      }
      var timers = extractTimers(scanReport.scanMetrics());
      for (var timer: timers.entrySet()) {
        report(prefix + timer.getKey(), timer.getValue());
      }
    } else {
      throw new IllegalArgumentException("unknown report type: " + report.getClass().getName());
    }
  }

  private void report(String name, CounterResult counterResult, List<Tag> tags) {
    if (counterResult == null) {
      return;
    }
    var counter = meterRegistry.counter(name, tags);
    counter.increment(counterResult.value());
  }

  private void report(String name, TimerResult timerResult) {
    if (timerResult == null) {
      return;
    }
    var timer = meterRegistry.timer(name);
    timer.record(timerResult.count(), timerResult.timeUnit());
  }

  private static Map<String, CounterResult> extractCounters(Object obj) {
    return extractFieldValues(obj, CounterResult.class);
  }

  private static Map<String, TimerResult> extractTimers(Object obj) {
    return extractFieldValues(obj, TimerResult.class);
  }

  private static <T> Map<String, T> extractFieldValues(final Object instance, final Class<T> fieldType) {
    Map<String, T> map = new HashMap<>();
    for (Field field : instance.getClass().getDeclaredFields()) {
      if (field.getType().equals(fieldType)) {
        T value = (T) valueOf(field, instance);
        if (value != null) {
          map.put(field.getName(), value);
        }
      }
    }
    return map;
  }

  private static Object valueOf(Field f, Object obj) {
    try {
      f.setAccessible(true);
      return f.get(obj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
