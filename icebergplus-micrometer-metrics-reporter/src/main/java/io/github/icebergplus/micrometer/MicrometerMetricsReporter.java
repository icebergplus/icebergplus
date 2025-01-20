package io.github.icebergplus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;


public class MicrometerMetricsReporter implements MetricsReporter {
  private MeterRegistry meterRegistry;

  public MicrometerMetricsReporter() {
    // todo
  }

  public MicrometerMetricsReporter(MeterRegistry registry) {
    this.meterRegistry = registry;
  }

  @Override
  public void report(MetricsReport report) {
    // todo
  }
}
