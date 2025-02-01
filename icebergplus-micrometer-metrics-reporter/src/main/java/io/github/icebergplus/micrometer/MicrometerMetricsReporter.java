package io.github.icebergplus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;


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
    if (report instanceof CommitReport commitReport) {
      report("commitReport.addedDeleteFiles", commitReport.commitMetrics().addedDeleteFiles());
      report("commitReport.addedDataFiles", commitReport.commitMetrics().addedDataFiles());
    } else if (report instanceof ScanReport scanReport) {
      report("scanReport.totalFileSizeInBytes", scanReport.scanMetrics().totalFileSizeInBytes());
    } else {
      throw new IllegalArgumentException("unknown report type");
    }
  }

  private void report(String name, CounterResult counterResult) {
    if (counterResult == null) {
      return;
    }
    var counter = meterRegistry.counter(name);
    counter.increment(counterResult.value());
  }
}
