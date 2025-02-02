package io.github.icebergplus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableCounterResult;
import org.apache.iceberg.metrics.ImmutableScanMetricsResult;
import org.apache.iceberg.metrics.ImmutableScanReport;

import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MicrometerMetricsReporterTest {

  @Test
  void scanReport_happyPath() {
    var scanResult = ImmutableScanMetricsResult.builder()
        .totalFileSizeInBytes(ImmutableCounterResult.builder().unit(MetricsContext.Unit.BYTES).value(5678).build())
        .resultDataFiles(ImmutableCounterResult.builder().unit(MetricsContext.Unit.COUNT).value(222).build())
        .build();
    var report = ImmutableScanReport.builder()
        .tableName("fooTable")
        .snapshotId(2345)
        .schemaId(666)
        .filter(Expressions.alwaysTrue())
        .scanMetrics(scanResult)
        .build();

    var registry = new SimpleMeterRegistry();

    try (var reporter = new MicrometerMetricsReporter(registry)) {
      reporter.report(report);
    }

    assertThat(meterNames(registry))
        .containsExactlyInAnyOrder("iceberg.scanReport.totalFileSizeInBytes", "iceberg.scanReport.resultDataFiles");
  }

  @Test
  void commitReport_happyPath() {
    var commitResult = ImmutableCommitMetricsResult.builder()
        .addedDataFiles(ImmutableCounterResult.builder()
            .unit(MetricsContext.Unit.COUNT)
            .value(10)
            .build())
        .addedDeleteFiles(ImmutableCounterResult.builder()
            .unit(MetricsContext.Unit.COUNT)
            .value(20)
            .build())
        .build();
    var report = ImmutableCommitReport.builder()
        .tableName("tableFoobar")
        .snapshotId(555)
        .sequenceNumber(444)
        .operation("helloOperation")
        .commitMetrics(commitResult)
        .build();
    var registry = new SimpleMeterRegistry();
    try (var reporter = new MicrometerMetricsReporter(registry)) {
      reporter.report(report);
    }

    assertThat(meterNames(registry))
        .containsExactlyInAnyOrder(
            "iceberg.commitReport.addedDeleteFiles",
            "iceberg.commitReport.addedDataFiles");

    assertThat(registry.get("iceberg.commitReport.addedDataFiles").counter().count())
        .isEqualTo(10);
    assertThat(registry.get("iceberg.commitReport.addedDeleteFiles").counter().count())
        .isEqualTo(20);
  }

  private Stream<String> meterNames(MeterRegistry registry) {
    return registry.getMeters().stream()
        .map(m -> m.getId().getName());
  }
}
