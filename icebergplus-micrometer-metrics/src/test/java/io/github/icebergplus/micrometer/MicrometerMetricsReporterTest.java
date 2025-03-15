package io.github.icebergplus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
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
        .resultDataFiles(ImmutableCounterResult.builder().unit(MetricsContext.Unit.COUNT).value(222).build())
        .resultDeleteFiles(ImmutableCounterResult.builder().unit(MetricsContext.Unit.COUNT).value(333).build())
        .totalFileSizeInBytes(ImmutableCounterResult.builder().unit(MetricsContext.Unit.BYTES).value(5678).build())
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
        .containsExactlyInAnyOrder("iceberg.scanReport.totalFileSizeInBytes",
            "iceberg.scanReport.resultDataFiles",
            "iceberg.scanReport.resultDeleteFiles");
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
        .addedRecords(ImmutableCounterResult.builder()
            .unit(MetricsContext.Unit.COUNT)
            .value(30)
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
            "iceberg.commitReport.addedDataFiles",
            "iceberg.commitReport.addedDeleteFiles",
            "iceberg.commitReport.addedRecords"
            );

    var counterMap = Map.of(
        "iceberg.commitReport.addedDataFiles", 10,
        "iceberg.commitReport.addedDeleteFiles", 20,
        "iceberg.commitReport.addedRecords", 30
    );

    for (var entry : counterMap.entrySet()) {
      var counter = registry.get(entry.getKey()).counter();
      int actualCount = (int) counter.count();
      assertThat(actualCount).isEqualTo(entry.getValue());
      assertThat(counter.getId().getTags()).containsExactly(
          Tag.of("tableName", "tableFoobar")
      );
    }
  }

  static private Stream<String> meterNames(MeterRegistry registry) {
    return registry.getMeters().stream()
        .map(m -> m.getId().getName());
  }
}
