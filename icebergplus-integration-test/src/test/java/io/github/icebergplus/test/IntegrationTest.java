package io.github.icebergplus.test;

import io.github.icebergplus.local.LocalIcebergCatalog;
import java.util.Map;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.junit.jupiter.api.Test;

class IntegrationTest {

  @Test
  void happyPath() {
    var props = Map.of(
        "metrics-reporter-impl", InMemoryMetricsReporter.class.getName()
    );
    LocalIcebergCatalog catalog = new LocalIcebergCatalog(props);
    catalog.start();
    catalog.stop();
  }
}
