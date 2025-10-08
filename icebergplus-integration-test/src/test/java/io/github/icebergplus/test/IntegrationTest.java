package io.github.icebergplus.test;

import io.github.icebergplus.local.LocalIcebergCatalog;
import io.github.icebergplus.micrometer.MicrometerMetricsReporter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;


class IntegrationTest {

  private static final ZoneOffset ZONE_OFFSET = ZoneOffset.ofHours(5);

  private static final List<Map<String, Object>> RECORD_DATA =
      List.of(makeData("Hello world", 22, true, OffsetDateTime.of(2005, 12, 1, 0, 0, 0, 0, ZONE_OFFSET)),
          makeData("Hello moon", 33, false, OffsetDateTime.of(2005, 12, 24, 0, 0, 0, 0, ZONE_OFFSET)));

  private static Map<String, Object> makeData(String text, int count, boolean b, OffsetDateTime time) {
    return Map.of("text", text, "count", count, "amazing", b, "event_timestamp", time);
  }

  @Test
  void happyPath() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    var reporter = new MicrometerMetricsReporter(registry);

    LocalIcebergCatalog localCatalog = new LocalIcebergCatalog();
    localCatalog.setMetricsReporter(reporter);
    localCatalog.start();

    final Namespace namespace = Namespace.of("mynamespace");
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "mytable");

    final var columns =
        List.of(required(1, "text", Types.StringType.get()), required(2, "count", Types.IntegerType.get()),
            required(3, "amazing", Types.BooleanType.get()),
            required(4, "event_timestamp", Types.TimestampType.withZone()));

    final Schema schema = new Schema(columns);

    final PartitionSpec spec = PartitionSpec.builderFor(schema).build();

    {
      Catalog catalog = localCatalog.getCatalog();
      Table t = catalog.createTable(tableIdentifier, schema);
      Table loaded = catalog.loadTable(tableIdentifier);
      assertThat(t).isNotNull();
      assertThat(loaded).isNotNull();
      assertThat(t.location()).isEqualTo(loaded.location());
      assertThat(t.schema().schemaId()).isEqualTo(loaded.schema().schemaId());
      assertThat(t.schema().sameSchema(loaded.schema())).isTrue();

      for (Map<String, Object> data : RECORD_DATA) {
        OutputFile outputFile =
            loaded.io().newOutputFile(loaded.location() + "/foobar/" + UUID.randomUUID() + ".parquet");

        DataWriter<Record> dataWriter =
            Parquet.writeData(outputFile).schema(schema).createWriterFunc(GenericParquetWriter::create).overwrite()
                .metricsConfig(MetricsConfig.forTable(loaded)).withSpec(spec).build();

        GenericRecord record = GenericRecord.create(schema);
        data.forEach(record::setField);

        dataWriter.write(record);
        dataWriter.close();

        AppendFiles appendFiles = loaded.newAppend();

        InputFile inputFile = outputFile.toInputFile();
        System.out.println("location: " + inputFile.location());

        appendFiles.appendFile(DataFiles.builder(spec).withInputFile(inputFile).withRecordCount(1).build());
        appendFiles.commit();
        loaded.refresh();
      }

      var meterNames = registry.getMeters().stream()
          .map(m -> m.getId().getName())
          .collect(Collectors.toUnmodifiableSet());

      assertThat(meterNames)
          .containsExactlyInAnyOrder("iceberg.commitReport.attempts",
              "iceberg.commitReport.addedDataFiles",
              "iceberg.commitReport.totalPositionalDeletes",
              "iceberg.commitReport.totalDataFiles",
              "iceberg.commitReport.addedRecords",
              "iceberg.commitReport.totalDeleteFiles",
              "iceberg.commitReport.totalFilesSizeInBytes",
              "iceberg.commitReport.totalRecords",
              "iceberg.commitReport.totalEqualityDeletes",
              "iceberg.commitReport.addedFilesSizeInBytes",
              "iceberg.commitReport.totalDuration");

      loaded.refresh();

      var scanTasks = loaded.newScan().planFiles();
      assertThat(scanTasks).isNotNull();

      var scanReportMeterNames = registry.getMeters().stream()
          .map(m -> m.getId().getName())
          .filter(name -> name.toLowerCase().contains("scanreport"))
          .collect(Collectors.toUnmodifiableSet());

      /* todo
      assertThat(scanReportMeterNames).containsExactly(
          "foobar");

       */

      localCatalog.stop();
    }
  }
}
