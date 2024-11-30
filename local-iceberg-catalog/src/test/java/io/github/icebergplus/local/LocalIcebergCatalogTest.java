package io.github.icebergplus.local;

import java.io.File;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
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
import software.amazon.awssdk.services.s3.S3Client;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

class LocalIcebergCatalogTest {

  @Test
  void happyPath() throws Exception {

    LocalIcebergCatalog localCatalog = new LocalIcebergCatalog();
    assertThat(localCatalog.getLocalDirectory()).exists().isDirectory();

    localCatalog.start();

    final Namespace namespace = Namespace.of("mynamespace");
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "mytable");

    final var columns = List.of(
        required(1, "text", Types.StringType.get()),
        required(2, "count", Types.IntegerType.get()),
        required(3, "amazing", Types.BooleanType.get()),
        required(4, "event_timestamp", Types.TimestampType.withZone())
        );

    final Schema schema = new Schema(columns);

    final PartitionSpec spec = PartitionSpec.builderFor(schema).build();

    {
      Catalog catalog = localCatalog.getCatalog();
      Table t = catalog.createTable(tableIdentifier, schema);
      Table loaded = catalog.loadTable(tableIdentifier);
      assertThat(t).isNotNull();
      assertThat(loaded).isNotNull();
      assertThat(t.location())
          .isEqualTo(loaded.location());
      assertThat(t.schema().schemaId())
          .isEqualTo(loaded.schema().schemaId());
      assertThat(t.schema().sameSchema(loaded.schema()))
          .isTrue();

      OutputFile outputFile = loaded.io().newOutputFile(loaded.location() + "/foobar/" + UUID.randomUUID() + ".parquet");

      DataWriter<Record> dataWriter =
          Parquet.writeData(outputFile)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .metricsConfig(MetricsConfig.forTable(loaded))
              .withSpec(spec)
              .build();

      GenericRecord record = GenericRecord.create(schema);
      record.setField("text", "Hello world");
      record.setField("count", 555);
      record.setField("amazing", Boolean.TRUE);
      record.setField("event_timestamp", OffsetDateTime.of(2005, 12, 1, 0, 0, 0, 0, ZoneOffset.ofHours(5)));

      dataWriter.write(record);
      dataWriter.close();

      AppendFiles appendFiles = loaded.newAppend();

      InputFile inputFile = outputFile.toInputFile();
      System.out.println("location: " + inputFile.location());

      appendFiles.appendFile(DataFiles.builder(spec)
          .withInputFile(inputFile)
          .withRecordCount(1)
          .build());
      appendFiles.commit();
    }

    localCatalog.stop();
    assertThat(localCatalog.isStopped()).isTrue();

    for (int i = 0; i < 2; i++) {
      localCatalog.start();
      assertThat(localCatalog.isStopped()).isFalse();

      File localDir = localCatalog.getLocalDirectory();
      assertThat(localDir).exists().isDirectory();

      {
        Catalog catalog = localCatalog.getCatalog();
        Table loaded = catalog.loadTable(tableIdentifier);
        assertThat(loaded).isNotNull();
        assertThat(loaded.io()).isInstanceOf(S3FileIO.class);
        assertThat(loaded.location()).startsWith("s3://test-bucket/iceberg/mynamespace/mytable");
      }

      localCatalog.stop();

      localCatalog = new LocalIcebergCatalog(localDir);
      localCatalog.start();
      {
        Catalog catalog = localCatalog.getCatalog();
        Table loaded = catalog.loadTable(tableIdentifier);
        assertThat(loaded).isNotNull();
        assertThat(loaded.io()).isInstanceOf(S3FileIO.class);
        assertThat(loaded.location()).startsWith("s3://test-bucket/iceberg/mynamespace/mytable");
        assertThat(loaded.schema().sameSchema(schema))
            .isTrue();
        try (S3Client s3 = localCatalog.createS3Client()) {
          assertBucketExists(s3, localCatalog.getS3BucketName());
        }
      }
      localCatalog.stop();
    }
  }

  private static void assertBucketExists(S3Client s3Client, String bucketName) {
    var response = s3Client.headBucket(req -> req.bucket(bucketName));
    assertThat(response.sdkHttpResponse().isSuccessful()).isTrue();
  }
}
