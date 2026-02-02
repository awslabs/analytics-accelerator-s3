/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * Integration tests for Parquet Schema Tracking. Validates that files are grouped by schema and
 * column access patterns are tracked per schema.
 */
public class ParquetColumnTrackingIntegrationTest extends ParquetIntegrationTestBase {

  /**
   * Tests schema hash generation for files with same and different schemas.
   *
   * <p>Validates that files with identical column names produce matching schema hashes, while files
   * with different column names produce different hashes.
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testSchemaHashGeneration(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object file1 = S3Object.SCHEMA_MATCH_FILE1_PARQUET;
      S3Object file2 = S3Object.SCHEMA_MATCH_FILE2_PARQUET;
      S3Object file3 = S3Object.VALID_SMALL_PARQUET;

      int hash1 = getSchemaHash(reader, file1);
      int hash2 = getSchemaHash(reader, file2);
      int hash3 = getSchemaHash(reader, file3);

      Set<String> columns1 = getColumnNames(reader, file1);
      Set<String> columns2 = getColumnNames(reader, file2);
      Set<String> columns3 = getColumnNames(reader, file3);

      // Verify files with identical schemas have matching hashes
      assertEquals(columns1, columns2, "File1 and File2 should have identical column names");
      assertFalse(columns1.isEmpty(), "Files should have at least one column");
      assertEquals(hash1, hash2, "Files with identical schemas should have matching hashes");

      // Verify files with different schemas have different hashes
      assertNotEquals(columns1, columns3, "File1 and File3 should have different column names");
      assertNotEquals(hash1, hash3, "Files with different schemas should have different hashes");
    }
  }

  /**
   * Tests that column access patterns are automatically tracked per schema when columns are read.
   *
   * <p>Validates that when a column is read from a Parquet file, the library automatically detects
   * and tracks it for that schema, enabling predictive prefetching for subsequent files.
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testColumnAccessPatternsTrackedPerSchema(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object file = S3Object.SCHEMA_MATCH_FILE1_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        ParquetColumnPrefetchStore store = getStore(reader);
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);
        assertFalse(
            columnMappers.getOffsetIndexToColumnMap().isEmpty(), "Column map should not be empty");

        int schemaHash =
            columnMappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();

        // Verify no columns tracked initially
        Set<String> initialTracked = store.getUniqueRecentColumnsForSchema(schemaHash);
        assertTrue(initialTracked.isEmpty(), "No columns should be tracked initially");

        // Find first column and read from it
        Map.Entry<Long, ColumnMetadata> firstColumn =
            columnMappers.getOffsetIndexToColumnMap().entrySet().iterator().next();
        ColumnMetadata columnMetadata = firstColumn.getValue();
        String columnName = columnMetadata.getColumnName();
        long columnOffset = firstColumn.getKey();

        // Calculate dictionary size to ensure we read more than that (to trigger column tracking)
        long dictionarySize =
            columnMetadata.getDataPageOffset() - columnMetadata.getDictionaryOffset();
        int readSize = calculateColumnReadSize(dictionarySize);

        // Read from column (read more than dictionary size to trigger column tracking)
        stream.seek(columnOffset);
        int bytesRead = stream.read(new byte[readSize]);
        assertTrue(bytesRead > 0, "Should read bytes from column");

        // Async tracking
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        // Verify column was tracked
        Set<String> trackedColumns = store.getUniqueRecentColumnsForSchema(schemaHash);
        assertTrue(
            trackedColumns.contains(columnName),
            "Column '" + columnName + "' should be automatically tracked after reading");
      }
    }
  }

  /**
   * Tests that tracked columns for one schema don't affect files with different schemas. Validates
   * schema isolation.
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testSchemaIsolation(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object fileWithSchema1 = S3Object.SCHEMA_MATCH_FILE1_PARQUET;
      S3Object fileWithSchema2 = S3Object.VALID_SMALL_PARQUET;

      ParquetColumnPrefetchStore store = getStore(reader);

      // Read column from Schema 1
      SchemaColumnInfo schema1Info = readColumnFromSchema(reader, fileWithSchema1, store);
      int schema1Hash = schema1Info.schemaHash;
      String schema1Column = schema1Info.columnName;

      // Read column from Schema 2
      SchemaColumnInfo schema2Info = readColumnFromSchema(reader, fileWithSchema2, store);
      int schema2Hash = schema2Info.schemaHash;
      String schema2Column = schema2Info.columnName;

      // Verify schema isolation
      assertNotEquals(schema1Hash, schema2Hash, "Different schemas should have different hashes");

      Set<String> schema1Tracked = store.getUniqueRecentColumnsForSchema(schema1Hash);
      Set<String> schema2Tracked = store.getUniqueRecentColumnsForSchema(schema2Hash);

      // Schema 1 should have its column tracked
      assertTrue(
          schema1Tracked.contains(schema1Column),
          "Schema1 should have '" + schema1Column + "' tracked");

      // Schema 2 should have its column tracked
      assertTrue(
          schema2Tracked.contains(schema2Column),
          "Schema2 should have '" + schema2Column + "' tracked");

      // Schema 1 should NOT have Schema 2's column
      assertFalse(
          schema1Tracked.contains(schema2Column),
          "Schema1 should NOT have Schema2's column '" + schema2Column + "'");

      // Schema 2 should NOT have Schema 1's column
      assertFalse(
          schema2Tracked.contains(schema1Column),
          "Schema2 should NOT have Schema1's column '" + schema1Column + "'");
    }
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testUnalignedReadColumnDetection(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object file = S3Object.SCHEMA_MATCH_FILE1_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        ParquetColumnPrefetchStore store = getStore(reader);
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);
        int schemaHash =
            columnMappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();

        // Find a column with sufficient size
        ColumnMetadata targetColumn = null;
        long columnStart = 0;
        String columnName = null;

        for (Map.Entry<Long, ColumnMetadata> entry :
            columnMappers.getOffsetIndexToColumnMap().entrySet()) {
          if (entry.getValue().getCompressedSize() > 600000) {
            targetColumn = entry.getValue();
            columnStart = entry.getKey();
            columnName = targetColumn.getColumnName();
            break;
          }
        }

        assertNotNull(targetColumn, "Should find column with size > 600KB");

        // UNALIGNED read - read > 500KB to trigger unaligned detection
        long unalignedPosition = columnStart + 100;
        int readSize = 550000;

        stream.seek(unalignedPosition);
        int bytesRead = stream.read(new byte[readSize]);
        assertTrue(bytesRead > 0, "Should read bytes from unaligned position");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        Set<String> trackedAfter = store.getUniqueRecentColumnsForSchema(schemaHash);
        assertTrue(
            trackedAfter.contains(columnName),
            "Unaligned read should track column '" + columnName + "'");
      }
    }
  }

  private ColumnMappers getColumnMappersForFile(S3AALClientStreamReader reader, S3Object file)
      throws Exception {
    try (S3SeekableInputStream stream =
        reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
      ColumnMappers mappers = waitForColumnMappers(reader, stream);
      assertFalse(mappers.getOffsetIndexToColumnMap().isEmpty(), "Column map should not be empty");
      return mappers;
    }
  }

  private int getSchemaHash(S3AALClientStreamReader reader, S3Object file) throws Exception {
    ColumnMappers mappers = getColumnMappersForFile(reader, file);
    return mappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();
  }

  private Set<String> getColumnNames(S3AALClientStreamReader reader, S3Object file)
      throws Exception {
    ColumnMappers mappers = getColumnMappersForFile(reader, file);
    return mappers.getOffsetIndexToColumnMap().values().stream()
        .map(ColumnMetadata::getColumnName)
        .collect(Collectors.toSet());
  }

  /**
   * Helper method to read a column from a schema and return schema hash and column name.
   *
   * @param reader the stream reader
   * @param file the S3 object to read from
   * @param store the prefetch store
   * @return schema column information
   */
  private SchemaColumnInfo readColumnFromSchema(
      S3AALClientStreamReader reader, S3Object file, ParquetColumnPrefetchStore store)
      throws Exception {
    try (S3SeekableInputStream stream =
        reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
      ColumnMappers columnMappers = waitForColumnMappers(reader, stream);
      assertFalse(
          columnMappers.getOffsetIndexToColumnMap().isEmpty(), "Column map should not be empty");

      int schemaHash =
          columnMappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();

      Map.Entry<Long, ColumnMetadata> firstColumn =
          columnMappers.getOffsetIndexToColumnMap().entrySet().iterator().next();
      ColumnMetadata columnMetadata = firstColumn.getValue();
      String columnName = columnMetadata.getColumnName();

      long dictionarySize =
          columnMetadata.getDataPageOffset() - columnMetadata.getDictionaryOffset();
      int readSize = calculateColumnReadSize(dictionarySize);

      stream.seek(firstColumn.getKey());
      int bytesRead = stream.read(new byte[readSize]);
      assertTrue(bytesRead > 0, "Should read bytes from column");
      Thread.sleep(ASYNC_TRACKING_WAIT_MS);

      return new SchemaColumnInfo(schemaHash, columnName);
    }
  }

  /** Simple data class to hold schema hash and column name. */
  private static class SchemaColumnInfo {
    final int schemaHash;
    final String columnName;

    SchemaColumnInfo(int schemaHash, String columnName) {
      this.schemaHash = schemaHash;
      this.columnName = columnName;
    }
  }
}
