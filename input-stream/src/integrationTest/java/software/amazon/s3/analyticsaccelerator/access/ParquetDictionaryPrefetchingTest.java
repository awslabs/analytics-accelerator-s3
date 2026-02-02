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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * Tests that the library differentiates between dictionary page reads and column data reads, and
 * tracks them separately for selective queries.
 */
public class ParquetDictionaryPrefetchingTest extends ParquetIntegrationTestBase {

  /**
   * Tests that dictionary reads and column data reads are tracked separately.
   *
   * <p>In Parquet, for selective queries like "SELECT col WHERE col = 'value'", the dictionary page
   * is read first for predicate pushdown, then column data is read if the predicate matches. The
   * library tracks these separately to optimize prefetching for selective queries.
   *
   * <p>Classification logic: - Dictionary read: readSize <= (dataPageOffset - dictionaryOffset) -
   * Column read: readSize > (dataPageOffset - dictionaryOffset)
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testDictionaryAndColumnTrackingSeparation(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.SCHEMA_MATCH_FILE1_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ParquetColumnPrefetchStore store = getStore(reader);
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);
        int schemaHash =
            columnMappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();

        // Find a column with dictionary encoding
        ColumnMetadata columnWithDict = findColumnWithDictionary(columnMappers);
        assertNotNull(
            columnWithDict, "Test file must have at least one column with dictionary encoding");

        String columnName = columnWithDict.getColumnName();
        long dictOffset = columnWithDict.getDictionaryOffset();
        int dictSize = (int) (columnWithDict.getDataPageOffset() - dictOffset);
        long totalColumnSize = columnWithDict.getCompressedSize();

        // Read ONLY dictionary (small read <= dictionary size)
        int dictionaryReadSize = Math.min(dictSize, DICTIONARY_READ_SIZE);
        stream.seek(dictOffset);
        int dictBytesRead = stream.read(new byte[dictionaryReadSize]);
        assertTrue(dictBytesRead > 0, "Should read dictionary bytes");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        Set<String> trackedDictsAfterDictRead =
            store.getUniqueRecentDictionaryForSchema(schemaHash);
        Set<String> trackedColsAfterDictRead = store.getUniqueRecentColumnsForSchema(schemaHash);

        // Dictionary read tracked in dictionary list, not in column list
        assertTrue(
            trackedDictsAfterDictRead.contains(columnName),
            "Dictionary read should track '" + columnName + "' in dictionary list");
        assertFalse(
            trackedColsAfterDictRead.contains(columnName),
            "Dictionary read should NOT track '" + columnName + "' in column list");

        // Read FULL column (large read > dictionary size)
        int columnReadSize = (int) Math.min(dictSize + COLUMN_READ_BUFFER, totalColumnSize);
        stream.seek(dictOffset);
        int colBytesRead = stream.read(new byte[columnReadSize]);
        assertTrue(colBytesRead > 0, "Should read column bytes");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        Set<String> trackedColsAfterColRead = store.getUniqueRecentColumnsForSchema(schemaHash);
        Set<String> trackedDictsAfterColRead = store.getUniqueRecentDictionaryForSchema(schemaHash);

        // Column read tracked in column list
        assertTrue(
            trackedColsAfterColRead.contains(columnName),
            "Column read should track '" + columnName + "' in column list");

        // Both tracking lists exist independently
        assertTrue(
            trackedDictsAfterColRead.contains(columnName),
            "Dictionary tracking should persist after column read");
      }
    }
  }

  /**
   * Verifies that dictionary tracking is isolated per schema.
   *
   * <p>Ensures dictionaries read from one schema are not tracked when opening files with different
   * schemas. Each schema maintains its own independent dictionary tracking list.
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testDictionaryTrackingSchemaIsolation(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object file1 = S3Object.SCHEMA_MATCH_FILE1_PARQUET;
      S3Object file2 = S3Object.VALID_SMALL_PARQUET;

      int schema1Hash, schema2Hash;
      String schema1DictColumn;
      ParquetColumnPrefetchStore store = getStore(reader);

      // Read dictionary from Schema 1
      try (S3SeekableInputStream stream1 =
          reader.createReadStream(file1, OpenStreamInformation.DEFAULT)) {
        ColumnMappers columnMappers1 = waitForColumnMappers(reader, stream1);
        schema1Hash =
            columnMappers1.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();

        ColumnMetadata dictColumn1 = findColumnWithDictionary(columnMappers1);
        assertNotNull(dictColumn1, "Schema1 must have at least one column with dictionary");

        schema1DictColumn = dictColumn1.getColumnName();
        long dictOffset = dictColumn1.getDictionaryOffset();
        int dictSize = (int) (dictColumn1.getDataPageOffset() - dictOffset);

        // Read dictionary from Schema 1
        stream1.seek(dictOffset);
        int dictBytesRead = stream1.read(new byte[Math.min(dictSize, DICTIONARY_READ_SIZE)]);
        assertTrue(dictBytesRead > 0, "Should read dictionary bytes from Schema 1");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);
      }

      // Open Schema 2 file (different schema, no dictionary reads)
      try (S3SeekableInputStream stream2 =
          reader.createReadStream(file2, OpenStreamInformation.DEFAULT)) {
        ColumnMappers columnMappers2 = waitForColumnMappers(reader, stream2);
        schema2Hash =
            columnMappers2.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();
      }

      // Verify schema isolation
      assertNotEquals(schema1Hash, schema2Hash, "Different schemas should have different hashes");

      Set<String> schema1Dicts = store.getUniqueRecentDictionaryForSchema(schema1Hash);
      Set<String> schema2Dicts = store.getUniqueRecentDictionaryForSchema(schema2Hash);

      // Schema 1 should have its dictionary tracked
      assertTrue(
          schema1Dicts.contains(schema1DictColumn),
          "Schema1 should have dictionary '" + schema1DictColumn + "' tracked");

      // Schema 2 should NOT have Schema 1's dictionary
      assertFalse(
          schema2Dicts.contains(schema1DictColumn),
          "Schema2 should NOT have Schema1's dictionary '" + schema1DictColumn + "'");

      // Schema 2 should have empty dictionary list (no dictionaries read from it)
      assertTrue(
          schema2Dicts.isEmpty(),
          "Schema2 should have no dictionaries tracked since we didn't read any from it");
    }
  }

  /**
   * Verifies graceful error handling for corrupted dictionary data.
   *
   * <p>When dictionary prefetching encounters corrupted data, the library should gracefully handle
   * the error by skipping prefetch while still allowing successful read operations without
   * propagating exceptions to users.
   *
   * @param s3ClientKind the S3 client type to use for testing
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testMalformedDictionary(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object file = S3Object.CORRUPTED_DICTIONARY_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        // Verify first read succeeds despite malformed dictionary
        stream.seek(0);
        byte[] buffer = new byte[100];
        int bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "First read should succeed despite malformed dictionary");

        // Verify subsequent read also succeeds (system remains stable)
        stream.seek(100);
        bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "Subsequent read should succeed with malformed dictionary");
      }
    }
  }

  private ColumnMetadata findColumnWithDictionary(ColumnMappers columnMappers) {
    for (ColumnMetadata metadata : columnMappers.getOffsetIndexToColumnMap().values()) {
      if (metadata.getDictionaryOffset() != 0) {
        return metadata;
      }
    }
    return null;
  }
}
