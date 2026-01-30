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

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Tests that files with multiple row groups are handled correctly, with each row group processed
 * independently and correct column identification per row group.
 */
public class ParquetMultiRowGroupIsolationTest extends ParquetIntegrationTestBase {

  /**
   * Tests that a Parquet file with multiple row groups has all row groups correctly parsed and
   * represented in the column mappers.
   *
   * <p>When a Parquet file contains multiple row groups, the ParquetMetadataParsingTask should
   * create ColumnMetadata entries for each column in each row group. Each ColumnMetadata contains
   * a rowGroupIndex field that identifies which row group it belongs to.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testMultiRowGroupMetadataParsing(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.MULTI_ROW_GROUP_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);

        // Get all unique column names
        Set<String> uniqueColumnNames = new HashSet<>();
        Map<Integer, Set<String>> columnsPerRowGroup = new HashMap<>();

        for (ColumnMetadata metadata : columnMappers.getOffsetIndexToColumnMap().values()) {
          uniqueColumnNames.add(metadata.getColumnName());
          columnsPerRowGroup
              .computeIfAbsent(metadata.getRowGroupIndex(), k -> new HashSet<>())
              .add(metadata.getColumnName());
        }

        // Verify multiple row groups exist
        assertTrue(
            columnsPerRowGroup.size() > 1,
            "File should have multiple row groups, found: " + columnsPerRowGroup.size());

        // Verify each row group has the same columns
        Set<String> firstRowGroupColumns = columnsPerRowGroup.get(0);
        assertNotNull(firstRowGroupColumns, "Row group 0 should exist");
        assertFalse(firstRowGroupColumns.isEmpty(), "Row group 0 should have columns");

        for (int i = 1; i < columnsPerRowGroup.size(); i++) {
          Set<String> rowGroupColumns = columnsPerRowGroup.get(i);
          assertEquals(
              firstRowGroupColumns,
              rowGroupColumns,
              "Row group " + i + " should have same columns as row group 0");
        }

        // Verify each column appears once per row group
        for (String columnName : uniqueColumnNames) {
          List<ColumnMetadata> columnInstances =
              columnMappers.getColumnNameToColumnMap().get(columnName);
          assertNotNull(columnInstances, "Column " + columnName + " should exist in map");
          
          assertEquals(
              columnsPerRowGroup.size(),
              columnInstances.size(),
              "Column "
                  + columnName
                  + " should appear once per row group ("
                  + columnsPerRowGroup.size()
                  + " times)");

          // Verify row group indexes are sequential and unique
          Set<Integer> rowGroupIndexes = new HashSet<>();
          for (ColumnMetadata metadata : columnInstances) {
            rowGroupIndexes.add(metadata.getRowGroupIndex());
          }
          assertEquals(
              columnsPerRowGroup.size(),
              rowGroupIndexes.size(),
              "Column " + columnName + " should have unique row group indexes");
        }
      }
    }
  }

  /**
   * Tests that column reads are correctly identified and tracked per row group.
   *
   * When reading from a specific position in a Parquet file, the library uses the
   * offsetIndexToColumnMap to determine which column is being read. Since each row group has
   * columns at different file offsets, the library must correctly identify both the column name
   * AND the row group index.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testColumnIdentificationAcrossRowGroups(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.MULTI_ROW_GROUP_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);

        // Find a column that exists in multiple row groups
        String testColumnName = null;
        List<ColumnMetadata> columnInstances = null;

        for (Map.Entry<String, List<ColumnMetadata>> entry :
            columnMappers.getColumnNameToColumnMap().entrySet()) {
          if (entry.getValue().size() >= 2) {
            testColumnName = entry.getKey();
            columnInstances = entry.getValue();
            break;
          }
        }

        assertNotNull(testColumnName, "Should find a column that exists in multiple row groups");
        assertNotNull(columnInstances, "Column instances should not be null");
        assertTrue(
            columnInstances.size() >= 2, "Column should exist in at least 2 row groups");

        // Get column metadata for row group 0 and row group 1
        ColumnMetadata rowGroup0Column =
            columnInstances.stream()
                .filter(m -> m.getRowGroupIndex() == 0)
                .findFirst()
                .orElse(null);
        ColumnMetadata rowGroup1Column =
            columnInstances.stream()
                .filter(m -> m.getRowGroupIndex() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull(rowGroup0Column, "Column should exist in row group 0");
        assertNotNull(rowGroup1Column, "Column should exist in row group 1");

        // Verify same column name
        assertEquals(
            testColumnName,
            rowGroup0Column.getColumnName(),
            "Row group 0 column should have correct name");
        assertEquals(
            testColumnName,
            rowGroup1Column.getColumnName(),
            "Row group 1 column should have correct name");

        // Verify different offsets
        assertNotEquals(
            rowGroup0Column.getStartPos(),
            rowGroup1Column.getStartPos(),
            "Same column in different row groups should have different offsets");

        // Verify offsetIndexToColumnMap contains both
        assertTrue(
            columnMappers.getOffsetIndexToColumnMap().containsKey(rowGroup0Column.getStartPos()),
            "Offset map should contain row group 0 column offset");
        assertTrue(
            columnMappers.getOffsetIndexToColumnMap().containsKey(rowGroup1Column.getStartPos()),
            "Offset map should contain row group 1 column offset");

        // Verify correct metadata is returned for each offset
        ColumnMetadata retrievedRG0 =
            columnMappers.getOffsetIndexToColumnMap().get(rowGroup0Column.getStartPos());
        ColumnMetadata retrievedRG1 =
            columnMappers.getOffsetIndexToColumnMap().get(rowGroup1Column.getStartPos());

        assertEquals(0, retrievedRG0.getRowGroupIndex(), "Should retrieve row group 0 metadata");
        assertEquals(1, retrievedRG1.getRowGroupIndex(), "Should retrieve row group 1 metadata");
      }
    }
  }


  /**
   * Tests that column tracking works correctly when reading the same column from different row groups.
   * The library maintains a recentlyReadColumnsPerSchema list that tracks column NAMES (not per row group).
   * This test verifies that reading the same column name from different row groups doesn't inflate
   * the unique column count, as the library tracks by column name for predictive prefetching.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testColumnTrackingAcrossRowGroups(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.MULTI_ROW_GROUP_PARQUET;

      try (S3SeekableInputStream stream =
                   reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ParquetColumnPrefetchStore store = getStore(reader);
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);

        assertFalse(columnMappers.getOffsetIndexToColumnMap().isEmpty(), "Column map should not be empty");
        int schemaHash =
                columnMappers.getOffsetIndexToColumnMap().values().iterator().next().getSchemaHash();


        // Find a column in multiple row groups
        String testColumnName = null;
        ColumnMetadata rowGroup0Column = null;
        ColumnMetadata rowGroup1Column = null;

        for (Map.Entry<String, List<ColumnMetadata>> entry :
                columnMappers.getColumnNameToColumnMap().entrySet()) {
          if (entry.getValue().size() >= 2) {
            testColumnName = entry.getKey();
            for (ColumnMetadata metadata : entry.getValue()) {
              if (metadata.getRowGroupIndex() == 0) {
                rowGroup0Column = metadata;
              } else if (metadata.getRowGroupIndex() == 1) {
                rowGroup1Column = metadata;
              }
            }
            if (rowGroup0Column != null && rowGroup1Column != null) {
              break;
            }
          }
        }

        assertNotNull(testColumnName, "Should find a column in multiple row groups");
        assertNotNull(rowGroup0Column, "Should find column in row group 0");
        assertNotNull(rowGroup1Column, "Should find column in row group 1");

        // Read from row group 0
        long rg0Offset = rowGroup0Column.getStartPos();
        long dictionarySize = rowGroup0Column.getDataPageOffset() - rowGroup0Column.getDictionaryOffset();
        int rg0ReadSize = (int) Math.min(rowGroup0Column.getCompressedSize(), calculateColumnReadSize(dictionarySize));

        stream.seek(rg0Offset);
        int bytesRead = stream.read(new byte[rg0ReadSize]);
        assertTrue(bytesRead > 0, "Should read bytes from row group 0");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        Set<String> trackedAfterRG0 = store.getUniqueRecentColumnsForSchema(schemaHash);
        assertTrue(
                trackedAfterRG0.contains(testColumnName),
                "Column should be tracked after reading from row group 0");

        // After reading from RG0, verify tracking count
        int trackingCountAfterRG0 = trackedAfterRG0.size();

        // Read from row group 1
        long rg1Offset = rowGroup1Column.getStartPos();
        dictionarySize = rowGroup1Column.getDataPageOffset() - rowGroup1Column.getDictionaryOffset();
        int rg1ReadSize = (int) Math.min(rowGroup1Column.getCompressedSize(), calculateColumnReadSize(dictionarySize));

        stream.seek(rg1Offset);
        bytesRead = stream.read(new byte[rg1ReadSize]);
        assertTrue(bytesRead > 0, "Should read bytes from row group 1");
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        Set<String> trackedAfterRG1 = store.getUniqueRecentColumnsForSchema(schemaHash);
        assertTrue(
                trackedAfterRG1.contains(testColumnName),
                "Column should still be tracked after reading from row group 1");

        // The unique set should not grow since we're reading the same column name from different row groups.
        assertEquals(trackingCountAfterRG0, trackedAfterRG1.size(),
                "Unique column count should remain same when reading same column from different row groups");
      }
    }
  }


  /**
   * Tests that row group prefetch tracking works correctly to avoid duplicate prefetches.
   *
   * <p>When prefetch mode is ROW_GROUP, the library prefetches columns only from the current row
   * group being read. To avoid prefetching the same row group multiple times, the library
   * maintains a columnRowGroupsPrefetched map that tracks which row groups have been prefetched
   * for each file.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testRowGroupPrefetchTracking(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.MULTI_ROW_GROUP_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ParquetColumnPrefetchStore store = getStore(reader);
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);
        S3URI uri = getS3URI(stream);

        // Initially, no row groups should be prefetched
        assertFalse(
            store.isColumnRowGroupPrefetched(uri, 0),
            "Row group 0 should not be prefetched initially");
        assertFalse(
            store.isColumnRowGroupPrefetched(uri, 1),
            "Row group 1 should not be prefetched initially");

        // Mark row group 0 as prefetched
        store.storeColumnPrefetchedRowGroupIndex(uri, 0);
        assertTrue(
            store.isColumnRowGroupPrefetched(uri, 0),
            "Row group 0 should be marked as prefetched");
        assertFalse(
            store.isColumnRowGroupPrefetched(uri, 1),
            "Row group 1 should still not be prefetched");

        // Mark row group 1 as prefetched
        store.storeColumnPrefetchedRowGroupIndex(uri, 1);
        assertTrue(
            store.isColumnRowGroupPrefetched(uri, 0), "Row group 0 should still be prefetched");
        assertTrue(
            store.isColumnRowGroupPrefetched(uri, 1),
            "Row group 1 should now be marked as prefetched");
      }
    }
  }
  

  /**
   * Tests that the parser correctly assigns row group indexes to each column.
   *
   * <p>The library must correctly increment rowGroupIndex when parsing the footer,
   * ensuring each column in each row group gets the correct index (0, 1, 2, ...).
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testRowGroupIndexAssignment(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration config = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, config)) {
      S3Object parquetFile = S3Object.MULTI_ROW_GROUP_PARQUET;

      try (S3SeekableInputStream stream =
                   reader.createReadStream(parquetFile, OpenStreamInformation.DEFAULT)) {
        ColumnMappers columnMappers = waitForColumnMappers(reader, stream);

        assertFalse(columnMappers.getColumnNameToColumnMap().isEmpty(), "Column name map should not be empty");
        String testColumn = columnMappers.getColumnNameToColumnMap().keySet().iterator().next();
        List<ColumnMetadata> columnInstances = columnMappers.getColumnNameToColumnMap().get(testColumn);


        assertTrue(columnInstances.size() >= 3, "Test file should have at least 3 row groups");

        // Sort by row group index to ensure correct ordering
        List<ColumnMetadata> sortedInstances = columnInstances.stream()
                .sorted(Comparator.comparingInt(ColumnMetadata::getRowGroupIndex))
                .collect(Collectors.toList());

        ColumnMetadata rg0 = sortedInstances.get(0);
        ColumnMetadata rg1 = sortedInstances.get(1);
        ColumnMetadata rg2 = sortedInstances.get(2);

        assertEquals(0, rg0.getRowGroupIndex(), "First instance should have rowGroupIndex=0");
        assertEquals(1, rg1.getRowGroupIndex(), "Second instance should have rowGroupIndex=1");
        assertEquals(2, rg2.getRowGroupIndex(), "Third instance should have rowGroupIndex=2");

        assertEquals(testColumn, rg0.getColumnName());
        assertEquals(testColumn, rg1.getColumnName());
        assertEquals(testColumn, rg2.getColumnName());

        assertNotEquals(rg0.getStartPos(), rg1.getStartPos(), "Different row groups should have different offsets");
        assertNotEquals(rg1.getStartPos(), rg2.getStartPos(), "Different row groups should have different offsets");
      }
    }
  }
}
