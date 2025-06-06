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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import static software.amazon.s3.analyticsaccelerator.util.Constants.DEFAULT_MIN_ADJACENT_COLUMN_LENGTH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/**
 * Task for predictively prefetching columns of a parquet file.
 *
 * <p>When a parquet file is opened, it's metadata is parsed asynchronously in {@link
 * ParquetMetadataParsingTask}. Once the metadata has been parsed successfully, this task is
 * responsible for prefetching any recent columns that exist in the currently open file. {@link
 * ParquetColumnPrefetchStore} is responsible for track which columns are currently being read for a
 * particular schema, where two Parquet files are said to belong to the same schema if they contain
 * exactly the same columns, that is, the Hash(concatenated_string_of_column_names_in_file) is
 * equal.
 *
 * <p>As an example, assume two files A.parquet and B.parquet, both belonging to the store_sales
 * schema. A.parquet has metadata [{path_in_schema: ss_a, file_offset: 500, total_uncompressed_size:
 * 500}, {path_in_schema: ss_b, file_offset: 1000, total_uncompressed_size: 300}], and B.parquet has
 * schema [{path_in_schema: ss_a, file_offset: 600, total_uncompressed_size: 300}, {path_in_schema:
 * ss_b, file_offset: 900, total_uncompressed_size: 300}]. Since the hash of (ss_a, ss_b) is equal,
 * both these parquet files are from the same schema/data table. When A.parquet is opened, it's
 * metadata is parsed and {@link ColumnMappers} are built so we have maps of 1/ <the file offset of
 * where each column starts, ColumnMetadata of this column> and 2/ <columnName, ColumnMetadata of
 * this column>. For A.parquet, this will be [<500, ColumnMetadata of A>, <1000, ColumnMetadata of
 * B>] and [<ss_a, ColumnMetadata of A>, <ss_b, ColumnMetadata of B>].
 *
 * <p>When a read on a stream at a particular position for A.parquet happens, for example,
 * read(500), {@code addToRecentColumnList()} will check the offsetToColumnMetadata map, and if
 * there is a column at starts at this offset, it is added to the recent read columns list for this
 * schema. In this case, for a read pattern like read(500), position 500 corresponds to column ss_a
 * for schema store_sales, so ss_a is added to the recently read list, <store_sales, List<ss_a>>.
 * Then for read(1000), position 1000 corresponds to column ss_b, so ss_b is added to the recently
 * read list, <store_sales, List<ss_a, ss_b>>.
 *
 * <p>When B.parquet is opened, {@code prefetchRecentColumns()} will check this recently read list,
 * which will return <ss_a, ss_b>. We then prefetch ss_a and ss_b for B.parquet, using the file
 * offsets and total_uncompressed_size fields in the metadata to get the correct bytes. In this
 * example, for B.parquet two GET requests will be made with ranges [600-899, 900-1199] which
 * correspond to the ranges of ss_a and ss_b in B.parquet.
 */
public class ParquetPredictivePrefetchingTask {
  private final S3URI s3Uri;
  private final Telemetry telemetry;
  private final PhysicalIO physicalIO;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private static final String OPERATION_PARQUET_PREFETCH_COLUMNS = "parquet.task.prefetch.columns";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetPredictivePrefetchingTask.class);

  /**
   * Creates a new instance of {@link ParquetPredictivePrefetchingTask}
   *
   * @param s3Uri the object's S3URI
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO PhysicalIO instance
   * @param parquetColumnPrefetchStore object containing Parquet usage information
   */
  public ParquetPredictivePrefetchingTask(
      @NonNull S3URI s3Uri,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO,
      @NonNull ParquetColumnPrefetchStore parquetColumnPrefetchStore) {
    this.s3Uri = s3Uri;
    this.telemetry = telemetry;
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetColumnPrefetchStore = parquetColumnPrefetchStore;
  }

  /**
   * Checks if the current position corresponds to a column, and if yes, adds it to the recent
   * columns list.
   *
   * @param position current read position
   * @param len the length of the current read
   * @return name of column added as recent column
   */
  public List<ColumnMetadata> addToRecentColumnList(long position, int len) {
    if (parquetColumnPrefetchStore.getColumnMappers(s3Uri) != null) {
      ColumnMappers columnMappers = parquetColumnPrefetchStore.getColumnMappers(s3Uri);
      List<ColumnMetadata> addedColumns = new ArrayList<>();

      if (columnMappers.getOffsetIndexToColumnMap().containsKey(position)) {
        ColumnMetadata columnMetadata = columnMappers.getOffsetIndexToColumnMap().get(position);

        // If the column has a dictionary and the length of the read is <= the size of the
        // dictionary, then assume current read is for a dictionary only.
        if (isDictionaryRead(columnMetadata, len)) {
          parquetColumnPrefetchStore.addRecentDictionary(columnMetadata);
          prefetchDictionariesForCurrentRowGroup(columnMappers, columnMetadata);
          addedColumns.add(columnMetadata);
        } else {
          parquetColumnPrefetchStore.addRecentColumn(columnMetadata);
          // Maybe prefetch all recent columns for the current row group, if they have not been
          // prefetched already.
          prefetchColumnsForCurrentRowGroup(columnMappers, columnMetadata);

          addedColumns = addAdjacentColumnsInLength(columnMetadata, columnMappers, position, len);
          addedColumns.add(columnMetadata);
        }

        return addedColumns;
      } else if (len > DEFAULT_MIN_ADJACENT_COLUMN_LENGTH) {
        // If the read does not align to a column boundary, then check if it lies within the
        // boundary of a column, this can happen when reading adjacent columns, where reads don't
        // always start at the file_offset. The check for len >
        // DEFAULT_MIN_ADJACENT_COLUMN_LENGTH
        // is required to prevent doing this multiple times. This is especially important as when
        // reading dictionaries/columnIndexes,
        // parquet-mr issues thousands of 1 byte read(0, pos, 1), and so without this we will end up
        // in this else clause more times than intended!
        return addCurrentColumnAtPosition(position, columnMappers);
      }
    }

    return Collections.emptyList();
  }

  /**
   * When PrefetchMode is ROW_GROUP, only prefetch recent columns when a read to a column is
   * detected, and NOT on an open of the file. For prefetching, only prefetch recent columns that
   * belong to the row group of the column currently being read, if they have not been prefetched
   * already. Columns from this row group may have been prefetched already due to a read to another
   * column for this row group.
   *
   * @param columnMappers Parquet file column mappings
   * @param columnMetadata Column metadata of the current column being read
   */
  private void prefetchColumnsForCurrentRowGroup(
      ColumnMappers columnMappers, ColumnMetadata columnMetadata) {
    // When prefetch mode is per row group, only prefetch columns from the current row group.
    if (logicalIOConfiguration.getPrefetchingMode() == PrefetchMode.ROW_GROUP
        && !parquetColumnPrefetchStore.isColumnRowGroupPrefetched(
            s3Uri, columnMetadata.getRowGroupIndex())) {
      prefetchRecentColumns(
          columnMappers, ParquetUtils.constructRowGroupsToPrefetch(columnMetadata), false);
      parquetColumnPrefetchStore.storeColumnPrefetchedRowGroupIndex(
          s3Uri, columnMetadata.getRowGroupIndex());
    }
  }

  private void prefetchDictionariesForCurrentRowGroup(
      ColumnMappers columnMappers, ColumnMetadata columnMetadata) {
    if (logicalIOConfiguration.getPrefetchingMode() == PrefetchMode.ROW_GROUP
        && !parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(
            s3Uri, columnMetadata.getRowGroupIndex())) {
      prefetchRecentColumns(
          columnMappers, ParquetUtils.constructRowGroupsToPrefetch(columnMetadata), true);
      parquetColumnPrefetchStore.storeDictionaryPrefetchedRowGroupIndex(
          s3Uri, columnMetadata.getRowGroupIndex());
    }
  }

  /**
   * If any recent columns exist in the current parquet file, prefetch them.
   *
   * @param columnMappers Parquet file column mappings
   * @param rowGroupsToPrefetch List of row group indexes to prefetch
   * @param isDictionary true if the current prefetch is for dictionaries only
   * @return ranges prefetched
   */
  public IOPlanExecution prefetchRecentColumns(
      ColumnMappers columnMappers, List<Integer> rowGroupsToPrefetch, Boolean isDictionary) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_COLUMNS)
                .attribute(StreamAttributes.uri(this.s3Uri))
                .build(),
        () -> {
          try {
            // Ranges for dictionary data only
            List<Range> dictionaryRanges = new ArrayList<>();
            // Ranges for column data
            List<Range> columnRanges = new ArrayList<>();

            LOG.info("DISPLAYING PREFETCH INFO...");
            LOG.info(
                "HERE IS A LIST OF BUCKETS: {}",
                getRecentColumns(columnMappers.getOffsetIndexToColumnMap(), isDictionary)
                    .toString());
            LOG.info("HERE IS THE S3 URI: {}", this.s3Uri);
            LOG.info("_____________________________-");

            for (String recentColumn :
                getRecentColumns(columnMappers.getOffsetIndexToColumnMap(), isDictionary)) {

              if (columnMappers.getColumnNameToColumnMap().containsKey(recentColumn)) {
                List<ColumnMetadata> columnMetadataList =
                    columnMappers.getColumnNameToColumnMap().get(recentColumn);
                for (ColumnMetadata columnMetadata : columnMetadataList) {
                  if (rowGroupsToPrefetch.contains(columnMetadata.getRowGroupIndex())) {
                    // If the reader is currently reading dictionaries, only prefetch dictionary
                    // bytes for the columns. This prevents over-reading for highly selective
                    // queries, as we prefetch column data only if the predicate matches.
                    if (isDictionary && columnMetadata.getDictionaryOffset() != 0) {
                      dictionaryRanges.add(
                          new Range(
                              columnMetadata.getDictionaryOffset(),
                              columnMetadata.getDictionaryOffset()
                                  + (columnMetadata.getDataPageOffset()
                                      - columnMetadata.getDictionaryOffset()
                                      - 1)));
                      LOG.debug(
                          "Column dictionary {} found in schema for {}, and rowGroupIndex {}, adding to prefetch list",
                          recentColumn,
                          this.s3Uri.getKey(),
                          columnMetadata.getRowGroupIndex());
                    } else {
                      columnRanges.add(
                          new Range(
                              columnMetadata.getStartPos(),
                              columnMetadata.getStartPos()
                                  + columnMetadata.getCompressedSize()
                                  - 1));
                      LOG.debug(
                          "Column {} found in schema for {}, and rowGroupIndex {}, adding to prefetch list",
                          recentColumn,
                          this.s3Uri.getKey(),
                          columnMetadata.getRowGroupIndex());
                    }
                  }
                }
              }
            }

            IOPlan dictionaryIoPlan =
                (dictionaryRanges.isEmpty()) ? IOPlan.EMPTY_PLAN : new IOPlan(dictionaryRanges);
            physicalIO.execute(dictionaryIoPlan);

            IOPlan columnIoPlan =
                (columnRanges.isEmpty())
                    ? IOPlan.EMPTY_PLAN
                    : new IOPlan(ParquetUtils.mergeRanges(columnRanges));
            return physicalIO.execute(columnIoPlan);
          } catch (Throwable t) {
            LOG.debug("Unable to prefetch columns for {}.", this.s3Uri.getKey(), t);
            return IOPlanExecution.builder().state(IOPlanState.SKIPPED).build();
          }
        });
  }

  /**
   * When reading adjacent columns in a schema, reads may not fully align to the parquet schema. If
   * the schema is like:
   *
   * <p>ColumnChunk name = ss_a file_offset =0 total_compressed_size = 7MB ColumnChunk name = ss_b
   * file_offset = 7.5MB total_compressed_size = 2MB ColumnChunk name = ss_c file_offset = 9.5MB
   * total_compressed_size = 5MB
   *
   * <p>And we want to read columns ss_a, ss_b, ss_c, then the reads from the parquet reader can
   * look like:
   *
   * <p>read(0, 8MB) // Read at pos 0, with len 8MB, for ss_a and a part of ss_b read(8MB, 5MB) //
   * Read the remainder of ss_a, ss_c
   *
   * <p>Since the reads do not align to column boundaries, that is, they do not start at the file
   * offset of the column, to track columns for prefetching additional logic is required. Here, we
   * loop through the column file offsets and find the column that this read belongs to. For
   * example, for the read(8MB, 5MB) means we are reading column ss_b, since the position 8MB lies
   * within the boundary of ss_b as 8MB > file offset of ss_b > and 8MB < fil_offset of ss_c.
   *
   * @param position The current position in the read
   * @param columnMappers Parquet file column mappings
   * @return Optional<ColumnMetadata> The column added to the recently read list
   */
  private List<ColumnMetadata> addCurrentColumnAtPosition(
      long position, ColumnMappers columnMappers) {
    ArrayList<Long> columnPositions =
        new ArrayList<>(columnMappers.getOffsetIndexToColumnMap().keySet());
    Collections.sort(columnPositions);

    // For the last index, also add its end position so we can track reads that lie within
    // that column boundary
    long lastColumnStartPos = columnPositions.get(columnPositions.size() - 1);
    ColumnMetadata lastColumnMetadata =
        columnMappers.getOffsetIndexToColumnMap().get(lastColumnStartPos);
    columnPositions.add(lastColumnStartPos + lastColumnMetadata.getCompressedSize());

    for (int i = 0; i < columnPositions.size() - 1; i++) {
      if (position > columnPositions.get(i) && position < columnPositions.get(i + 1)) {
        ColumnMetadata currentColumnMetadata =
            columnMappers.getOffsetIndexToColumnMap().get(columnPositions.get(i));
        parquetColumnPrefetchStore.addRecentColumn(currentColumnMetadata);
        List<ColumnMetadata> addedColumns = new ArrayList<>();
        addedColumns.add(currentColumnMetadata);
        return addedColumns;
      }
    }

    return Collections.emptyList();
  }

  /**
   * Adds adjacent columns to recently read list. For more details on why this required, see
   * documentation for addCurrentColumnAtPosition(). Here, we track all columns that are contained
   * in the read. For example, the read(0, 8MB) contains both ss_a and ss_b, so add both to the
   * recently tracked list. logicalIOConfiguration.getMinAdjacentColumnLength() controls the minimum
   * length for which this is done, so in case the schema has many small columns, this value can be
   * adjusted to prevent many columns being added to the tracked list.
   *
   * @param columnMetadata Column metadata of the current column being read
   * @param columnMappers Parquet file column mappings
   * @param position The current position in the stream
   * @param len The length of the current read
   * @return List<ColumnMetadata> List of column metadata read
   */
  private List<ColumnMetadata> addAdjacentColumnsInLength(
      ColumnMetadata columnMetadata, ColumnMappers columnMappers, long position, int len) {
    List<ColumnMetadata> addedColumns = new ArrayList<>();

    if (len > columnMetadata.getCompressedSize() && len > DEFAULT_MIN_ADJACENT_COLUMN_LENGTH) {

      long remainingLen = len - columnMetadata.getCompressedSize();
      long currentPos = position + columnMetadata.getCompressedSize();

      while (remainingLen > 0) {
        ColumnMetadata currentColumnMetadata =
            columnMappers.getOffsetIndexToColumnMap().get(currentPos);

        if (currentColumnMetadata == null || columnMetadata.getCompressedSize() == 0) {
          break;
        }

        parquetColumnPrefetchStore.addRecentColumn(currentColumnMetadata);
        remainingLen = remainingLen - currentColumnMetadata.getCompressedSize();
        currentPos = currentPos + currentColumnMetadata.getCompressedSize();
        addedColumns.add(currentColumnMetadata);
      }
    }

    return addedColumns;
  }

  private Set<String> getRecentColumns(
      Map<Long, ColumnMetadata> offsetIndexToColumnMap, boolean isDictionary) {
    if (!offsetIndexToColumnMap.isEmpty()) {
      Map.Entry<Long, ColumnMetadata> firstColumnData =
          offsetIndexToColumnMap.entrySet().iterator().next();

      int schemaHash = firstColumnData.getValue().getSchemaHash();

      if (isDictionary) {
        return parquetColumnPrefetchStore.getUniqueRecentDictionaryForSchema(schemaHash);
      } else {
        return parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema(schemaHash);
      }
    }

    return Collections.emptySet();
  }

  private boolean isDictionaryRead(ColumnMetadata columnMetadata, int len) {
    return columnMetadata.getDictionaryOffset() != 0
        && len <= (columnMetadata.getDataPageOffset() - columnMetadata.getDictionaryOffset());
  }
}
