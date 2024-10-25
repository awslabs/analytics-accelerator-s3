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
package software.amazon.s3.dataaccelerator.io.logical.parquet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.dataaccelerator.common.telemetry.Operation;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.dataaccelerator.request.Range;
import software.amazon.s3.dataaccelerator.util.PrefetchMode;
import software.amazon.s3.dataaccelerator.util.S3URI;
import software.amazon.s3.dataaccelerator.util.StreamAttributes;

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
   * @return name of column added as recent column
   */
  public Optional<String> addToRecentColumnList(long position, int len) {
    if (parquetColumnPrefetchStore.getColumnMappers(s3Uri) != null) {
      ColumnMappers columnMappers = parquetColumnPrefetchStore.getColumnMappers(s3Uri);
      if (columnMappers.getOffsetIndexToColumnMap().containsKey(position)) {
        ColumnMetadata columnMetadata = columnMappers.getOffsetIndexToColumnMap().get(position);
        parquetColumnPrefetchStore.addRecentColumn(columnMetadata);

        LOG.info("READING COLUMN {}", columnMetadata.getColumnName());

        // Maybe prefetch all recent columns for the current row group, if they have not been
        // prefetched already.
        prefetchCurrentRowGroup(columnMappers, columnMetadata);

        if (len / (double) columnMetadata.getCompressedSize() > 0.3) {
          return Optional.of(columnMetadata.getColumnName());
        }

        return Optional.empty();
      }
    }

    return Optional.empty();
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
  private void prefetchCurrentRowGroup(ColumnMappers columnMappers, ColumnMetadata columnMetadata) {
    // When prefetch mode is per row group, only prefetch columns from the current row group.
    if (logicalIOConfiguration.getPrefetchingMode() == PrefetchMode.ROW_GROUP
        && !parquetColumnPrefetchStore.isRowGroupPrefetched(
            s3Uri, columnMetadata.getRowGroupIndex())) {
      prefetchRecentColumns(
          columnMappers, ParquetUtils.constructRowGroupsToPrefetch(columnMetadata));
      parquetColumnPrefetchStore.storePrefetchedRowGroupIndex(
          s3Uri, columnMetadata.getRowGroupIndex());
    }
  }

  /**
   * If any recent columns exist in the current parquet file, prefetch them.
   *
   * @param columnMappers Parquet file column mappings
   * @param rowGroupsToPrefetch List of row group indexes to prefetch
   * @return ranges prefetched
   */
  public IOPlanExecution prefetchRecentColumns(
      ColumnMappers columnMappers, List<Integer> rowGroupsToPrefetch) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_COLUMNS)
                .attribute(StreamAttributes.uri(this.s3Uri))
                .build(),
        () -> {
          List<Range> prefetchRanges = new ArrayList<>();
          for (String recentColumn : getRecentColumns(columnMappers.getOffsetIndexToColumnMap())) {
            if (columnMappers.getColumnNameToColumnMap().containsKey(recentColumn)) {
              LOG.info(
                  "Column {} found in schema for {}, adding to prefetch list",
                  recentColumn,
                  this.s3Uri.getKey());

              List<ColumnMetadata> columnMetadataList =
                  columnMappers.getColumnNameToColumnMap().get(recentColumn);
              for (ColumnMetadata columnMetadata : columnMetadataList) {
                if (rowGroupsToPrefetch.contains(columnMetadata.getRowGroupIndex())) {

                  // A separate request for the dictionary
                  if (columnMetadata.getDictionaryOffset() != 0) {
                    prefetchRanges.add(
                        new Range(
                            columnMetadata.getDictionaryOffset(),
                            columnMetadata.getDictionaryOffset() + (columnMetadata.getDataPageOffset() - columnMetadata.getDictionaryOffset() - 1))
                        );

                    LOG.info("PREFETCHING DICTIONARY {} - {}", columnMetadata.getDictionaryOffset(), columnMetadata.getDictionaryOffset() + (columnMetadata.getDataPageOffset() - columnMetadata.getDictionaryOffset() - 1));
                  }

                  prefetchRanges.add(
                      new Range(
                          columnMetadata.getStartPos(),
                          columnMetadata.getStartPos() + columnMetadata.getCompressedSize() - 1));
                }
              }
            }
          }

          IOPlan ioPlan =
              (prefetchRanges.isEmpty()) ? IOPlan.EMPTY_PLAN : new IOPlan(prefetchRanges);
          try {
            return physicalIO.execute(ioPlan);
          } catch (Exception e) {
            LOG.error(
                "Error in executing predictive prefetch plan for {}. Will fallback to synchronous reading for this key.",
                this.s3Uri.getKey(),
                e);
            throw new CompletionException("Error in executing predictive prefetching", e);
          }
        });
  }

  private Set<String> getRecentColumns(HashMap<Long, ColumnMetadata> offsetIndexToColumnMap) {
    if (!offsetIndexToColumnMap.isEmpty()) {
      Map.Entry<Long, ColumnMetadata> firstColumnData =
          offsetIndexToColumnMap.entrySet().iterator().next();

      int schemaHash = firstColumnData.getValue().getSchemaHash();
      return parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema(schemaHash);
    }

    return Collections.emptySet();
  }
}
