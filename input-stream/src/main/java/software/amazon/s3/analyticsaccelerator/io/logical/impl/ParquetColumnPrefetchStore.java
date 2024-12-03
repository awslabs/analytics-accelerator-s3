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
package software.amazon.s3.analyticsaccelerator.io.logical.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMetadata;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetMetadataParsingTask;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetPredictivePrefetchingTask;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * This class maintains a shared state required for Parquet prefetching operations that is required
 * independent of the life of individual streams. It is used to store Parquet metadata for
 * individual files, and a list of recently read columns. This is meant to be shared across multiple
 * streams as calling applications may open and close a stream to a file several times while
 * reading. For Spark, this was observed to happen as a stream to a Parquet file is first opened to
 * read the footer, and then a separate stream is opened to read the data.
 */
@SuppressFBWarnings(
    value = "SE_BAD_FIELD",
    justification = "The closure classes trigger this. We never use serialization on this class")
public class ParquetColumnPrefetchStore {

  /**
   * * This is a mapping of S3 URI's of Parquet files to their {@link ColumnMappers}. When a stream
   * for a Parquet file is read, these ColumnMappers are constructed in {@link
   * ParquetMetadataParsingTask} asynchronously. This ColumnMappers class contains two maps, and
   * offsetIndexToColumnMap and a columnNameToColumnMap. For a Parquet file, offsetIndexToColumnMap
   * maps the starting position of each column in the file to it's {@link ColumnMetadata}. For
   * example, this will be [<100, ss_a_metadata>, <600, ss_b_metadata>]. The columnNameToColumnMap
   * maps a column name to its metadata, this will be [<ss_a, ss_a_metadata>, <ss_b,
   * ss_b_metadata>].
   *
   * <p>When a read for particular position is made, offsetIndexToColumnMap is used to check if this
   * position corresponds to a column for this file. For example, if a read() is made at position
   * 100, then for the above, offsetIndexToColumnMap is used to infer that this read was for the
   * colum ss_a. This column is then added to the list of recently read columns.
   *
   * <p>columnNameToColumnMap is required when predictively prefetching columns for a newly opened
   * file in {@link ParquetPredictivePrefetchingTask}. For a list of recently read columns, for
   * example [ss_a, ss_b], to prefetch these columns for a new file, the columnNameToColumnMap is
   * used to find the metadata for a column called ss_a in the newly opened file. If such a key does
   * exist, then the information stored in it's ColumnMetadata, specifically the start position and
   * length is used to prefetch the correct range for this column.
   */
  private final Map<S3URI, ColumnMappers> columnMappersStore;

  /**
   * This is a mapping of schema and the recently read columns for it. For a Parquet file, a hash is
   * calculated by concatenating all the column names in the file metadata into a single string, and
   * then computing the hash. This helps separate all Parquet files belonging to the same table. Eg:
   * Two files belonging to store_sales table will have the same columns, and so have the same
   * schema hash.
   *
   * <p>This map is then used to store a list of recently read columns for each schema. For example,
   * if a stream recently read columns [ss_a, ss_b] for a store_sales schema, the list would contain
   * [ss_a, ss_b]. The list is limited to a size defined by maxColumnAccessCountStoreSize in {@link
   * LogicalIOConfiguration}. By default, this 15. This means that this list will contain the last
   * 15 columns that were read for this particular schema.
   *
   * <p>If a query is reading ss_a and ss_b, this list will look something like [ss_a, ss_b, ss_a,
   * ss_b]. This helps us maintain a history of the columns currently being read.
   */
  private final Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema;



  private final Map<Integer, LinkedList<String>> recentlyReadDictionariesPerSchema;

  /**
   * This is a mapping of S3 URI's of Parquet files to a list of row group indexes prefetched. This
   * is used when {@link PrefetchMode} is equal to ROW_GROUP. In this mode, prefetching only happens
   * when a read to a column is detected. This is different to the ALL mode, where prefetching will
   * happen on the first open of the file.
   *
   * <p>In ROW_GROUP mode, only columns that belong to the row group of the column currently being
   * read are prefetched. For example, for a file with 2 row groups with Parquet metadata: [ [<name:
   * ss_a, offset: 100>, <name: ss_b, offset: 500>], [<name: ss_a, offset: 800>, <name: ss_b,
   * offset: 1200>] ]
   *
   * <p>When there is a read at position to 100 or 500, this corresponds to a read to ss_a or ss_b
   * from row group 0, so in this case, any recent columns from row group 0 will be prefetched. 0
   * will then be added to this rowGroupsPrefetched map, so that if another read happens to a column
   * in this row group, prefetches for the row group are not triggerred again.
   */
  private final Map<S3URI, List<Integer>> columnRowGroupsPrefetched;

  private final Map<S3URI, List<Integer>> dictionaryRowGroupsPrefetched;

  private final LogicalIOConfiguration configuration;

  /**
   * Creates a new instance of ParquetMetadataStore.
   *
   * @param configuration object containing information about the metadata store size
   */
  public ParquetColumnPrefetchStore(LogicalIOConfiguration configuration) {
    this(
        configuration,
        new LinkedHashMap<S3URI, ColumnMappers>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<S3URI, ColumnMappers> eldest) {
            return this.size() > configuration.getParquetMetadataStoreSize();
          }
        },
        new LinkedHashMap<Integer, LinkedList<String>>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<Integer, LinkedList<String>> eldest) {
            return this.size() > configuration.getMaxColumnAccessCountStoreSize();
          }
        },
        new LinkedHashMap<Integer, LinkedList<String>>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<Integer, LinkedList<String>> eldest) {
            return this.size() > configuration.getMaxColumnAccessCountStoreSize();
          }
        },
        new LinkedHashMap<S3URI, List<Integer>>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<S3URI, List<Integer>> eldest) {
            return this.size() > configuration.getParquetMetadataStoreSize();
          }
        },
        new LinkedHashMap<S3URI, List<Integer>>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<S3URI, List<Integer>> eldest) {
            return this.size() > configuration.getParquetMetadataStoreSize();
          }
        });
  }

  /**
   * Creates a new instance of ParquetMetadataStore. This constructor is used for dependency
   * injection.
   *
   * @param configuration LogicalIO configuration
   * @param columnMappersStore Store of column mappings
   * @param recentlyReadColumnsPerSchema List of recent read columns for each schema
   * @param columnRowGroupsPrefetched Map of Parquet file URI to row groups that have been prefetched for
   *     it
   */
  ParquetColumnPrefetchStore(
      LogicalIOConfiguration configuration,
      Map<S3URI, ColumnMappers> columnMappersStore,
      Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema,
      Map<Integer, LinkedList<String>> recentlyReadDictionariesPerSchema,
      Map<S3URI, List<Integer>> columnRowGroupsPrefetched,
      Map<S3URI, List<Integer>> dictionaryRowGroupsPrefetched
      ) {
    this.configuration = configuration;
    this.columnMappersStore = columnMappersStore;
    this.recentlyReadColumnsPerSchema = recentlyReadColumnsPerSchema;
    this.columnRowGroupsPrefetched = columnRowGroupsPrefetched;
    this.recentlyReadDictionariesPerSchema = recentlyReadDictionariesPerSchema;
    this.dictionaryRowGroupsPrefetched = dictionaryRowGroupsPrefetched;
  }

  /**
   * Gets column mappers for a key.
   *
   * @param s3URI The S3URI to get column mappers for.
   * @return Column mappings
   */
  public synchronized ColumnMappers getColumnMappers(S3URI s3URI) {
    return columnMappersStore.get(s3URI);
  }

  /**
   * Stores column mappers for an object.
   *
   * @param s3URI S3URI to store mappers for
   * @param columnMappers Parquet metadata column mappings
   */
  public synchronized void putColumnMappers(S3URI s3URI, ColumnMappers columnMappers) {
    columnMappersStore.put(s3URI, columnMappers);
  }

  /**
   * Adds a column to the list of recent columns for a particular schema. This is a fixed sized
   * list, whose size is defined by maxColumnAccessCountStoreSize in {@link LogicalIOConfiguration}.
   * The default value is 15.
   *
   * <p>Reads at particular file offset correspond to a specific column being read. When a read
   * happens, {@link ColumnMappers} are used to find if this read corresponds to a column for the
   * currently open Parquet file. When a read happens, {@code
   * ParquetPredictivePrefetchingTask.addToRecentColumnList()} is used to decipher if it corresponds
   * to a column, that is, is there a column in the Parquet file with the same file_offset as the
   * current position of the stream? If yes, this column gets added to the recently read columns for
   * that particular schema. All Parquet files that have the exact same columns, and so the same
   * hash(concatenated string of columnNames), are said to belong to the same schema eg:
   * "store_sales".
   *
   * <p>This list maintains names of the last 15 columns that were read for a schema. This is used
   * to maintain a brief recent history of the columns being read by a workload and make predictions
   * when prefetching. Only columns that exist in this list are prefetched by {@link
   * ParquetPredictivePrefetchingTask}.
   *
   * <p>For example, assume that this list is of size 3, and the current query executing is Select
   * ss_a, ss_b from store_sales. This list will then contain something like [ss_a, ss_b, ss_a]. If
   * the query changes to Select ss_d, ss_e from store_sales, this list will start getting updated
   * as the reads for column ss_d and ss_e are requested.
   *
   * <p>The list will change as follows: // new read for ss_d is requested, update list. Since list
   * is already at capacity, remove the first element from it as this the column we saw least
   * recently. The list then becomes: [ss_b, ss_a, ss_d]
   *
   * <p>// A read for ss_e comes in, update list. Again, since list is at capacity, remove the least
   * recently seen column. List becomes: [ss_a, ss_d, ss_e]
   *
   * <p>After the next read, list will be [ss_d, ss_e, ss_d]
   *
   * <p>In this way, a fixed size list helps maintain the most recent columns and increases accuracy
   * when prefetching.
   *
   * @param columnMetadata column to be added
   */
  public synchronized void addRecentColumn(ColumnMetadata columnMetadata) {
    updateColumnAccessList(columnMetadata, recentlyReadColumnsPerSchema);
  }

  public synchronized void addRecentDictionary(ColumnMetadata columnMetadata) {
    updateColumnAccessList(columnMetadata, recentlyReadDictionariesPerSchema);
  }


  private void updateColumnAccessList(ColumnMetadata columnMetadata,
      Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema) {

    LinkedList<String> schemaRecentColumns =
        recentlyReadColumnsPerSchema.getOrDefault(
            columnMetadata.getSchemaHash(), new LinkedList<>());

    if (schemaRecentColumns.size() == this.configuration.getMaxColumnAccessCountStoreSize()) {
      schemaRecentColumns.removeFirst();
    }

    schemaRecentColumns.add(columnMetadata.getColumnName());

    recentlyReadColumnsPerSchema.put(columnMetadata.getSchemaHash(), schemaRecentColumns);
  }

  /**
   * Gets a list of unique column names from the recently read list. The recently read list contains
   * a history of recent columns read for a particular schema. For example, for a store_sales schema
   * this can be [ss_a, ss_b, ss_a, ss_b, ss_c, ss_c]. For prefetching, a set of unique columns to
   * prefetch from this list is required. In this case, this will [ss_a, ss_b, ss_c].
   *
   * @param schemaHash the schema for which to retrieve columns for
   * @return Unique set of recently read columns
   */
  public synchronized Set<String> getUniqueRecentColumnsForSchema(int schemaHash) {
   return getRecentAccessList(recentlyReadColumnsPerSchema, schemaHash);
  }

  public synchronized Set<String> getUniqueRecentDictionaryForSchema(int schemaHash) {
    return getRecentAccessList(recentlyReadDictionariesPerSchema, schemaHash);
  }

  private Set<String> getRecentAccessList(Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema, int schemaHash) {
    List<String> schemaRecentColumns = recentlyReadColumnsPerSchema.get(schemaHash);

    if (schemaRecentColumns != null) {
      return new HashSet<>(schemaRecentColumns);
    }

    return Collections.emptySet();
  }

  /**
   * Checks if columns for a row group have been prefetched.
   *
   * @param s3URI to check row group index for
   * @param rowGroupIndex to check
   * @return Boolean returns true if this row group has been prefetched for this key
   */
  public synchronized boolean isColumnRowGroupPrefetched(S3URI s3URI, Integer rowGroupIndex) {
   return isRowGroupPrefetched(s3URI, rowGroupIndex, columnRowGroupsPrefetched);
  }

  public synchronized boolean isDictionaryRowGroupPrefetched(S3URI s3URI, Integer rowGroupIndex) {
   return isRowGroupPrefetched(s3URI, rowGroupIndex, dictionaryRowGroupsPrefetched);
  }

  private boolean isRowGroupPrefetched(S3URI s3URI, Integer rowGroupIndex, Map<S3URI, List<Integer>> rowGroupsPrefetched) {
    List<Integer> rowGroupsPrefetchedForKey = rowGroupsPrefetched.get(s3URI);

    if (rowGroupsPrefetchedForKey == null) {
      return false;
    }

    // If columns for this row group have already been prefetched, don't prefetch again
    return rowGroupsPrefetchedForKey.contains(rowGroupIndex);
  }

  public synchronized void storeColumnPrefetchedRowGroupIndex(S3URI s3URI, Integer rowGroupIndex) {
   storePrefetchedRowGroupIndex(s3URI, rowGroupIndex, columnRowGroupsPrefetched);
  }

  public synchronized void storeDictionaryPrefetchedRowGroupIndex(S3URI s3URI, Integer rowGroupIndex) {
    storePrefetchedRowGroupIndex(s3URI, rowGroupIndex, dictionaryRowGroupsPrefetched);
  }

  /**
   * Stores row group indexes for which columns have been prefetched for a particular S3 URI. This
   * is required when prefetch mode is ROW_GROUP, where only recent columns for the current row
   * group being read are prefetched.
   *
   * @param s3URI to store prefetched row indexes for
   * @param rowGroupIndex for which recent columns have been prefetched
   */
  private void storePrefetchedRowGroupIndex(S3URI s3URI, Integer rowGroupIndex, Map<S3URI, List<Integer>> rowGroupsPrefetched) {
    List<Integer> rowGroupsPrefetchedForKey =
        rowGroupsPrefetched.getOrDefault(s3URI, new ArrayList<>());
    rowGroupsPrefetchedForKey.add(rowGroupIndex);
    rowGroupsPrefetched.put(s3URI, rowGroupsPrefetchedForKey);
  }
}
