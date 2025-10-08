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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Task for parsing the footer bytes to get the parquet FileMetaData and build maps which can be
 * used to track current columns being read. Best effort only, exceptions in parsing should be
 * suppressed by the calling class
 */
public class ParquetMetadataParsingTask {
  private final S3URI s3URI;
  private final ParquetParser parquetParser;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;
  private final RequestCallback requestCallback;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataParsingTask.class);

  /**
   * Creates a new instance of {@link ParquetMetadataParsingTask}.
   *
   * @param s3URI the S3Uri of the object
   * @param parquetColumnPrefetchStore object containing Parquet usage information
   * @param requestCallback callback for tracking IoStats to upstream integrations such as S3A
   */
  public ParquetMetadataParsingTask(
      S3URI s3URI,
      ParquetColumnPrefetchStore parquetColumnPrefetchStore,
      RequestCallback requestCallback) {
    this(s3URI, parquetColumnPrefetchStore, new ParquetParser(), requestCallback);
  }

  /**
   * Creates a new instance of {@link ParquetMetadataParsingTask}. This version of the constructor
   * is useful for testing as it allows dependency injection.
   *
   * @param s3URI the S3Uri of the object
   * @param parquetColumnPrefetchStore object containing Parquet usage information
   * @param parquetParser parser for getting the file metadata
   * @param requestCallback callback for tracking IoStats to upstream integrations such as S3A
   */
  ParquetMetadataParsingTask(
      @NonNull S3URI s3URI,
      @NonNull ParquetColumnPrefetchStore parquetColumnPrefetchStore,
      @NonNull ParquetParser parquetParser,
      @NonNull RequestCallback requestCallback) {
    this.s3URI = s3URI;
    this.parquetParser = parquetParser;
    this.parquetColumnPrefetchStore = parquetColumnPrefetchStore;
    this.requestCallback = requestCallback;
  }

  /**
   * Stores parquet metadata column mappings for future use
   *
   * @param fileTail tail of parquet file to be parsed
   * @return Column mappings
   */
  public ColumnMappers storeColumnMappers(FileTail fileTail) {
    try {
      FileMetaData fileMetaData =
          parquetParser.parseParquetFooter(
              fileTail.getFileTail(), fileTail.getFileTailLength(), this.s3URI);
      ColumnMappers columnMappers = buildColumnMaps(fileMetaData);
      parquetColumnPrefetchStore.putColumnMappers(this.s3URI, columnMappers);
      return columnMappers;
    } catch (Exception e) {
      requestCallback.footerParsingFailed();
      LOG.debug(
          "Unable to parse parquet footer for {}, parquet prefetch optimisations will be disabled for this key.",
          this.s3URI.getKey(),
          e);
      throw new CompletionException("Error parsing parquet footer", e);
    }
  }

  private ColumnMappers buildColumnMaps(FileMetaData fileMetaData) {
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    String concatenatedColumnNames = concatColumnNames(fileMetaData);

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {

      for (ColumnChunk columnChunk : rowGroup.getColumns()) {

        // Get the full path to support nested schema
        String columnName = String.join(".", columnChunk.getMeta_data().getPath_in_schema());

        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          ColumnMetadata columnMetadata =
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getMeta_data().getData_page_offset(),
                  columnChunk.getMeta_data().getDictionary_page_offset(),
                  columnChunk.getMeta_data().getDictionary_page_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size(),
                  concatenatedColumnNames.hashCode());
          offsetIndexToColumnMap.put(
              columnChunk.getMeta_data().getDictionary_page_offset(), columnMetadata);
          List<ColumnMetadata> columnMetadataList =
              columnNameToColumnMap.computeIfAbsent(columnName, metadataList -> new ArrayList<>());
          columnMetadataList.add(columnMetadata);
        } else {
          ColumnMetadata columnMetadata =
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getMeta_data().getData_page_offset(),
                  0,
                  columnChunk.getFile_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size(),
                  concatenatedColumnNames.hashCode());
          offsetIndexToColumnMap.put(columnChunk.getFile_offset(), columnMetadata);
          List<ColumnMetadata> columnMetadataList =
              columnNameToColumnMap.computeIfAbsent(columnName, metadataList -> new ArrayList<>());
          columnMetadataList.add(columnMetadata);
        }
      }

      rowGroupIndex++;
    }

    return new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap);
  }

  private String concatColumnNames(FileMetaData fileMetaData) {
    StringBuilder concatenatedColumnNames = new StringBuilder();
    RowGroup rowGroup = fileMetaData.getRow_groups().get(0);
    // Concat all column names in a string from which schema hash can be constructed
    for (ColumnChunk columnChunk : rowGroup.getColumns()) {
      // Get the full path to support nested schema
      String columnName = String.join(".", columnChunk.getMeta_data().getPath_in_schema());
      concatenatedColumnNames.append(columnName);
    }

    return concatenatedColumnNames.toString();
  }
}
