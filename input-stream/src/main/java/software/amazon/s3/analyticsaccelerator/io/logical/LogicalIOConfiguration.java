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
package software.amazon.s3.analyticsaccelerator.io.logical;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_GB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  private static final boolean DEFAULT_PREFETCH_FOOTER_ENABLED = true;
  private static final boolean DEFAULT_PREFETCH_PAGE_INDEX_ENABLED = true;
  private static final long DEFAULT_PREFETCH_FILE_METADATA_SIZE = 32 * ONE_KB;
  private static final long DEFAULT_PREFETCH_LARGE_FILE_METADATA_SIZE = ONE_MB;
  private static final long DEFAULT_PREFETCH_FILE_PAGE_INDEX_SIZE = ONE_MB;
  private static final long DEFAULT_PREFETCH_LARGE_FILE_PAGE_INDEX_SIZE = 8 * ONE_MB;
  private static final long DEFAULT_LARGE_FILE_SIZE = ONE_GB;
  private static final boolean DEFAULT_PREFETCHING_SMALL_OBJECT_ENABLED = true;
  private static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;
  private static final int DEFAULT_PARQUET_METADATA_STORE_SIZE = 45;
  private static final int DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE = 15;
  private static final String DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX = "^.*.(parquet|par)$";
  private static final String DEFAULT_CSV_FORMAT_SELECTOR_REGEX = "^.*\\.(csv|CSV)$";
  private static final String DEFAULT_JSON_FORMAT_SELECTOR_REGEX = "^.*\\.(json|JSON)$";
  private static final String DEFAULT_TXT_FORMAT_SELECTOR_REGEX = "^.*\\.(txt|TXT)$";
  private static final PrefetchMode DEFAULT_PREFETCHING_MODE = PrefetchMode.ROW_GROUP;
  private static final boolean DEFAULT_COLUMN_DATA_CACHING_ENABLED = false;
  private static final String DEFAULT_CPS_ENDPOINT = "";

  private static final long DEFAULT_PARTITION_SIZE = 128 * ONE_MB;

  @Builder.Default private boolean prefetchFooterEnabled = DEFAULT_PREFETCH_FOOTER_ENABLED;

  private static final String FOOTER_PREFETCH_ENABLED_KEY = "prefetch.footer.enabled";

  @Builder.Default private boolean prefetchPageIndexEnabled = DEFAULT_PREFETCH_PAGE_INDEX_ENABLED;

  private static final String PAGE_INDEX_PREFETCH_ENABLED_KEY = "prefetch.page.index.enabled";

  @Builder.Default private long prefetchFileMetadataSize = DEFAULT_PREFETCH_FILE_METADATA_SIZE;

  private static final String PREFETCH_FILE_METADATA_SIZE_KEY = "prefetch.file.metadata.size";

  @Builder.Default
  private long prefetchLargeFileMetadataSize = DEFAULT_PREFETCH_LARGE_FILE_METADATA_SIZE;

  private static final String PREFETCH_LARGE_FILE_METADATA_SIZE_KEY =
      "prefetch.large.file.metadata.size";

  @Builder.Default private long prefetchFilePageIndexSize = DEFAULT_PREFETCH_FILE_PAGE_INDEX_SIZE;

  private static final String PREFETCH_FILE_PAGE_INDEX_SIZE_KEY = "prefetch.file.page.index.size";

  @Builder.Default
  private long prefetchLargeFilePageIndexSize = DEFAULT_PREFETCH_LARGE_FILE_PAGE_INDEX_SIZE;

  private static final String LARGE_FILE_PAGE_INDEX_PREFETCH_SIZE_KEY =
      "prefetch.large.file.page.index.size";

  @Builder.Default private long largeFileSize = DEFAULT_LARGE_FILE_SIZE;

  private static final String LARGE_FILE_SIZE = "large.file.size";

  @Builder.Default
  private boolean smallObjectsPrefetchingEnabled = DEFAULT_PREFETCHING_SMALL_OBJECT_ENABLED;

  private static final String SMALL_OBJECTS_PREFETCHING_ENABLED_KEY =
      "small.objects.prefetching.enabled";

  @Builder.Default private long smallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  private static final String SMALL_OBJECT_SIZE_THRESHOLD_KEY = "small.object.size.threshold";

  private static final String METADATA_AWARE_PREFETCHING_ENABLED_KEY =
      "metadata.aware.prefetching.enabled";

  @Builder.Default private PrefetchMode prefetchingMode = DEFAULT_PREFETCHING_MODE;

  private static final String PREFETCHING_MODE_KEY = "prefetching.mode";

  @Builder.Default private int parquetMetadataStoreSize = DEFAULT_PARQUET_METADATA_STORE_SIZE;

  private static final String PARQUET_METADATA_STORE_SIZE_KEY = "parquet.metadata.store.size";

  @Builder.Default private int maxColumnAccessCountStoreSize = DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE;

  private static final String MAX_COLUMN_ACCESS_STORE_SIZE_KEY = "max.column.access.store.size";

  @Builder.Default
  private String parquetFormatSelectorRegex = DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX;

  private static final String PARQUET_FORMAT_SELECTOR_REGEX = "parquet.format.selector.regex";

  public static final LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();

  @Builder.Default private long partitionSize = DEFAULT_PARTITION_SIZE;

  private static final String PARTITION_SIZE_KEY = "partition.size";

  @Builder.Default private String csvFormatSelectorRegex = DEFAULT_CSV_FORMAT_SELECTOR_REGEX;
  private static final String CSV_FORMAT_SELECTOR_REGEX = "csv.format.selector.regex";
  @Builder.Default private String jsonFormatSelectorRegex = DEFAULT_JSON_FORMAT_SELECTOR_REGEX;
  private static final String JSON_FORMAT_SELECTOR_REGEX = "json.format.selector.regex";
  @Builder.Default private String txtFormatSelectorRegex = DEFAULT_TXT_FORMAT_SELECTOR_REGEX;
  private static final String TXT_FORMAT_SELECTOR_REGEX = "txt.format.selector.regex";

  /** Enable column data caching with column prefetching server */
  @Builder.Default private boolean columnDataCachingEnabled = DEFAULT_COLUMN_DATA_CACHING_ENABLED;

  private static final String COLUMN_DATA_CACHING_ENABLED_KEY = "columndatacache.enabled";

  /** Column prefetching server endpoint */
  @Builder.Default private String cpsEndpoint = DEFAULT_CPS_ENDPOINT;

  private static final String CPS_ENDPOINT_KEY = "cps.endpoint";

  /**
   * Constructs {@link LogicalIOConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static LogicalIOConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return LogicalIOConfiguration.builder()
        .prefetchFooterEnabled(
            configuration.getBoolean(FOOTER_PREFETCH_ENABLED_KEY, DEFAULT_PREFETCH_FOOTER_ENABLED))
        .prefetchPageIndexEnabled(
            configuration.getBoolean(
                PAGE_INDEX_PREFETCH_ENABLED_KEY, DEFAULT_PREFETCH_PAGE_INDEX_ENABLED))
        .prefetchFileMetadataSize(
            configuration.getLong(
                PREFETCH_FILE_METADATA_SIZE_KEY, DEFAULT_PREFETCH_FILE_METADATA_SIZE))
        .prefetchLargeFileMetadataSize(
            configuration.getLong(
                PREFETCH_LARGE_FILE_METADATA_SIZE_KEY, DEFAULT_PREFETCH_LARGE_FILE_METADATA_SIZE))
        .prefetchFilePageIndexSize(
            configuration.getLong(
                PREFETCH_FILE_PAGE_INDEX_SIZE_KEY, DEFAULT_PREFETCH_FILE_PAGE_INDEX_SIZE))
        .prefetchLargeFilePageIndexSize(
            configuration.getLong(
                LARGE_FILE_PAGE_INDEX_PREFETCH_SIZE_KEY,
                DEFAULT_PREFETCH_LARGE_FILE_PAGE_INDEX_SIZE))
        .largeFileSize(configuration.getLong(LARGE_FILE_SIZE, DEFAULT_LARGE_FILE_SIZE))
        .smallObjectsPrefetchingEnabled(
            configuration.getBoolean(
                SMALL_OBJECTS_PREFETCHING_ENABLED_KEY, DEFAULT_PREFETCHING_SMALL_OBJECT_ENABLED))
        .smallObjectSizeThreshold(
            configuration.getLong(
                SMALL_OBJECT_SIZE_THRESHOLD_KEY, DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD))
        .parquetMetadataStoreSize(
            configuration.getInt(
                PARQUET_METADATA_STORE_SIZE_KEY, DEFAULT_PARQUET_METADATA_STORE_SIZE))
        .maxColumnAccessCountStoreSize(
            configuration.getInt(
                MAX_COLUMN_ACCESS_STORE_SIZE_KEY, DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE))
        .parquetFormatSelectorRegex(
            configuration.getString(
                PARQUET_FORMAT_SELECTOR_REGEX, DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX))
        .prefetchingMode(
            PrefetchMode.fromString(
                configuration.getString(PREFETCHING_MODE_KEY, DEFAULT_PREFETCHING_MODE.toString())))
        .partitionSize(configuration.getPositiveLong(PARTITION_SIZE_KEY, DEFAULT_PARTITION_SIZE))
        .csvFormatSelectorRegex(
            configuration.getString(CSV_FORMAT_SELECTOR_REGEX, DEFAULT_CSV_FORMAT_SELECTOR_REGEX))
        .jsonFormatSelectorRegex(
            configuration.getString(JSON_FORMAT_SELECTOR_REGEX, DEFAULT_JSON_FORMAT_SELECTOR_REGEX))
        .txtFormatSelectorRegex(
            configuration.getString(TXT_FORMAT_SELECTOR_REGEX, DEFAULT_TXT_FORMAT_SELECTOR_REGEX))
        .columnDataCachingEnabled(
            configuration.getBoolean(
                COLUMN_DATA_CACHING_ENABLED_KEY, DEFAULT_COLUMN_DATA_CACHING_ENABLED))
        .cpsEndpoint(configuration.getString(CPS_ENDPOINT_KEY, DEFAULT_CPS_ENDPOINT))
        .build();
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();

    builder.append("LogicalIO configuration:\n");
    builder.append("\tprefetchFooterEnabled: " + prefetchFooterEnabled + "\n");
    builder.append("\tprefetchPageIndexEnabled: " + prefetchPageIndexEnabled + "\n");
    builder.append("\tprefetchFileMetadataSize: " + prefetchFileMetadataSize + "\n");
    builder.append("\tprefetchLargeFileMetadataSize: " + prefetchLargeFileMetadataSize + "\n");
    builder.append("\tprefetchFilePageIndexSize: " + prefetchFilePageIndexSize + "\n");
    builder.append("\tprefetchLargeFilePageIndexSize: " + prefetchLargeFilePageIndexSize + "\n");
    builder.append("\tlargeFileSize: " + largeFileSize + "\n");
    builder.append("\tsmallObjectsPrefetchingEnabled: " + smallObjectsPrefetchingEnabled + "\n");
    builder.append("\tsmallObjectSizeThreshold: " + smallObjectSizeThreshold + "\n");
    builder.append("\tparquetMetadataStoreSize: " + parquetMetadataStoreSize + "\n");
    builder.append("\tmaxColumnAccessCountStoreSize: " + maxColumnAccessCountStoreSize + "\n");
    builder.append("\tparquetFormatSelectorRegex: " + parquetFormatSelectorRegex + "\n");
    builder.append("\tcsvFormatSelectorRegex: " + csvFormatSelectorRegex + "\n");
    builder.append("\tjsonFormatSelectorRegex: " + jsonFormatSelectorRegex + "\n");
    builder.append("\ttxtFormatSelectorRegex: " + txtFormatSelectorRegex + "\n");
    builder.append("\tprefetchingMode: " + prefetchingMode + "\n");
    builder.append("\tpartitionSize: " + partitionSize + "\n");
    builder.append("\tcolumnDataCachingEnabled: " + columnDataCachingEnabled + "\n");
    if (columnDataCachingEnabled) {
      builder.append("\tcpsEndpoint: " + cpsEndpoint + "\n");
    }

    return builder.toString();
  }
}
