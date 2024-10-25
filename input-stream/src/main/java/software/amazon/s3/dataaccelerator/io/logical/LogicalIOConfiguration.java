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
package software.amazon.s3.dataaccelerator.io.logical;

import static software.amazon.s3.dataaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.dataaccelerator.util.Constants.ONE_MB;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import software.amazon.s3.dataaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.dataaccelerator.util.PrefetchMode;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  private static final boolean DEFAULT_FOOTER_CACHING_ENABLED = true;
  private static final long DEFAULT_FOOTER_CACHING_SIZE = 32 * ONE_KB;
  private static final boolean DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED = true;
  private static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;
  private static final int DEFAULT_PARQUET_METADATA_STORE_SIZE = 45;
  private static final int DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE = 15;
  private static final String DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX = "^.*.(parquet|par)$";
  private static final PrefetchMode DEFAULT_PREFETCHING_MODE = PrefetchMode.ROW_GROUP;

  @Builder.Default private boolean footerCachingEnabled = DEFAULT_FOOTER_CACHING_ENABLED;

  private static final String FOOTER_CACHING_ENABLED_KEY = "footer.caching.enabled";

  @Builder.Default private long footerCachingSize = DEFAULT_FOOTER_CACHING_SIZE;

  private static final String FOOTER_CACHING_SIZE_KEY = "footer.caching.size";

  @Builder.Default
  private boolean smallObjectsPrefetchingEnabled = DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED;

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

  /**
   * Constructs {@link LogicalIOConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static LogicalIOConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return LogicalIOConfiguration.builder()
        .footerCachingEnabled(
            configuration.getBoolean(FOOTER_CACHING_ENABLED_KEY, DEFAULT_FOOTER_CACHING_ENABLED))
        .footerCachingSize(
            configuration.getLong(FOOTER_CACHING_SIZE_KEY, DEFAULT_FOOTER_CACHING_SIZE))
        .smallObjectsPrefetchingEnabled(
            configuration.getBoolean(
                SMALL_OBJECTS_PREFETCHING_ENABLED_KEY, DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED))
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
        .build();
  }
}
