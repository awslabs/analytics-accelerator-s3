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
package software.amazon.s3.analyticsaccelerator.io.physical;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_GB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialReadProgression;

/** Configuration for {@link PhysicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class PhysicalIOConfiguration {
  private static final long DEFAULT_MEMORY_CAPACITY_BYTES = 2 * ONE_GB;
  private static final long DEFAULT_CACHE_DATA_TIMEOUT_MILLISECONDS = 1000;
  private static final long DEFAULT_METADATA_STORE_TTL_MILLISECONDS = 86_400_000; // 24 hours
  private static final int DEFAULT_CAPACITY_METADATA_STORE = 5000;
  private static final boolean DEFAULT_USE_SINGLE_CACHE = true;
  private static final long DEFAULT_BLOCK_SIZE_BYTES = 8 * ONE_MB;
  private static final long DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;
  private static final double DEFAULT_SEQUENTIAL_PREFETCH_BASE = 2.0;
  private static final double DEFAULT_SEQUENTIAL_PREFETCH_SPEED = 1.0;
  private static final long DEFAULT_SEQUENTIAL_PREFETCH_MAX_SIZE = 128 * ONE_MB;
  private static final long DEFAULT_BLOCK_READ_TIMEOUT = 30_000;
  private static final int DEFAULT_BLOCK_READ_RETRY_COUNT = 20;
  private static final int DEFAULT_MEMORY_CLEANUP_FREQUENCY_MILLISECONDS = 5000;
  private static final boolean DEFAULT_SMALL_OBJECTS_PREFETCHING_ENABLED = true;
  private static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 8 * ONE_MB;
  private static final int DEFAULT_THREAD_POOL_SIZE = 96;
  private static final long DEFAULT_READ_BUFFER_SIZE = 128 * ONE_KB;
  private static final long DEFAULT_TARGET_REQUEST_SIZE = 8 * ONE_MB;
  private static final double DEFAULT_REQUEST_TOLERANCE_RATIO = 1.4;
  private static final boolean DEFAULT_COALESCE_REQUEST = true;
  private static final long DEFAULT_COALESCE_REQUEST_TOLERANCE = ONE_MB;

  /**
   * Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_MEMORY_CAPACITY_BYTES} by default.
   */
  @Builder.Default private long memoryCapacityBytes = DEFAULT_MEMORY_CAPACITY_BYTES;

  private static final String MEMORY_CAPACITY_BYTES_KEY = "max.memory.limit";

  /**
   * Capacity, in blobs. {@link
   * PhysicalIOConfiguration#DEFAULT_MEMORY_CLEANUP_FREQUENCY_MILLISECONDS} by default.
   */
  @Builder.Default
  private int memoryCleanupFrequencyMilliseconds = DEFAULT_MEMORY_CLEANUP_FREQUENCY_MILLISECONDS;

  private static final String MEMORY_CLEANUP_FREQUENCY_MILLISECONDS_KEY =
      "memory.cleanup.frequency";

  /**
   * Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_CACHE_DATA_TIMEOUT_MILLISECONDS} by
   * default.
   */
  @Builder.Default
  private long cacheDataTimeoutMilliseconds = DEFAULT_CACHE_DATA_TIMEOUT_MILLISECONDS;

  private static final String CACHE_DATA_TIMEOUT_MILLISECONDS_KEY = "cache.timeout";

  /**
   * TTL for metadata cache entries, in milliseconds. {@link
   * PhysicalIOConfiguration#DEFAULT_METADATA_STORE_TTL_MILLISECONDS} by default.
   */
  @Builder.Default
  private long metadataCacheTtlMilliseconds = DEFAULT_METADATA_STORE_TTL_MILLISECONDS;

  private static final String METADATA_CACHE_TTL_MILLISECONDS_KEY = "metadatastore.ttl";

  /**
   * Maximum size for metadata cache entries. {@link
   * PhysicalIOConfiguration#DEFAULT_CAPACITY_METADATA_STORE} by default.
   */
  @Builder.Default private int metadataStoreCapacity = DEFAULT_CAPACITY_METADATA_STORE;

  private static final String METADATA_STORE_CAPACITY_KEY = "metadatastore.capacity";

  /** Block size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default. */
  @Builder.Default private long blockSizeBytes = DEFAULT_BLOCK_SIZE_BYTES;

  private static final String BLOCK_SIZE_BYTES_KEY = "blocksizebytes";

  /** Read ahead, in bytes. {@link PhysicalIOConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default. */
  @Builder.Default private long readAheadBytes = DEFAULT_READ_AHEAD_BYTES;

  private static final String READ_AHEAD_BYTES_KEY = "readaheadbytes";

  /**
   * Base constant in the sequential prefetching geometric progression. See {@link
   * SequentialReadProgression} for the exact formula. {@link
   * PhysicalIOConfiguration#DEFAULT_SEQUENTIAL_PREFETCH_BASE} by default.
   */
  @Builder.Default private double sequentialPrefetchBase = DEFAULT_SEQUENTIAL_PREFETCH_BASE;

  private static final String SEQUENTIAL_PREFETCH_BASE_KEY = "sequentialprefetch.base";

  /**
   * Constant controlling the rate of physical block _growth_ in the sequential prefetching
   * geometric progression. See {@link SequentialReadProgression} for the exact formula. {@link
   * PhysicalIOConfiguration#DEFAULT_SEQUENTIAL_PREFETCH_SPEED} by default.
   */
  @Builder.Default private double sequentialPrefetchSpeed = DEFAULT_SEQUENTIAL_PREFETCH_SPEED;

  private static final String SEQUENTIAL_PREFETCH_SPEED_KEY = "sequentialprefetch.speed";

  /** Defines the maximum size (in bytes) for sequential prefetching */
  @Builder.Default private long sequentialPrefetchMaxSize = DEFAULT_SEQUENTIAL_PREFETCH_MAX_SIZE;

  private static final String SEQUENTIAL_PREFETCH_MAX_SIZE_KEY = "sequentialprefetch.max.size";

  /** Timeout duration (in milliseconds) for reading a block object from S3 */
  @Builder.Default private long blockReadTimeout = DEFAULT_BLOCK_READ_TIMEOUT;

  private static final String BLOCK_READ_TIMEOUT_KEY = "blockreadtimeout";

  /** Number of retries for block read failure */
  @Builder.Default private int blockReadRetryCount = DEFAULT_BLOCK_READ_RETRY_COUNT;

  private static final String BLOCK_READ_RETRY_COUNT_KEY = "blockreadretrycount";

  /** Controls whether small object prefetching is enabled */
  @Builder.Default
  private boolean smallObjectsPrefetchingEnabled = DEFAULT_SMALL_OBJECTS_PREFETCHING_ENABLED;

  private static final String SMALL_OBJECTS_PREFETCHING_ENABLED_KEY =
      "small.objects.prefetching.enabled";

  /**
   * Defines the maximum size (in bytes) for an object to be considered "small" and eligible for
   * prefetching
   */
  @Builder.Default private long smallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  private static final String SMALL_OBJECT_SIZE_THRESHOLD_KEY = "small.object.size.threshold";

  private static final String THREAD_POOL_SIZE_KEY = "thread.pool.size";

  @Builder.Default private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

  private static final String READ_BUFFER_SIZE_KEY = "readbuffersize";
  @Builder.Default private long readBufferSize = DEFAULT_READ_BUFFER_SIZE;

  /**
   * Target S3 request size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_TARGET_REQUEST_SIZE}
   * by default.
   */
  @Builder.Default private long targetRequestSize = DEFAULT_TARGET_REQUEST_SIZE;

  private static final String TARGET_REQUEST_SIZE_KEY = "target.request.size";

  /**
   * Request tolerance ratio. {@link PhysicalIOConfiguration#DEFAULT_REQUEST_TOLERANCE_RATIO} by
   * default.
   */
  @Builder.Default private double requestToleranceRatio = DEFAULT_REQUEST_TOLERANCE_RATIO;

  private static final String REQUEST_TOLERANCE_RATIO_KEY = "request.tolerance.ratio";

  /** Flag to enable request Coalescing */
  @Builder.Default private boolean requestCoalesce = DEFAULT_COALESCE_REQUEST;

  private static final String REQUEST_COALESCE_KEY = "request.coalesce";

  /** Number of bytes to read and swallow when merging two requests */
  @Builder.Default private long requestCoalesceTolerance = DEFAULT_COALESCE_REQUEST_TOLERANCE;

  private static final String REQUEST_COALESCE_TOLERANCE_KEY = "request.coalesce.tolerance";

  /** Default set of settings for {@link PhysicalIO} */
  public static final PhysicalIOConfiguration DEFAULT = PhysicalIOConfiguration.builder().build();

  /**
   * Constructs {@link PhysicalIOConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return PhysicalIOConfiguration
   */
  public static PhysicalIOConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return PhysicalIOConfiguration.builder()
        .memoryCapacityBytes(
            configuration.getLong(MEMORY_CAPACITY_BYTES_KEY, DEFAULT_MEMORY_CAPACITY_BYTES))
        .memoryCleanupFrequencyMilliseconds(
            configuration.getInt(
                MEMORY_CLEANUP_FREQUENCY_MILLISECONDS_KEY,
                DEFAULT_MEMORY_CLEANUP_FREQUENCY_MILLISECONDS))
        .cacheDataTimeoutMilliseconds(
            configuration.getLong(
                CACHE_DATA_TIMEOUT_MILLISECONDS_KEY, DEFAULT_CACHE_DATA_TIMEOUT_MILLISECONDS))
        .metadataCacheTtlMilliseconds(
            configuration.getLong(
                METADATA_CACHE_TTL_MILLISECONDS_KEY, DEFAULT_METADATA_STORE_TTL_MILLISECONDS))
        .metadataStoreCapacity(
            configuration.getInt(METADATA_STORE_CAPACITY_KEY, DEFAULT_CAPACITY_METADATA_STORE))
        .blockSizeBytes(configuration.getLong(BLOCK_SIZE_BYTES_KEY, DEFAULT_BLOCK_SIZE_BYTES))
        .readAheadBytes(configuration.getLong(READ_AHEAD_BYTES_KEY, DEFAULT_READ_AHEAD_BYTES))
        .sequentialPrefetchBase(
            configuration.getDouble(SEQUENTIAL_PREFETCH_BASE_KEY, DEFAULT_SEQUENTIAL_PREFETCH_BASE))
        .sequentialPrefetchSpeed(
            configuration.getDouble(
                SEQUENTIAL_PREFETCH_SPEED_KEY, DEFAULT_SEQUENTIAL_PREFETCH_SPEED))
        .sequentialPrefetchMaxSize(
            configuration.getLong(
                SEQUENTIAL_PREFETCH_MAX_SIZE_KEY, DEFAULT_SEQUENTIAL_PREFETCH_MAX_SIZE))
        .blockReadTimeout(configuration.getLong(BLOCK_READ_TIMEOUT_KEY, DEFAULT_BLOCK_READ_TIMEOUT))
        .blockReadRetryCount(
            configuration.getInt(BLOCK_READ_RETRY_COUNT_KEY, DEFAULT_BLOCK_READ_RETRY_COUNT))
        .smallObjectsPrefetchingEnabled(
            configuration.getBoolean(
                SMALL_OBJECTS_PREFETCHING_ENABLED_KEY, DEFAULT_SMALL_OBJECTS_PREFETCHING_ENABLED))
        .smallObjectSizeThreshold(
            configuration.getLong(
                SMALL_OBJECT_SIZE_THRESHOLD_KEY, DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD))
        .threadPoolSize(configuration.getInt(THREAD_POOL_SIZE_KEY, DEFAULT_THREAD_POOL_SIZE))
        .readBufferSize(configuration.getLong(READ_BUFFER_SIZE_KEY, DEFAULT_READ_BUFFER_SIZE))
        .targetRequestSize(
            configuration.getLong(TARGET_REQUEST_SIZE_KEY, DEFAULT_TARGET_REQUEST_SIZE))
        .requestToleranceRatio(
            configuration.getDouble(REQUEST_TOLERANCE_RATIO_KEY, DEFAULT_REQUEST_TOLERANCE_RATIO))
        .requestCoalesce(configuration.getBoolean(REQUEST_COALESCE_KEY, DEFAULT_COALESCE_REQUEST))
        .requestCoalesceTolerance(
            configuration.getLong(
                REQUEST_COALESCE_TOLERANCE_KEY, DEFAULT_COALESCE_REQUEST_TOLERANCE))
        .build();
  }

  /**
   * Constructs {@link PhysicalIOConfiguration}.
   *
   * @param memoryCapacityBytes The capacity of the BlobStore
   * @param memoryCleanupFrequencyMilliseconds The blobstore clean up frequency
   * @param cacheDataTimeoutMilliseconds The ttl of items in blobstore
   * @param metadataCacheTtlMilliseconds The TTL for metadata cache entries in milliseconds
   * @param metadataStoreCapacity The maximum size for metadata cache entries
   * @param blockSizeBytes Block size, in bytes
   * @param readAheadBytes Read ahead, in bytes
   * @param sequentialPrefetchBase Scale factor to control the size of sequentially prefetched
   *     physical blocks. Example: A constant of 2.0 means doubling the block sizes.
   * @param sequentialPrefetchSpeed Constant controlling the rate of growth of sequentially
   *     prefetched physical blocks.
   * @param sequentialPrefetchMaxSize Maximum size in bytes for sequential prefetching
   * @param blockReadTimeout Timeout duration (in milliseconds) for reading a block object from S3
   * @param blockReadRetryCount Number of retries for block read failure
   * @param smallObjectsPrefetchingEnabled Whether small object prefetching is enabled
   * @param smallObjectSizeThreshold Maximum size in bytes for an object to be considered small
   * @param threadPoolSize Size of thread pool to be used for async operations
   * @param readBufferSize Size of the maximum buffer for read operations
   * @param targetRequestSize Target S3 request size, in bytes
   * @param requestToleranceRatio Request tolerance ratio
   * @param requestCoalesce Flag to enable request Coalescing
   * @param requestCoalesceTolerance Number of bytes to read and swallow when merging two requests
   * @throws IllegalArgumentException if any of the parameters are invalid
   */
  @Builder
  private PhysicalIOConfiguration(
      long memoryCapacityBytes,
      int memoryCleanupFrequencyMilliseconds,
      long cacheDataTimeoutMilliseconds,
      long metadataCacheTtlMilliseconds,
      int metadataStoreCapacity,
      long blockSizeBytes,
      long readAheadBytes,
      double sequentialPrefetchBase,
      double sequentialPrefetchSpeed,
      long sequentialPrefetchMaxSize,
      long blockReadTimeout,
      int blockReadRetryCount,
      boolean smallObjectsPrefetchingEnabled,
      long smallObjectSizeThreshold,
      int threadPoolSize,
      long readBufferSize,
      long targetRequestSize,
      double requestToleranceRatio,
      boolean requestCoalesce,
      long requestCoalesceTolerance) {
    Preconditions.checkArgument(memoryCapacityBytes > 0, "`memoryCapacityBytes` must be positive");
    Preconditions.checkArgument(
        memoryCleanupFrequencyMilliseconds > 0,
        "`memoryCleanupFrequencyMilliseconds` must be positive");
    Preconditions.checkArgument(
        cacheDataTimeoutMilliseconds > 0, "`cacheDataTimeoutMilliseconds` must be positive");
    Preconditions.checkArgument(
        metadataCacheTtlMilliseconds >= 0, "`metadataCacheTTLMilliseconds` must be positive");
    Preconditions.checkArgument(
        metadataStoreCapacity > 0, "`metadataStoreCapacity` must be positive");
    Preconditions.checkArgument(blockSizeBytes > 0, "`blockSizeBytes` must be positive");
    Preconditions.checkArgument(readAheadBytes > 0, "`readAheadLengthBytes` must be positive");
    Preconditions.checkArgument(
        sequentialPrefetchBase > 0, "`sequentialPrefetchBase` must be positive");
    Preconditions.checkArgument(
        sequentialPrefetchSpeed > 0, "`sequentialPrefetchSpeed` must be positive");
    Preconditions.checkArgument(
        sequentialPrefetchMaxSize > 0, "`sequentialPrefetchMaxSize` must be positive");
    Preconditions.checkArgument(blockReadTimeout > 0, "`blockReadTimeout` must be positive");
    Preconditions.checkArgument(
        blockReadRetryCount >= 0, "`blockReadRetryCount` must be non-negative");
    Preconditions.checkArgument(
        smallObjectSizeThreshold > 0, "`smallObjectSizeThreshold` must be positive");
    Preconditions.checkNotNull(threadPoolSize > 0, "`threadPoolSize` must be positive");
    Preconditions.checkArgument(readBufferSize > 0, "`readBufferSize` must be positive");
    Preconditions.checkArgument(targetRequestSize > 0, "`targetRequestSize` must be positive");
    Preconditions.checkArgument(
        requestToleranceRatio >= 1, "`requestToleranceRatio` must be greater than or equal than 1");

    this.memoryCapacityBytes = memoryCapacityBytes;
    this.memoryCleanupFrequencyMilliseconds = memoryCleanupFrequencyMilliseconds;
    this.cacheDataTimeoutMilliseconds = cacheDataTimeoutMilliseconds;
    this.metadataCacheTtlMilliseconds = metadataCacheTtlMilliseconds;
    this.metadataStoreCapacity = metadataStoreCapacity;
    this.blockSizeBytes = blockSizeBytes;
    this.readAheadBytes = readAheadBytes;
    this.sequentialPrefetchBase = sequentialPrefetchBase;
    this.sequentialPrefetchSpeed = sequentialPrefetchSpeed;
    this.sequentialPrefetchMaxSize = sequentialPrefetchMaxSize;
    this.blockReadTimeout = blockReadTimeout;
    this.blockReadRetryCount = blockReadRetryCount;
    this.smallObjectsPrefetchingEnabled = smallObjectsPrefetchingEnabled;
    this.smallObjectSizeThreshold = smallObjectSizeThreshold;
    this.threadPoolSize = threadPoolSize;
    this.readBufferSize = readBufferSize;
    this.targetRequestSize = targetRequestSize;
    this.requestToleranceRatio = requestToleranceRatio;
    this.requestCoalesce = requestCoalesce;
    this.requestCoalesceTolerance = requestCoalesceTolerance;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();

    builder.append("PhysicalIO configuration:\n");
    builder.append("\tmemoryCapacityBytes: " + memoryCapacityBytes + "\n");
    builder.append(
        "\tmemoryCleanupFrequencyMilliseconds: " + memoryCleanupFrequencyMilliseconds + "\n");
    builder.append("\tcacheDataTimeoutMilliseconds: " + cacheDataTimeoutMilliseconds + "\n");
    builder.append("\tmetadataCacheTtlMilliseconds: " + metadataCacheTtlMilliseconds + "\n");
    builder.append("\tmetadataStoreCapacity: " + metadataStoreCapacity + "\n");
    builder.append("\tblockSizeBytes: " + blockSizeBytes + "\n");
    builder.append("\treadAheadBytes: " + readAheadBytes + "\n");
    builder.append("\tsequentialPrefetchBase: " + sequentialPrefetchBase + "\n");
    builder.append("\tsequentialPrefetchSpeed: " + sequentialPrefetchSpeed + "\n");
    builder.append("\tsequentialPrefetchMaxSize: " + sequentialPrefetchMaxSize + "\n");
    builder.append("\tblockReadTimeout: " + blockReadTimeout + "\n");
    builder.append("\tblockReadRetryCount: " + blockReadRetryCount + "\n");
    builder.append("\tsmallObjectsPrefetchingEnabled: " + smallObjectsPrefetchingEnabled + "\n");
    builder.append("\tsmallObjectSizeThreshold: " + smallObjectSizeThreshold + "\n");
    builder.append("\tthreadPoolSize: " + threadPoolSize + "\n");
    builder.append("\treadBufferSize: " + readBufferSize + "\n");
    builder.append("\ttargetRequestSize: " + targetRequestSize + "\n");
    builder.append("\trequestToleranceRatio: " + requestToleranceRatio + "\n");
    builder.append("\trequestCoalesce: " + requestCoalesce + "\n");
    builder.append("\trequestCoalesceTolerance: " + requestCoalesceTolerance + "\n");
    return builder.toString();
  }
}
