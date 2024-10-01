package com.amazon.connector.s3.io.physical;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.common.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link PhysicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class PhysicalIOConfiguration {
  private static final int DEFAULT_CAPACITY_BLOB_STORE = 50;
  private static final int DEFAULT_CAPACITY_METADATA_STORE = 50;
  private static final boolean DEFAULT_USE_SINGLE_CACHE = true;
  private static final long DEFAULT_BLOCK_SIZE_BYTES = 8 * ONE_MB;
  private static final long DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;
  private static final long DEFAULT_MAX_RANGE_SIZE = 8 * ONE_MB;
  private static final long DEFAULT_PART_SIZE = 8 * ONE_MB;
  public static final int DEFAULT_CACHE_EVICTION_TIME_MILLIS = 15 * 1000;

  /** Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_CAPACITY_BLOB_STORE} by default. */
  @Builder.Default private int blobStoreCapacity = DEFAULT_CAPACITY_BLOB_STORE;

  private static final String BLOB_STORE_CAPACITY_KEY = "blobstore.capacity";

  /**
   * Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_CAPACITY_METADATA_STORE} by default.
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
   * Maximum range size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_MAX_RANGE_SIZE} by
   * default.
   */
  @Builder.Default private long maxRangeSizeBytes = DEFAULT_MAX_RANGE_SIZE;

  private static final String MAX_RANGE_SIZE_BYTES_KEY = "maxrangesizebytes";

  /** Part size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_PART_SIZE} by default. */
  @Builder.Default private long partSizeBytes = DEFAULT_PART_SIZE;

  /**
   * Cache eviction time in milliseconds. {@link
   * PhysicalIOConfiguration#DEFAULT_CACHE_EVICTION_TIME_MILLIS} by default.
   */
  @Builder.Default private int cacheEvictionTimeMillis = DEFAULT_CACHE_EVICTION_TIME_MILLIS;

  private static final String CACHE_EVICTION_TIME_KEY = "cache.eviction.time";

  private static final String PART_SIZE_BYTES_KEY = "partsizebytes";

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
        .blobStoreCapacity(
            configuration.getInt(BLOB_STORE_CAPACITY_KEY, DEFAULT_CAPACITY_BLOB_STORE))
        .metadataStoreCapacity(
            configuration.getInt(METADATA_STORE_CAPACITY_KEY, DEFAULT_CAPACITY_METADATA_STORE))
        .blockSizeBytes(configuration.getLong(BLOCK_SIZE_BYTES_KEY, DEFAULT_BLOCK_SIZE_BYTES))
        .readAheadBytes(configuration.getLong(READ_AHEAD_BYTES_KEY, DEFAULT_READ_AHEAD_BYTES))
        .maxRangeSizeBytes(configuration.getLong(MAX_RANGE_SIZE_BYTES_KEY, DEFAULT_MAX_RANGE_SIZE))
        .partSizeBytes(configuration.getLong(PART_SIZE_BYTES_KEY, DEFAULT_PART_SIZE))
        .cacheEvictionTimeMillis(
            configuration.getInt(CACHE_EVICTION_TIME_KEY, DEFAULT_CACHE_EVICTION_TIME_MILLIS))
        .build();
  }

  /**
   * Constructs {@link PhysicalIOConfiguration}.
   *
   * @param blobStoreCapacity The capacity of the BlobStore
   * @param metadataStoreCapacity The capacity of the MetadataStore
   * @param blockSizeBytes Block size, in bytes
   * @param readAheadBytes Read ahead, in bytes
   * @param maxRangeSizeBytes Maximum physical read issued against the object store
   * @param partSizeBytes What part size to use when splitting up logical reads
   * @param cacheEvictionTimeMillis Cache eviction time in milliseconds
   */
  @Builder
  private PhysicalIOConfiguration(
      int blobStoreCapacity,
      int metadataStoreCapacity,
      long blockSizeBytes,
      long readAheadBytes,
      long maxRangeSizeBytes,
      long partSizeBytes,
      int cacheEvictionTimeMillis) {
    Preconditions.checkArgument(blobStoreCapacity > 0, "`blobStoreCapacity` must be positive");
    Preconditions.checkArgument(
        metadataStoreCapacity > 0, "`metadataStoreCapacity` must be positive");
    Preconditions.checkArgument(blockSizeBytes > 0, "`blockSizeBytes` must be positive");
    Preconditions.checkArgument(readAheadBytes > 0, "`readAheadLengthBytes` must be positive");
    Preconditions.checkArgument(maxRangeSizeBytes > 0, "`maxRangeSize` must be positive");
    Preconditions.checkArgument(partSizeBytes > 0, "`partSize` must be positive");
    Preconditions.checkArgument(
        cacheEvictionTimeMillis > 0, "`cacheEvictionTime` must be positive");

    this.blobStoreCapacity = blobStoreCapacity;
    this.metadataStoreCapacity = metadataStoreCapacity;
    this.blockSizeBytes = blockSizeBytes;
    this.readAheadBytes = readAheadBytes;
    this.maxRangeSizeBytes = maxRangeSizeBytes;
    this.partSizeBytes = partSizeBytes;
    this.cacheEvictionTimeMillis = cacheEvictionTimeMillis;
  }
}
