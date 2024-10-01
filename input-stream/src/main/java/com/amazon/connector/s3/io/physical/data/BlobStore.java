package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.util.S3URI;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.time.Duration;
import lombok.NonNull;

/** A BlobStore is a container for Blobs and functions as a data cache. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class BlobStore implements Closeable {
  private final Cache<S3URI, Blob> blobCache;
  private final MetadataStore metadataStore;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration configuration;
  private final MemoryTracker memoryTracker;

  /**
   * Construct an instance of BlobStore.
   *
   * @param metadataStore the MetadataStore storing object metadata information
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   * @param memoryTracker the memory tracker
   */
  public BlobStore(
      @NonNull MetadataStore metadataStore,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull MemoryTracker memoryTracker) {
    this.metadataStore = metadataStore;
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.memoryTracker = memoryTracker;
    this.blobCache =
        Caffeine.newBuilder()
            .maximumSize(configuration.getBlobStoreCapacity())
            .expireAfterWrite(Duration.ofMillis(configuration.getCacheEvictionTimeMillis()))
            .removalListener((S3URI s3URI, Blob blob, RemovalCause cause) -> blob.close())
            .build();
    this.configuration = configuration;
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param s3URI the S3 URI of the object
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(S3URI s3URI) {
    return blobCache
        .asMap()
        .computeIfAbsent(
            s3URI,
            uri ->
                new Blob(
                    uri,
                    metadataStore,
                    new BlockManager(
                        uri, objectClient, metadataStore, telemetry, configuration, memoryTracker),
                    telemetry));
  }

  /** Closes the {@link BlobStore} and frees up all resources it holds. */
  @Override
  public void close() {
    blobCache.asMap().forEach((k, v) -> v.close());
  }
}
