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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static java.time.Instant.now;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** A BlobStore is a container for Blobs and functions as a data cache. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class BlobStore implements Closeable {
  private final Map<ObjectKey, Blob> blobMap;
  protected final Cache<BlockKey, Integer> indexCache;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration configuration;
  private static final Logger LOG = LoggerFactory.getLogger(BlobStore.class);
  private AtomicLong memoryUsageAcrossBlobMap;

  private final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;
  private static final long CLEANUP_INTERVAL = 10_000; // 10 seconds in milliseconds
  private final ScheduledExecutorService maintenanceExecutor;

  private String now() {
    return FORMATTER.format(Instant.now());
  }

  /**
   * Construct an instance of BlobStore.
   *
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   */
  public BlobStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration) {
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.blobMap = Collections.synchronizedMap(new LinkedHashMap<ObjectKey, Blob>());
    this.indexCache =
        Caffeine.newBuilder()
            .expireAfterAccess(configuration.getBlobStoreTimeoutInMillis(), TimeUnit.MILLISECONDS)
            .weigher((BlockKey blockId, Integer blockSize) -> blockSize)
            .maximumWeight(configuration.getBlobStoreCapacity())
            .build();
    this.maintenanceExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setPriority(Thread.MIN_PRIORITY);
              return t;
            });
    this.configuration = configuration;
    this.memoryUsageAcrossBlobMap = new AtomicLong(0);

    LOG.info(
        "blobstore capacity: {} blobstore timeout: {}",
        configuration.getBlobStoreCapacity(),
        configuration.getBlobStoreTimeoutInMillis());
  }

  /** hh */
  public void schedulePeriodicCleanup() {
    maintenanceExecutor.scheduleAtFixedRate(
        this::scheduleCleanupIfNotRunning,
        CLEANUP_INTERVAL,
        CLEANUP_INTERVAL,
        TimeUnit.MILLISECONDS);
  }

  private void scheduleCleanupIfNotRunning() {
    if (cleanupInProgress.compareAndSet(false, true)) {
      try {
        asyncCleanup();
      } catch (Exception ex) {
        LOG.info("Error during cleanup", ex);
      } finally {
        cleanupInProgress.set(false);
      }
    } else {
      LOG.info("Skipping cleanup as previous task is still running");
    }
  }

  private void asyncCleanup() {
    String startAttempt = now();
    long startTimeMillis = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    LOG.info("[{}] [Thread-{}] Starting cleanup operation", startAttempt, threadId);
    blobMap.forEach((k, v) -> v.asyncCleanup());
    long endTimeMillis = System.currentTimeMillis();
    long durationMillis = endTimeMillis - startTimeMillis;
    double durationSeconds = durationMillis / 1000.0;
    LOG.info(
        "[{}] Cleanup operation completed Processed in {} seconds",
        startAttempt,
        String.format("%.3f", durationSeconds));
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object we are computing
   * @param streamContext contains audit headers to be attached in the request header
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(ObjectKey objectKey, ObjectMetadata metadata, StreamContext streamContext) {
    LOG.info("rajdchak clean OnRemoval Blobmap size is {}", blobMap.size());
    LOG.info("Current weight of blobMap in bytes is {}", memoryUsageAcrossBlobMap);
    return blobMap.computeIfAbsent(
        objectKey,
        uri ->
            new Blob(
                uri,
                metadata,
                new BlockManager(
                    uri, objectClient, metadata, telemetry, configuration, streamContext),
                telemetry,
                indexCache,
                memoryUsageAcrossBlobMap));
  }

  /**
   * Evicts the specified key from the cache
   *
   * @param objectKey the etag and S3 URI of the object
   */
  public void evictKey(ObjectKey objectKey) {
    this.blobMap.remove(objectKey);
  }

  /**
   * Returns the number of objects currently cached in the blobstore.
   *
   * @return an int containing the total amount of cached blobs
   */
  public int blobCount() {
    return this.blobMap.size();
  }

  /** Closes the {@link BlobStore} and frees up all resources it holds. */
  @Override
  public void close() {
    blobMap.forEach((k, v) -> v.close());
  }
}
