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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.MetricComputationUtils;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** A BlobStore is a container for Blobs and functions as a data cache. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class BlobStore implements Closeable {
  private final Map<ObjectKey, Blob> blobMap;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration configuration;
  private final ExecutorService threadPool;

  @Getter private final Metrics metrics;
  final BlobStoreIndexCache indexCache;
  private final ScheduledExecutorService maintenanceExecutor;
  private static final Logger LOG = LoggerFactory.getLogger(BlobStore.class);
  final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);

  /**
   * Construct an instance of BlobStore.
   *
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   * @param metrics an instance of {@link Metrics} to track metrics across the factory
   * @param threadPool a thread pool for async operations
   */
  public BlobStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull Metrics metrics,
      @NonNull ExecutorService threadPool) {
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.metrics = metrics;
    this.blobMap = Collections.synchronizedMap(new LinkedHashMap<ObjectKey, Blob>());
    this.indexCache = new BlobStoreIndexCache(configuration);
    this.maintenanceExecutor =
        Executors.newSingleThreadScheduledExecutor(
            cleanupTask -> {
              Thread cleanupThread = new Thread(cleanupTask);
              cleanupThread.setDaemon(true);
              cleanupThread.setPriority(Thread.MIN_PRIORITY);
              return cleanupThread;
            });
    this.configuration = configuration;
    this.threadPool = threadPool;
  }

  /** Schedules a periodic cleanup task to sync the blop map with the index cache */
  public void schedulePeriodicCleanup() {
    maintenanceExecutor.scheduleAtFixedRate(
        this::scheduleCleanupIfNotRunning,
        configuration.getMemoryCleanupFrequencyMilliseconds(),
        configuration.getMemoryCleanupFrequencyMilliseconds(),
        TimeUnit.MILLISECONDS);
  }

  void scheduleCleanupIfNotRunning() {
    if (metrics.get(MetricKey.MEMORY_USAGE) > 0 && cleanupInProgress.compareAndSet(false, true)) {
      try {
        asyncCleanup();
      } catch (Exception ex) {
        LOG.debug("Error during cleanup", ex);
      } finally {
        cleanupInProgress.set(false);
      }
    }
  }

  void asyncCleanup() {
    LOG.debug(
        "Current memory usage of blobMap in bytes before eviction is: {}",
        metrics.get(MetricKey.MEMORY_USAGE));
    blobMap.forEach((k, v) -> v.asyncCleanup());
    LOG.debug(
        "Current memory usage of blobMap in bytes after eviction is: {}",
        metrics.get(MetricKey.MEMORY_USAGE));
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object we are computing
   * @param openStreamInformation contains stream information
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(
      ObjectKey objectKey, ObjectMetadata metadata, OpenStreamInformation openStreamInformation) {
    return blobMap.computeIfAbsent(
        objectKey,
        uri ->
            new Blob(
                uri,
                metadata,
                new BlockManager(
                    objectKey,
                    objectClient,
                    metadata,
                    telemetry,
                    configuration,
                    metrics,
                    indexCache,
                    openStreamInformation,
                    threadPool),
                telemetry));
  }

  /**
   * Evicts the specified key from the cache
   *
   * @param objectKey the etag and S3 URI of the object
   * @return a boolean stating if the object existed or not
   */
  public boolean evictKey(ObjectKey objectKey) {
    return this.blobMap.remove(objectKey) != null;
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
    try {
      if (maintenanceExecutor != null) {
        maintenanceExecutor.shutdownNow();
      }
      blobMap.forEach((k, v) -> v.close());
      indexCache.cleanUp();
      long hits = metrics.get(MetricKey.CACHE_HIT);
      long miss = metrics.get(MetricKey.CACHE_MISS);
      LOG.debug(
          "Cache Hits: {}, Misses: {}, Hit Rate: {}%",
          hits, miss, MetricComputationUtils.computeCacheHitRate(hits, miss));
    } catch (Exception e) {
      LOG.error("Error while closing BlobStore", e);
    }
  }
}
