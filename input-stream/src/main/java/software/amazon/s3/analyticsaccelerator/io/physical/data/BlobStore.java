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

import static software.amazon.s3.analyticsaccelerator.util.Constants.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

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
  private AtomicLong memoryUsage;
  private long blobStoreMaxMemoryLimit;
  public static final long RESERVE_MEMORY = 200 * ONE_MB;
  private static final double EVICTION_THRESHOLD = 0.90;
  private static final double TARGET_USAGE_AFTER_EVICTION = 0.70;
  private static final Logger LOG = Logger.getLogger(BlobStore.class.getName());

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
    this.blobMap = Collections.synchronizedMap(new LinkedHashMap<ObjectKey, Blob>(16, 0.75f, true));
    if (configuration.getMaxMemoryLimitAAL() != Long.MAX_VALUE) {
      long reserveMemory =
          Math.min((long) Math.ceil(0.10 * configuration.getMaxMemoryLimitAAL()), RESERVE_MEMORY);
      this.blobStoreMaxMemoryLimit = configuration.getMaxMemoryLimitAAL() - reserveMemory;
    } else {
      this.blobStoreMaxMemoryLimit = calculateDefaultMemoryLimit();
    }
    LOG.info("BlobStore max memory limit is set to " + blobStoreMaxMemoryLimit);
    this.configuration = configuration;
    this.memoryUsage = new AtomicLong(0);
  }

  /**
   * Calculates the max memory limit of the BlobStore in case not configured
   *
   * @return the max memory limit of the BlobStore
   */
  private long calculateDefaultMemoryLimit() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
    return (long) Math.ceil(0.20 * maxHeapMemory);
  }

  /** @return the max memory limit of the BlobStore */
  public long getBlobStoreMaxMemoryLimit() {
    return blobStoreMaxMemoryLimit;
  }

  /** @return the blob map */
  public Map<ObjectKey, Blob> getBlobMap() {
    return blobMap;
  }

  /**
   * Updates the memory usage of the BlobStore and checks if eviction is required
   *
   * @param bytes to be added to the current memory usage of the BlobStore
   */
  public void updateMemoryUsage(long bytes) {
    memoryUsage.addAndGet(bytes);
    checkBlobStoreMemoryUsageAndEvictIfRequired();
  }

  /** Checks memory usage of the BlobStore and performs eviction if required */
  private void checkBlobStoreMemoryUsageAndEvictIfRequired() {
    if (shouldEvict()) {
      LOG.info(
          "Needs eviction because blobstore memory usage exceeded and currently is "
              + memoryUsage.get());
      evictBlobs();
      LOG.info("After eviction blobstore memory usage memory usage is " + memoryUsage.get());
    }
  }

  /** @return the current memory usage of BlobStore */
  public long getMemoryUsage() {
    return memoryUsage.get();
  }

  /** @return if eviction ois required for the BlobStore */
  private boolean shouldEvict() {
    return memoryUsage.get() >= EVICTION_THRESHOLD * blobStoreMaxMemoryLimit;
  }

  /**
   * Returns the size of a blob
   *
   * @param blob blob
   * @return the size of the blob
   */
  public long getBlobSize(Blob blob) {
    return blob.getBlockManager().getBlockStore().getBlocks().stream()
        .mapToLong(Block::getLength)
        .sum();
  }

  /** Evicts blobs from the BlobStore. */
  private void evictBlobs() {
    synchronized (blobMap) {
      StringBuilder sb = new StringBuilder(String.format("Blob Map Contents before eviction:%n"));
      for (Map.Entry<ObjectKey, Blob> entry : blobMap.entrySet()) {
        sb.append(String.format("  %s %n", entry.getKey().getS3URI()));
      }
      LOG.info(String.valueOf(sb));

      Iterator<Map.Entry<ObjectKey, Blob>> iterator = blobMap.entrySet().iterator();
      while (memoryUsage.get() > TARGET_USAGE_AFTER_EVICTION * blobStoreMaxMemoryLimit
          && iterator.hasNext()) {
        Map.Entry<ObjectKey, Blob> entry = iterator.next();
        Blob blob = entry.getValue();
        if (blob.getActiveReaders() == 0) {
          long blobSize = getBlobSize(blob);
          if (blobSize > 0) {
            iterator.remove();
            memoryUsage.addAndGet(-blobSize);
            LOG.info("Blob removed is " + blob.getObjectKey().getS3URI());
            blob.close();
          }
        }
      }

      sb = new StringBuilder(String.format("Blob Map Contents after eviction:%n"));
      for (Map.Entry<ObjectKey, Blob> entry : blobMap.entrySet()) {
        sb.append(String.format("  %s %n", entry.getKey().getS3URI()));
      }
      LOG.info(String.valueOf(sb));
    }
  }

  /** @return the keys in order of access */
  public List<ObjectKey> getKeyOrder() {
    return new ArrayList<>(blobMap.keySet());
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object we are computing
   * @param streamContext contains audit headers to be attached in the request header
   * @param memoryManager manages memory usage of the blobstore
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(
      ObjectKey objectKey,
      ObjectMetadata metadata,
      StreamContext streamContext,
      MemoryManager memoryManager) {
    Blob blob =
        blobMap.computeIfAbsent(
            objectKey,
            uri ->
                new Blob(
                    uri,
                    metadata,
                    new BlockManager(
                        uri,
                        objectClient,
                        metadata,
                        telemetry,
                        configuration,
                        streamContext,
                        memoryManager),
                    telemetry));

    blob.updateActiveReaders(1);
    checkBlobStoreMemoryUsageAndEvictIfRequired();
    return blob;
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
    blobMap.forEach((k, v) -> v.close());
  }
}
