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

import com.github.benmanes.caffeine.cache.Cache;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** A Blob representing an object. */
public class Blob implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Blob.class);
  private static final String OPERATION_EXECUTE = "blob.execute";

  private final ObjectKey objectKey;
  @Getter private final BlockManager blockManager;
  private final ObjectMetadata metadata;
  private final Telemetry telemetry;
  private final Cache<BlockKey, Integer> indexCache;
  @Getter private AtomicLong memoryUsageAcrossBlobMap;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

  private String now() {
    return FORMATTER.format(Instant.now());
  }

  /**
   * Construct a new Blob.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object
   * @param blockManager the BlockManager for this object
   * @param telemetry an instance of {@link Telemetry} to use
   * @param indexCache caching the block keys across all blobs
   * @param memoryUsageAcrossBlobMap memory use
   */
  public Blob(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectMetadata metadata,
      @NonNull BlockManager blockManager,
      @NonNull Telemetry telemetry,
      @NonNull Cache<BlockKey, Integer> indexCache,
      AtomicLong memoryUsageAcrossBlobMap) {

    this.objectKey = objectKey;
    this.metadata = metadata;
    this.blockManager = blockManager;
    this.telemetry = telemetry;
    this.indexCache = indexCache;
    this.memoryUsageAcrossBlobMap = memoryUsageAcrossBlobMap;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {

    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");
    LOG.info("Current weight of blobMap in bytes is {}", memoryUsageAcrossBlobMap);
    String startAttempt = now();
    LOG.info(
        "[{}] Attempting to start read operation for blob {} at position {}",
        startAttempt,
        objectKey.getS3URI(),
        pos);
    long startTime = System.currentTimeMillis();
    lock.readLock().lock();
    double lockAcquisitionTime = (System.currentTimeMillis() - startTime) / 1000.0;
    String lockAcquired = now();
    LOG.info(
        "[{}] Read lock acquired for blob {} for position {} after {} seconds",
        lockAcquired,
        objectKey.getS3URI(),
        pos,
        String.format("%.3f", lockAcquisitionTime));

    try {
      blockManager.makePositionAvailable(pos, ReadMode.SYNC, indexCache, memoryUsageAcrossBlobMap);
      Block block = blockManager.getBlock(pos).get();
      block.updateIsAccessed(true);
      int bytesRead = block.read(pos);
      return bytesRead;
    } finally {
      lock.readLock().unlock();
      LOG.info(
          "blob Cache Hits: {}, Misses: {}, Hit Rate: {}%",
          CacheStats.getHits(), CacheStats.getMisses(), CacheStats.getHitRate() * 100);
    }
  }

  /** clean up */
  public final void asyncCleanup() {
    if (getBlockManager().getBlockStore().getBlocks().isEmpty()) {
      return;
    }
    String startAttempt = now();
    LOG.info(
        "[{}] Attempting to start cleanup operation on blob {}",
        startAttempt,
        objectKey.getS3URI());
    lock.writeLock().lock();

    try {
      cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    LOG.info("Current weight of blobMap in bytes is {}", memoryUsageAcrossBlobMap);

    String startAttempt = now();
    LOG.info(
        "[{}] Attempting to start read operation for blob {} at position {}",
        startAttempt,
        objectKey.getS3URI(),
        pos);
    long startTime = System.currentTimeMillis();
    lock.readLock().lock();
    double lockAcquisitionTime = (System.currentTimeMillis() - startTime) / 1000.0;
    String lockAcquired = now();
    LOG.info(
        "[{}] Read lock acquired for blob {} for position {} after {} seconds",
        lockAcquired,
        objectKey.getS3URI(),
        pos,
        String.format("%.3f", lockAcquisitionTime));

    try {

      blockManager.makeRangeAvailable(
          pos, len, ReadMode.SYNC, indexCache, memoryUsageAcrossBlobMap);

      long nextPosition = pos;
      int numBytesRead = 0;

      while (numBytesRead < len && nextPosition < contentLength()) {
        final long nextPositionFinal = nextPosition;
        Block nextBlock =
            blockManager
                .getBlock(nextPosition)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            String.format(
                                "This block object key %s (for position %s) should have been available.",
                                objectKey.getS3URI().toString(), nextPositionFinal)));
        nextBlock.updateIsAccessed(true);

        int bytesRead = nextBlock.read(buf, off + numBytesRead, len - numBytesRead, nextPosition);

        if (bytesRead == -1) {
          return numBytesRead;
        }

        numBytesRead = numBytesRead + bytesRead;
        nextPosition += bytesRead;
      }

      return numBytesRead;
    } finally {
      lock.readLock().unlock();
      LOG.info(
          "blob Cache Hits: {}, Misses: {}, Hit Rate: {}%",
          CacheStats.getHits(), CacheStats.getMisses(), CacheStats.getHitRate() * 100);
    }
  }

  /** cleans data from memory */
  private void cleanUp() {

    Map<BlockKey, Block> blockMap = blockManager.getBlockStore().getBlocks();

    // Use an iterator to safely remove entries while iterating
    Iterator<Map.Entry<BlockKey, Block>> iterator = blockMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<BlockKey, Block> entry = iterator.next();
      BlockKey blockKey = entry.getKey();

      if (indexCache.getIfPresent(blockKey) == null) {
        // The block is not in the index cache, so remove it from the block store
        int range = blockKey.getRange().getLength();
        try {
          iterator.remove(); // Remove from the iterator as well

          LOG.info(
              "Removed block from iterator with key {} from block store during cleanup",
              blockKey.getObjectKey().getS3URI()
                  + "-"
                  + entry.getKey().getRange().getStart()
                  + "-"
                  + entry.getKey().getRange().getEnd());
          memoryUsageAcrossBlobMap.addAndGet(-range);

        } catch (Exception e) {
          LOG.info(
              "Error in removing block with key {} from block store during cleanup due to exception {} and stack {}",
              blockKey.getObjectKey().getS3URI()
                  + "-"
                  + entry.getKey().getRange().getStart()
                  + "-"
                  + entry.getKey().getRange().getEnd(),
              e.getMessage() + "-" + e.getCause(),
              e.getStackTrace());
        }
      }
    }

    LOG.info("Current weight of blobMap in bytes is {}", memoryUsageAcrossBlobMap);
  }

  /**
   * Execute an IOPlan.
   *
   * @param plan the IOPlan to execute
   * @return the status of execution
   */
  public IOPlanExecution execute(IOPlan plan) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                .attribute(StreamAttributes.ioPlan(plan))
                .build(),
        () -> {
          try {
            for (Range range : plan.getPrefetchRanges()) {
              this.blockManager.makeRangeAvailable(
                  range.getStart(),
                  range.getLength(),
                  ReadMode.ASYNC,
                  indexCache,
                  memoryUsageAcrossBlobMap);
            }

            return IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build();
          } catch (Exception e) {
            LOG.error("Failed to submit IOPlan to PhysicalIO", e);
            return IOPlanExecution.builder().state(IOPlanState.FAILED).build();
          }
        });
  }

  private long contentLength() {
    return metadata.getContentLength();
  }

  @Override
  public void close() {
    this.blockManager.close();
  }
}
