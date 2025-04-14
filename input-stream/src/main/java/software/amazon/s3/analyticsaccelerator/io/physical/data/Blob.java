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
import software.amazon.s3.analyticsaccelerator.util.*;

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
    String methodName = "read";
    Map<String, Object> logParams =
        LogParamsBuilder.create()
            .add("s3URI", objectKey.getS3URI().toString())
            .add("pos", pos)
            .build();
    LogUtils.logMethodEntry(LOG, methodName, logParams);

    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");
    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Current weight of blobMap in bytes is %d",
        memoryUsageAcrossBlobMap.get());

    // Lock acquisition timing
    long lockStartTime = System.nanoTime();
    lock.readLock().lock();
    double lockAcquisitionTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Read lock acquired for blob %s for position %d after %.3f seconds",
        objectKey.getS3URI(),
        pos,
        lockAcquisitionTime);

    try {
      // Make position available timing
      long makeAvailableStart = System.nanoTime();
      blockManager.makePositionAvailable(pos, ReadMode.SYNC, indexCache, memoryUsageAcrossBlobMap);
      double makeAvailableTime = (System.nanoTime() - makeAvailableStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Position made available for blob %s at position %d in %.3f seconds",
          objectKey.getS3URI(),
          pos,
          makeAvailableTime);

      // Get block and read timing
      int preJoinedBlocks = 0;

      long readStart = System.nanoTime();
      Block block = blockManager.getBlock(pos).get();
      if (block.getData1() != null && block.getData1().isDone()) {
        preJoinedBlocks++;
      }
      block.updateIsAccessed(true);
      int bytesRead = block.read(pos);
      double readTime = (System.nanoTime() - readStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Read operation completed for blob %s, Total blocks: %d, Pre-joined blocks: %d, total bytes read: %d in %.3f seconds",
          objectKey.getS3URI(),
          1,
          preJoinedBlocks,
          bytesRead,
          readTime);

      return bytesRead;
    } finally {
      // Unlock timing
      long unlockStart = System.nanoTime();
      lock.readLock().unlock();
      double unlockTime = (System.nanoTime() - unlockStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Read lock released for blob %s for position %d after %.3f seconds",
          objectKey.getS3URI().toString(),
          pos,
          unlockTime);

      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Cache stats - Hits: %d, Misses: %d, Hit Rate: %.2f%%",
          CacheStats.getHits(),
          CacheStats.getMisses(),
          CacheStats.getHitRate() * 100);

      // Total operation time
      double totalTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Total read operation completed for blob %s at position %d in %.3f seconds",
          objectKey.getS3URI(),
          pos,
          totalTime);
    }
  }

  /** clean up */
  public final void asyncCleanup() {
    String methodName = "asyncCleanup";
    Map<String, Object> logParams =
        LogParamsBuilder.create()
            .add("s3URI", objectKey.getS3URI().toString())
            .add("initialBlockCount", getBlockManager().getBlockStore().getBlocks().size())
            .build();
    LogUtils.logMethodEntry(LOG, methodName, logParams);

    if (getBlockManager().getBlockStore().getBlocks().isEmpty()) {
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Skipping cleanup for blob %s - no blocks present",
          objectKey.getS3URI());
      return;
    }

    // Lock acquisition timing
    long lockStartTime = System.nanoTime();
    lock.writeLock().lock();
    double lockAcquisitionTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Write lock acquired for blob %s after %.3f seconds",
        objectKey.getS3URI(),
        lockAcquisitionTime);

    try {
      // Cleanup timing
      long cleanupStart = System.nanoTime();
      cleanUp();
      double cleanupTime = (System.nanoTime() - cleanupStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Cleanup completed for blob %s in %.3f seconds",
          objectKey.getS3URI(),
          cleanupTime);

    } finally {
      // Unlock timing
      long unlockStart = System.nanoTime();
      lock.writeLock().unlock();
      double unlockTime = (System.nanoTime() - unlockStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Write lock released for blob %s after %.3f seconds",
          objectKey.getS3URI(),
          unlockTime);

      // Total operation time
      double totalTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Total cleanup operation completed for blob %s in %.3f seconds",
          objectKey.getS3URI(),
          totalTime);
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
    String methodName = "read";
    Map<String, Object> logParams =
        LogParamsBuilder.create()
            .add("s3URI", objectKey.getS3URI().toString())
            .add("pos", pos)
            .add("length", len)
            .add("offset", off)
            .add("bufferSize", buf.length)
            .build();
    LogUtils.logMethodEntry(LOG, methodName, logParams);

    // Validate arguments
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Current weight of blobMap in bytes is %d",
        memoryUsageAcrossBlobMap.get());

    // Lock acquisition timing
    long lockStartTime = System.nanoTime();
    lock.readLock().lock();
    double lockAcquisitionTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Read lock acquired for blob %s for position %d after %.3f seconds",
        objectKey.getS3URI(),
        pos,
        lockAcquisitionTime);

    try {
      // Make range available timing
      long makeRangeStart = System.nanoTime();
      blockManager.makeRangeAvailable(
          pos, len, ReadMode.SYNC, indexCache, memoryUsageAcrossBlobMap);
      double makeRangeTime = (System.nanoTime() - makeRangeStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Range made available for blob %s at position %d, length %d in %.3f seconds",
          objectKey.getS3URI(),
          pos,
          len,
          makeRangeTime);

      // Read operation timing
      long readStart = System.nanoTime();
      long nextPosition = pos;
      int numBytesRead = 0;
      int totalBlocks = 0;
      int preJoinedBlocks = 0;

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
        totalBlocks++;
        // Check if block's future was already completed
        if (nextBlock.getData1() != null && nextBlock.getData1().isDone()) {
          preJoinedBlocks++;
        }
        nextBlock.updateIsAccessed(true);

        int bytesRead = nextBlock.read(buf, off + numBytesRead, len - numBytesRead, nextPosition);

        if (bytesRead == -1) {
          double readTime = (System.nanoTime() - readStart) / 1_000_000_000.0;
          LogUtils.logInfo(
              LOG,
              methodName,
              logParams,
              "Read operation ended (EOF) for blob %s, total bytes read: %d in %.3f seconds",
              objectKey.getS3URI(),
              numBytesRead,
              readTime);
          return numBytesRead;
        }

        numBytesRead += bytesRead;
        nextPosition += bytesRead;
      }

      double readTime = (System.nanoTime() - readStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Read operation completed for blob %s, Total blocks: %d, Pre-joined blocks: %d, total bytes read: %d in %.3f seconds",
          objectKey.getS3URI(),
          totalBlocks,
          preJoinedBlocks,
          numBytesRead,
          readTime);

      return numBytesRead;

    } finally {
      // Unlock timing
      long unlockStart = System.nanoTime();
      lock.readLock().unlock();
      double unlockTime = (System.nanoTime() - unlockStart) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Read lock released for blob %s for position %d after %.3f seconds",
          objectKey.getS3URI(),
          pos,
          unlockTime);

      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Cache stats - Hits: %d, Misses: %d, Hit Rate: %.2f%%",
          CacheStats.getHits(),
          CacheStats.getMisses(),
          CacheStats.getHitRate() * 100);

      // Total operation time
      double totalTime = (System.nanoTime() - lockStartTime) / 1_000_000_000.0;
      LogUtils.logInfo(
          LOG,
          methodName,
          logParams,
          "Total read operation completed for blob %s at position %d in %.3f seconds",
          objectKey.getS3URI(),
          pos,
          totalTime);
    }
  }

  /** cleans data from memory */
  private void cleanUp() {
    String methodName = "cleanUp";
    Map<BlockKey, Block> blockMap = blockManager.getBlockStore().getBlocks();
    int blocksRemoved = 0; // Counter for removed blocks

    Map<String, Object> logParams =
        LogParamsBuilder.create().add("s3URI", objectKey.getS3URI().toString()).build();
    LogUtils.logMethodEntry(LOG, methodName, logParams);

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
          blocksRemoved++; // Increment counter

          LogUtils.logInfo(
              LOG,
              methodName,
              logParams,
              "Removed block from iterator with key %s-%d-%d from block store during cleanup",
              blockKey.getObjectKey().getS3URI(),
              entry.getKey().getRange().getStart(),
              entry.getKey().getRange().getEnd());

          memoryUsageAcrossBlobMap.addAndGet(-range);

        } catch (Exception e) {
          LogUtils.logInfo(
              LOG,
              methodName,
              logParams,
              "Error in removing block with key %s-%d-%d from block store during cleanup",
              blockKey.getObjectKey().getS3URI(),
              entry.getKey().getRange().getStart(),
              entry.getKey().getRange().getEnd());
        }
      }
    }

    LogUtils.logInfo(
        LOG,
        methodName,
        logParams,
        "Cleanup completed - Total blocks removed: %d, Current weight of blobMap in bytes: %d",
        blocksRemoved,
        memoryUsageAcrossBlobMap.get());
  }
  /**
   * Execute an IOPlan.
   *
   * @param plan the IOPlan to execute
   * @return the status of execution
   */
  public IOPlanExecution execute(IOPlan plan) {
    String methodName = "execute";
    Map<String, Object> logParams =
        LogParamsBuilder.create()
            .add("s3URI", objectKey.getS3URI().toString())
            .add("planRanges", plan.getPrefetchRanges())
            .build();
    LogUtils.logMethodEntry(LOG, methodName, logParams);

    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                .attribute(StreamAttributes.ioPlan(plan))
                .build(),
        () -> {
          long startTime = System.nanoTime();
          try {
            LogUtils.logInfo(
                LOG,
                methodName,
                logParams,
                "Starting to process %d ranges for blob %s",
                plan.getPrefetchRanges().size(),
                objectKey.getS3URI().toString());

            for (Range range : plan.getPrefetchRanges()) {

              this.blockManager.makeRangeAvailable(
                  range.getStart(),
                  range.getLength(),
                  ReadMode.ASYNC,
                  indexCache,
                  memoryUsageAcrossBlobMap);
            }

            double totalTime = (System.nanoTime() - startTime) / 1_000_000_000.0;
            LogUtils.logInfo(
                LOG,
                methodName,
                logParams,
                "Successfully completed IOPlan execution for blob %s in %.3f seconds",
                objectKey.getS3URI(),
                totalTime);

            return IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build();

          } catch (Exception e) {
            LogUtils.logMethodError(LOG, methodName, logParams, e);
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
