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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialReadProgression;
import software.amazon.s3.analyticsaccelerator.io.physical.reader.StreamReader;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.AnalyticsAcceleratorUtils;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManager implements Closeable {
  private final ObjectKey objectKey;
  private final ObjectMetadata metadata;

  @SuppressFBWarnings(
      value = "URF_UNREAD_FIELD",
      justification = "Field is injected and may be used in the future")
  private final Telemetry telemetry;

  private final PhysicalIOConfiguration configuration;
  private final Metrics aggregatingMetrics;
  private final BlobStoreIndexCache indexCache;
  private final StreamReader streamReader;
  private final BlockStore blockStore;
  private final SequentialReadProgression sequentialReadProgression;
  private final RangeOptimiser rangeOptimiser;

  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the key representing the S3 object, including its URI and ETag
   * @param objectClient the client used to fetch object content from S3
   * @param metadata metadata associated with the S3 object, including content length
   * @param telemetry the telemetry interface used for logging or instrumentation
   * @param configuration configuration for physical IO operations (e.g., read buffer size)
   * @param aggregatingMetrics the metrics aggregator for performance or usage monitoring
   * @param indexCache cache for blob index metadata (if applicable)
   * @param openStreamInformation contains stream information
   * @param threadPool Thread pool
   */
  public BlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull ObjectMetadata metadata,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull Metrics aggregatingMetrics,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull OpenStreamInformation openStreamInformation,
      @NonNull ExecutorService threadPool) {
    this.objectKey = objectKey;
    this.metadata = metadata;
    this.telemetry = telemetry;
    this.configuration = configuration;
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
    this.blockStore = new BlockStore(indexCache, aggregatingMetrics, configuration);
    this.streamReader =
        new StreamReader(
            objectClient,
            objectKey,
            threadPool,
            this::removeBlocks,
            aggregatingMetrics,
            openStreamInformation);
    this.sequentialReadProgression = new SequentialReadProgression(configuration);
    this.rangeOptimiser = new RangeOptimiser(configuration);

    prefetchSmallObject();
  }

  /**
   * Initializes the BlockManager with small object prefetching if applicable. This is done
   * asynchronously to avoid blocking the constructor.
   */
  private void prefetchSmallObject() {
    if (AnalyticsAcceleratorUtils.isSmallObject(configuration, metadata.getContentLength())) {
      try {
        makeRangeAvailable(0, metadata.getContentLength(), ReadMode.SMALL_OBJECT_PREFETCH);
      } catch (Exception e) {
        LOG.debug("Failed to prefetch small object for key: {}", objectKey.getS3URI().getKey(), e);
      }
    }
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    makeRangeAvailable(pos, 1, readMode);
  }

  /**
   * Method that ensures that a range is fully available in the object store. After calling this
   * method the BlockStore should contain all bytes in the range and we should be able to service a
   * read through the BlockStore.
   *
   * @param pos start of a read
   * @param len length of the read
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makeRangeAvailable(long pos, long len, ReadMode readMode)
      throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    long endPos = pos + len - 1;

    // Range is available, return
    if (isRangeAvailable(pos, endPos)) return;

    long generation = getGeneration(pos, readMode);

    /*
     There are three different range length we need to consider.
     1/ Length of the requested read
     2/ Read ahead bytes length
     3/ Sequential read pattern length
     We need to send the request for the largest of one of these 3 lengths
     to find the optimum request length
    */
    long maxReadLength = Math.max(len, configuration.getReadAheadBytes());

    // If generation is greater than 0, it is sequential read
    if (generation > 0) {
      maxReadLength =
          Math.max(maxReadLength, sequentialReadProgression.getSizeForGeneration(generation));
    }
    // Truncate end position to the object length
    long effectiveEnd = truncatePos(pos + maxReadLength - 1);

    // Find missing blocks for given range.
    // measure is false because we already add statistics in isRangeAvailable(),
    // so no need to add measure
    List<Integer> missingBlockIndexes =
        blockStore.getMissingBlockIndexesInRange(new Range(pos, effectiveEnd), false);

    // Return if all blocks are in store
    if (missingBlockIndexes.isEmpty()) return;

    // Split missing blocks into groups of sequential indexes that respect maximum range size
    List<List<Integer>> groupedReads = splitReads(missingBlockIndexes);

    // Process each group separately to optimize read operations
    for (List<Integer> group : groupedReads) {
      // Create blocks for this group of sequential indexes
      List<Block> blocksToFill = new ArrayList<>();
      for (int blockIndex : group) {
        BlockKey blockKey = new BlockKey(objectKey, getBlockIndexRange(blockIndex));
        Block block =
            new Block(
                blockKey,
                generation,
                this.indexCache,
                this.aggregatingMetrics,
                this.configuration.getBlockReadTimeout());
        // Add block to the store for future reference
        blockStore.add(block);
        blocksToFill.add(block);
      }

      // Perform a single read operation for this group of sequential blocks
      streamReader.read(blocksToFill, readMode);
    }
  }

  /**
   * Groups sequential block indexes into separate lists, ensuring each group doesn't exceed the
   * maximum block count.
   *
   * @param blockIndexes an ordered list of block indexes
   * @return a list of lists where each inner list contains sequential block indexes within size
   *     limits
   * @see RangeOptimiser#optimizeReads(List, long)
   */
  private List<List<Integer>> splitReads(List<Integer> blockIndexes) {
    return rangeOptimiser.optimizeReads(blockIndexes, configuration.getReadBufferSize());
  }

  /**
   * Detects sequential read pattern and finds the generation of the block
   *
   * @param pos position of the read
   * @param readMode whether this ask corresponds to a sync or async read
   * @return generation of the block
   */
  private long getGeneration(long pos, ReadMode readMode) {
    // Generation is zero for read modes which not allow request extension or first block of the
    // object
    if (!readMode.allowRequestExtension() || pos < configuration.getReadBufferSize()) return 0;

    Optional<Block> previousBlock = blockStore.getBlock(pos - 1);
    return previousBlock.map(block -> block.getGeneration() + 1).orElse(0L);
  }

  private long truncatePos(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return Math.min(pos, getLastObjectByte());
  }

  private boolean isRangeAvailable(long pos, long endPos) {
    // measure is true, since this is the first check if block exist or not
    List<Integer> missingBlockIndexes =
        blockStore.getMissingBlockIndexesInRange(new Range(pos, endPos), true);
    return missingBlockIndexes.isEmpty();
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
  }

  /**
   * Calculates the {@link Range} for a given block index within the S3 object.
   *
   * <p>The start of the range is calculated as {@code blockIndex * readBufferSize}. The end of the
   * range is the smaller of:
   *
   * <ul>
   *   <li>The last byte of the block: {@code ((blockIndex + 1) * readBufferSize) - 1}
   *   <li>The last byte of the S3 object: {@code getLastObjectByte()}
   * </ul>
   *
   * <p>This ensures that the returned range does not exceed the actual size of the object.
   *
   * @param blockIndex the index of the block for which the byte range is being calculated
   * @return a {@link Range} representing the byte range [start, end] for the specified block
   */
  private Range getBlockIndexRange(int blockIndex) {
    long start = blockIndex * configuration.getReadBufferSize();
    long end = Math.min(start + configuration.getReadBufferSize() - 1, getLastObjectByte());
    return new Range(start, end);
  }

  /**
   * Retrieves the {@link Block} containing the given position, if it exists in the block store.
   *
   * @param pos the byte position within the object to look up
   * @return an {@link Optional} containing the {@link Block} if present; otherwise, {@link
   *     Optional#empty()}
   */
  public synchronized Optional<Block> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Removes the specified {@link Block}s from the block store.
   *
   * @param blocks the list of {@link Block}s to remove
   */
  private synchronized void removeBlocks(final List<Block> blocks) {
    blocks.forEach(blockStore::remove);
  }

  /**
   * Checks whether the {@link BlockStore} currently holds any blocks.
   *
   * @return {@code true} if the block store is empty; {@code false} otherwise
   */
  public boolean isBlockStoreEmpty() {
    return this.blockStore.isEmpty();
  }

  /** cleans data from memory */
  public void cleanUp() {
    this.blockStore.cleanUp();
  }

  /** Closes the {@link BlockManager} and frees up all resources it holds */
  @Override
  public void close() {
    blockStore.close();
  }
}
