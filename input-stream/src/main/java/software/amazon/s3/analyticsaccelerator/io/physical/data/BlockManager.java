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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialPatternDetector;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialReadProgression;
import software.amazon.s3.analyticsaccelerator.io.physical.reader.StreamReader;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.*;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManager implements Closeable {
  private final ObjectKey objectKey;
  private final ObjectMetadata metadata;
  private final BlockStore blockStore;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final SequentialPatternDetector patternDetector;
  private final SequentialReadProgression sequentialReadProgression;
  private final PhysicalIOConfiguration configuration;
  private final RangeOptimiser rangeOptimiser;
  private final StreamContext streamContext;
  private final Metrics aggregatingMetrics;
  private final BlobStoreIndexCache indexCache;
  private final ExecutorService readThreadPool;

  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  private static final long blockSize = 8 * 1024; // TODO: pass in constructor
  private final StreamReader streamReader;

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param metadata the metadata for the object we are reading
   * @param configuration the physicalIO configuration
   * @param aggregatingMetrics factory metrics
   * @param indexCache blobstore index cache
   * @param readThreadPool IO thread pool for read
   */
  public BlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull ObjectMetadata metadata,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull Metrics aggregatingMetrics,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull ExecutorService readThreadPool) {
    this(
        objectKey,
        objectClient,
        metadata,
        telemetry,
        configuration,
        aggregatingMetrics,
        indexCache,
        readThreadPool,
        null);
  }

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param metadata the metadata for the object
   * @param configuration the physicalIO configuration
   * @param aggregatingMetrics factory metrics
   * @param indexCache blobstore index cache
   * @param readThreadPool IO thread pool for read
   * @param streamContext contains audit headers to be attached in the request header
   */
  public BlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull ObjectMetadata metadata,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull Metrics aggregatingMetrics,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull ExecutorService readThreadPool,
      StreamContext streamContext) {
    this.objectKey = objectKey;
    this.objectClient = objectClient;
    this.metadata = metadata;
    this.telemetry = telemetry;
    this.configuration = configuration;
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
    this.blockStore = new BlockStore(objectKey, metadata, aggregatingMetrics, indexCache);
    this.patternDetector = new SequentialPatternDetector(blockStore);
    this.sequentialReadProgression = new SequentialReadProgression(configuration);
    this.rangeOptimiser = new RangeOptimiser(configuration);
    this.readThreadPool = readThreadPool;
    this.streamContext = streamContext;
    this.streamReader = new StreamReader(objectClient, objectKey, readThreadPool, streamContext);

    prefetchSmallObject();
  }

  /**
   * Initializes the BlockManager with small object prefetching if applicable. This is done
   * asynchronously to avoid blocking the constructor.
   */
  private void prefetchSmallObject() {
    if (AnalyticsAcceleratorUtils.isSmallObject(configuration, metadata.getContentLength())) {
      CompletableFuture.runAsync(
          () -> {
            try {
              makeRangeAvailable(0, metadata.getContentLength(), ReadMode.SMALL_OBJECT_PREFETCH);
            } catch (IOException e) {
              LOG.debug(
                  "Failed to prefetch small object for key: {}", objectKey.getS3URI().getKey(), e);
            }
          });
    }
  }

  /** @return true if blockstore is empty */
  public boolean isBlockStoreEmpty() {
    return blockStore.isBlockStoreEmpty();
  }

  /**
   * Given the position of a byte, return the block holding it.
   *
   * @param pos the position of a byte
   * @return the Block holding the byte or empty if the byte is not in the BlockStore
   */
  public synchronized Optional<Block> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   * @throws IOException if an I/O error occurs
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    // Position is already available --> return corresponding block
    if (getBlock(pos).isPresent()) {
      return;
    }

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
   * @throws IOException if an I/O error occurs
   */
  public synchronized void makeRangeAvailable(long pos, long len, ReadMode readMode)
      throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    int startBlockIndex = getPositionIndex(pos);

    // In case of a sequential reading pattern, calculate the generation and adjust the requested
    // effectiveEnd of the requested range
    long effectiveEnd = pos + Math.max(len, configuration.getReadAheadBytes()) - 1;

    final long generation;
    if (readMode != ReadMode.ASYNC) {
      if (startBlockIndex == 0) generation = 0;
      else {
        Optional<Block> previousBlock = blockStore.getBlock(startBlockIndex - 1);
        generation = previousBlock.map(block -> block.getGeneration() + 1).orElse(0L);
        effectiveEnd =
            Math.max(
                effectiveEnd, pos + sequentialReadProgression.getSizeForGeneration(generation));
      }
    } else {
      generation = 0;
    }

    effectiveEnd = truncatePos(effectiveEnd);
    int endBlockIndex = getPositionIndex(effectiveEnd);

    List<Integer> missingBlockIndexes =
        blockStore.getMissingBlockIndexes(startBlockIndex, endBlockIndex);

    if (!missingBlockIndexes.isEmpty()) {

      List<Block> blocks = new ArrayList<>();
      int rangeStart = missingBlockIndexes.get(0);
      for (int index : missingBlockIndexes) {
        long blockStartPos = index * blockSize;
        Range range =
            new Range(blockStartPos, Math.min(blockStartPos + blockSize - 1, getLastObjectByte()));
        BlockKey blockKey = new BlockKey(objectKey, range);
        Block block = new Block(blockKey, generation, indexCache);
        if (blocks.isEmpty()) {
          blocks.add(block);
          blockStore.add(block);
          rangeStart = index;
        } else if (index - rangeStart <= 1024) {
          blocks.add(block);
          blockStore.add(block);
        } else {
          streamReader.read(blocks);
          blocks = new ArrayList<>();
          blocks.add(block);
          blockStore.add(block);
          rangeStart = index;
        }
      }

      streamReader.read(blocks);
    }
  }

  /**
   * Return blocks
   *
   * @param pos position
   * @param len length
   * @return list of blocks
   */
  public synchronized List<Block> getBlocks(long pos, long len) {
    int startBlockIndex = getPositionIndex(pos);
    int endBlockIndex = getPositionIndex(Math.min(pos + len - 1, getLastObjectByte()));

    List<Block> blocks = new ArrayList<>();
    for (int index = startBlockIndex; index <= endBlockIndex; index++) {
      blocks.add(blockStore.getBlockByIndex(index).get());
    }
    return blocks;
  }

  /** cleans data from memory */
  public void cleanUp() {
    blockStore.cleanUp();
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
  }

  private long truncatePos(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return Math.min(pos, getLastObjectByte());
  }

  /** Closes the {@link BlockManager} and frees up all resources it holds */
  @Override
  public void close() {
    blockStore.close();
  }

  private int getPositionIndex(long pos) {
    return (int) (pos / blockSize);
  }
}
