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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

/**
 * A container that manages a collection of {@link Block} instances. Each {@code Block} corresponds
 * to a fixed-size chunk of data based on the configured block size. This class provides methods to
 * retrieve, add, and track missing blocks within a specified data range.
 */
public class BlockStore implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStore.class);

  private final BlobStoreIndexCache indexCache;
  private final Metrics aggregatingMetrics;
  private final PhysicalIOConfiguration configuration;
  // It is safe to use Integer as key since maximum single file size is 5TB in S3
  // and if we assume that block size will be 8KB, total number of blocks is within range
  // 5 TB / 8 KB = (5 * 1024^4) / 8192 ≈ 671,088,640 blocks
  // Max int value = 2,147,483,647
  private final Map<Integer, Block> blocks;

  /**
   * Creates a new {@link BlockStore} with the specified configuration.
   *
   * @param configuration the {@link PhysicalIOConfiguration} used to define block size and other
   *     I/O settings
   * @param indexCache blobstore index cache
   * @param aggregatingMetrics blobstore metrics
   */
  public BlockStore(
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull Metrics aggregatingMetrics,
      @NonNull PhysicalIOConfiguration configuration) {
    this.indexCache = indexCache;
    this.aggregatingMetrics = aggregatingMetrics;
    this.configuration = configuration;
    blocks = new ConcurrentHashMap<>();
  }

  /**
   * Retrieves the {@link Block} containing the byte at the specified position, if it exists.
   *
   * @param pos the byte offset to locate
   * @return an {@link Optional} containing the {@code Block} if found, or empty if not present
   */
  public Optional<Block> getBlock(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    return getBlockByIndex(getPositionIndex(pos));
  }

  /**
   * Retrieves the {@link Block} at the specified index from the block store.
   *
   * @param index the index of the block to retrieve
   * @return an {@link Optional} containing the {@link Block} if present; otherwise, an empty {@link
   *     Optional}
   */
  public Optional<Block> getBlockByIndex(int index) {
    Preconditions.checkArgument(0 <= index, "`index` must not be negative");
    return Optional.ofNullable(blocks.get(index));
  }

  /**
   * Adds a new {@link Block} to the store if a block at the corresponding index doesn't already
   * exist.
   *
   * @param block the {@code Block} to add
   */
  public void add(Block block) {
    this.blocks.putIfAbsent(getBlockIndex(block), block);
  }

  /**
   * Removes the specified {@link Block} from the store and updates memory usage metrics.
   *
   * @param block the {@code Block} to remove
   */
  public void remove(Block block) {
    if (block == null) {
      return; // no-op on null input
    }

    int blockIndex = getBlockIndex(block);
    if (blocks.remove(blockIndex) != null) {
      aggregatingMetrics.reduce(MetricKey.MEMORY_USAGE, block.getBlockKey().getRange().getLength());
    }
  }

  /**
   * Returns the list of block indexes that are missing for the given byte range.
   *
   * @param range the byte range to check for missing blocks
   * @param measure whether to measure cache hits and misses. If true, metrics will be updated.
   * @return a list of missing block indexes within the specified range
   */
  public List<Integer> getMissingBlockIndexesInRange(Range range, boolean measure) {
    return getMissingBlockIndexesInRange(
        getPositionIndex(range.getStart()), getPositionIndex(range.getEnd()), measure);
  }

  private List<Integer> getMissingBlockIndexesInRange(
      int startIndex, int endIndex, boolean measure) {
    List<Integer> missingBlockIndexes = new ArrayList<>();

    for (int i = startIndex; i <= endIndex; i++) {
      if (!blocks.containsKey(i)) {
        missingBlockIndexes.add(i);

        if (measure) aggregatingMetrics.add(MetricKey.CACHE_MISS, 1L);
      } else {
        if (measure) aggregatingMetrics.add(MetricKey.CACHE_HIT, 1L);
      }
    }
    return missingBlockIndexes;
  }

  /**
   * Cleans data from memory by removing blocks that are no longer needed. This method iterates
   * through all blocks in memory and removes those that: 1. Have their data loaded AND 2. Are not
   * present in the index cache For each removed block, the method: - Removes the block from the
   * internal block store - Updates memory usage metrics
   */
  public void cleanUp() {
    Iterator<Map.Entry<Integer, Block>> iterator = blocks.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, Block> entry = iterator.next();
      Block block = entry.getValue();
      BlockKey blockKey = block.getBlockKey();
      if (block.isDataReady() && !indexCache.contains(blockKey)) {
        try {
          iterator.remove();
          aggregatingMetrics.reduce(MetricKey.MEMORY_USAGE, blockKey.getRange().getLength());
        } catch (Exception e) {
          LOG.error("Error in removing block {}", e.getMessage());
        }
      }
    }
  }

  private int getBlockIndex(Block block) {
    return getPositionIndex(block.getBlockKey().getRange().getStart());
  }

  private int getPositionIndex(long pos) {
    return (int) (pos / this.configuration.getReadBufferSize());
  }

  /**
   * Closes all {@link Block} instances in the store and clears the internal map. This should be
   * called to release any underlying resources or memory.
   */
  @Override
  public void close() {
    for (Block block : blocks.values()) {
      safeClose(block);
    }
    blocks.clear();
  }

  private void safeClose(Block block) {
    try {
      block.close();
    } catch (Exception e) {
      LOG.error("Exception when closing Block in the BlockStore", e);
    }
  }

  /**
   * Returns true if blockstore is empty
   *
   * @return true if blockstore is empty
   */
  public boolean isEmpty() {
    return this.blocks.isEmpty();
  }
}
