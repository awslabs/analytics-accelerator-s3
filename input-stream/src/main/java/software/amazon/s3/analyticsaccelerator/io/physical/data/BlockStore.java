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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  // Maps block index to Block instances. The block index is calculated by dividing the byte
  // position
  // by the configured block size (readBufferSize). This allows direct lookup of blocks by their
  // position.
  //
  // Example: With a 128KB block size, blocks at different file positions map as follows:
  // - Block at position 0-128KB     → index 0 (0/128KB = 0)
  // - Block at position 256KB-384KB → index 2 (256KB/128KB = 2)
  // - Block at position 512KB-640KB → index 4 (512KB/128KB = 4)
  //
  // Note: Blocks may not be contiguous in the map if only certain ranges are loaded.
  //
  // Integer key is safe since max S3 file size is 5TB. With 8KB blocks:
  // 5TB / 8KB = ~671M blocks, well within Integer.MAX_VALUE (2.1B)
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
    // All methods in BlockStore which make a change in blocks map,
    // are called by BlockManager. These caller BlockManager methods
    // are synchronised so, we can use HashMap<> here rather than
    // synchronised or ConcurrentHashMap to optimize the performance
    blocks = new HashMap<>();
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
    int blockIndex = getBlockIndex(block);
    if (blocks.containsKey(blockIndex)) {
      LOG.debug("Block already exists at index {}, skipping add", blockIndex);
    } else {
      blocks.put(blockIndex, block);
    }
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
    if (blocks.remove(blockIndex) != null && block.isDataReady()) {
      aggregatingMetrics.reduce(MetricKey.MEMORY_USAGE, block.getLength());
      safeClose(block);
    }
  }

  /**
   * Returns the list of block indexes that are missing for the given byte range.
   *
   * @param range the byte range to check for missing blocks
   * @return a list of missing block indexes within the specified range
   */
  public List<Integer> getMissingBlockIndexesInRange(Range range) {
    return getMissingBlockIndexesInRange(
        getPositionIndex(range.getStart()), getPositionIndex(range.getEnd()));
  }

  private List<Integer> getMissingBlockIndexesInRange(int startIndex, int endIndex) {
    List<Integer> missingBlockIndexes = new ArrayList<>();

    for (int i = startIndex; i <= endIndex; i++) {
      if (!blocks.containsKey(i)) {
        missingBlockIndexes.add(i);
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

  /**
   * Calculates the block index for a given block based on its starting position.
   *
   * <p>Examples with 128KB block size:
   *
   * <ul>
   *   <li>Block with range [0-128KB) → index 0 (0 / 128KB = 0)
   *   <li>Block with range [128KB-256KB) → index 1 (128KB / 128KB = 1)
   *   <li>Block with range [256KB-384KB) → index 2 (256KB / 128KB = 2)
   *   <li>Block with range [384KB-512KB) → index 3 (384KB / 128KB = 3)
   * </ul>
   *
   * @param block the block to get the index for
   * @return the calculated block index
   * @see #getPositionIndex(long) for the underlying calculation
   */
  private int getBlockIndex(Block block) {
    return getPositionIndex(block.getBlockKey().getRange().getStart());
  }

  /**
   * Calculates the block index for a given byte position by dividing the position by the configured
   * block size (readBufferSize).
   *
   * <p>Examples with 128KB block size:
   *
   * <ul>
   *   <li>Position 0 → index 0 (0 / 128KB = 0)
   *   <li>Position 64KB → index 0 (64KB / 128KB = 0, same block)
   *   <li>Position 128KB → index 1 (128KB / 128KB = 1)
   *   <li>Position 256KB → index 2 (256KB / 128KB = 2)
   *   <li>Position 320KB → index 2 (320KB / 128KB = 2.5, truncated to 2)
   * </ul>
   *
   * @param pos the byte position in the file
   * @return the block index that contains this position
   */
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
