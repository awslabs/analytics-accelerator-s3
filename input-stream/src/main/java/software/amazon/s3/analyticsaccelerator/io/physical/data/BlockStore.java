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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** A BlockStore, which is a collection of Blocks. */
public class BlockStore implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStore.class);

  private final ObjectKey s3URI;
  private final ObjectMetadata metadata;
  private final Map<Integer, Block> blocks;
  private final Metrics aggregatingMetrics;
  private final BlobStoreIndexCache indexCache;
  private static final long blockSize = 8 * 1024; // TODO: pass in constructor

  /**
   * Constructs a new instance of a BlockStore.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object
   * @param aggregatingMetrics blobstore metrics
   * @param indexCache blobstore index cache
   */
  public BlockStore(
      ObjectKey objectKey,
      ObjectMetadata metadata,
      Metrics aggregatingMetrics,
      BlobStoreIndexCache indexCache) {
    Preconditions.checkNotNull(objectKey, "`objectKey` must not be null");
    Preconditions.checkNotNull(metadata, "`metadata` must not be null");

    this.s3URI = objectKey;
    this.metadata = metadata;
    this.blocks = new ConcurrentHashMap<>();
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
  }

  /**
   * Returns true if blockstore is empty
   *
   * @return true if blockstore is empty
   */
  public boolean isBlockStoreEmpty() {
    return blocks.isEmpty();
  }

  /**
   * Given a position, return the Block holding the byte at that position.
   *
   * @param pos the position of the byte
   * @return the Block containing the byte from the BlockStore or empty if the byte is not present
   *     in the BlockStore
   */
  public Optional<Block> getBlock(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    Optional<Block> block = Optional.ofNullable(blocks.getOrDefault(getPositionIndex(pos), null));
    if (block.isPresent()) {
      aggregatingMetrics.add(MetricKey.CACHE_HIT, 1L);
    } else {
      aggregatingMetrics.add(MetricKey.CACHE_MISS, 1L);
    }
    return block;
  }

  /**
   * Return blocks by index
   *
   * @param index index
   * @return block
   */
  public Optional<Block> getBlockByIndex(int index) {
    return Optional.ofNullable(blocks.getOrDefault(index, null));
  }

  /**
   * Given a position, return the position of the next available byte to the right of the given byte
   * (or the position itself if it is present in the BlockStore). Available in this context means
   * that we already have a block that has loaded or is about to load the byte in question.
   *
   * @param pos a byte position
   * @return the position of the next available byte or empty if there is no next available byte
   */
  //  public OptionalLong findNextLoadedByte(long pos) {
  //    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
  //
  //    if (getBlock(pos).isPresent()) {
  //      return OptionalLong.of(pos);
  //    }
  //
  //    return blocks.values().stream()
  //        .mapToLong(block -> block.getBlockKey().getRange().getStart())
  //        .filter(startPos -> pos < startPos)
  //        .min();
  //  }

  /**
   * Given a position, return the position of the next byte that IS NOT present in the BlockStore to
   * the right of the given position.
   *
   * @param pos a byte position
   * @return the position of the next byte NOT present in the BlockStore or empty if all bytes are
   *     present
   * @throws IOException if an I/O error occurs
   */
  //  public OptionalLong findNextMissingByte(long pos) throws IOException {
  //    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
  //
  //    long nextMissingByte = pos;
  //    Optional<Block> nextBlock;
  //    while ((nextBlock = getBlock(nextMissingByte)).isPresent()) {
  //      nextMissingByte = nextBlock.get().getBlockKey().getRange().getEnd() + 1;
  //    }
  //
  //    return nextMissingByte <= getLastObjectByte()
  //        ? OptionalLong.of(nextMissingByte)
  //        : OptionalLong.empty();
  //  }

  /**
   * Add a Block to the BlockStore.
   *
   * @param block the block to add to the BlockStore
   */
  public void add(Block block) {
    Preconditions.checkNotNull(block, "`block` must not be null");

    this.blocks.putIfAbsent(getBlockIndex(block), block);
  }

  private int getBlockIndex(Block block) {
    return getPositionIndex(block.getBlockKey().getRange().getStart());
  }

  private int getPositionIndex(long pos) {
    return (int) (pos / blockSize);
  }

  /**
   * Cleans data from memory by removing blocks that are no longer needed. This method iterates
   * through all blocks in memory and removes those that: 1. Have their data loaded AND 2. Are not
   * present in the index cache For each removed block, the method: - Removes the block from the
   * internal block store - Updates memory usage metrics
   */
  public void cleanUp() {

    //    Iterator<Map.Entry<BlockKey, Block>> iterator = blocks.entrySet().iterator();
    //
    //    while (iterator.hasNext()) {
    //      Map.Entry<BlockKey, Block> entry = iterator.next();
    //      BlockKey blockKey = entry.getKey();
    //
    //      if (entry.getValue().isDataLoaded() && !indexCache.contains(blockKey)) {
    //        // The block is not in the index cache, so remove it from the block store
    //        int range = blockKey.getRange().getLength();
    //        try {
    //          iterator.remove(); // Remove from the iterator as well
    //          aggregatingMetrics.reduce(MetricKey.MEMORY_USAGE, range);
    //          LOG.debug(
    //              "Removed block with key {}-{}-{} from block store during cleanup",
    //              blockKey.getObjectKey().getS3URI(),
    //              blockKey.getRange().getStart(),
    //              blockKey.getRange().getEnd());
    //
    //        } catch (Exception e) {
    //          LOG.error("Error in removing block {}", e.getMessage());
    //        }
    //      }
    //    }
  }

  /**
   * Return missing block indexes
   *
   * @param startIndex start
   * @param endIndex end
   * @return list of indexes
   */
  public List<Integer> getMissingBlockIndexes(int startIndex, int endIndex) {
    // TODO Should be thread safe
    List<Integer> missingBlockIndexes = new ArrayList<>();

    for (int index = startIndex; index <= endIndex; index++) {
      if (!blocks.containsKey(index)) missingBlockIndexes.add(index);
    }

    return missingBlockIndexes;
  }

  private void safeClose(Block block) {
    try {
      block.close();
    } catch (Exception e) {
      LOG.error("Exception when closing Block in the BlockStore", e);
    }
  }

  @Override
  public void close() {
    blocks.forEach((key, block) -> this.safeClose(block));
  }
}
