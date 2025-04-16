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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.stats.CacheStats;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** A BlockStore, which is a collection of Blocks. */
public class BlockStore implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStore.class);

  private final ObjectKey s3URI;
  private final ObjectMetadata metadata;
  @Getter private final List<Block> blocks;

  /**
   * Constructs a new instance of a BlockStore.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object
   */
  public BlockStore(ObjectKey objectKey, ObjectMetadata metadata) {
    Preconditions.checkNotNull(objectKey, "`objectKey` must not be null");
    Preconditions.checkNotNull(metadata, "`metadata` must not be null");

    this.s3URI = objectKey;
    this.metadata = metadata;
    this.blocks = new LinkedList<>();
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

    Optional<Block> block = blocks.stream().filter(b -> b.contains(pos)).findFirst();
    if (block.isPresent()) {
      CacheStats.recordHit();
    } else {
      CacheStats.recordMiss();
    }
    return block;
  }

  /**
   * Given a position, return the position of the next available byte to the right of the given byte
   * (or the position itself if it is present in the BlockStore). Available in this context means
   * that we already have a block that has loaded or is about to load the byte in question.
   *
   * @param pos a byte position
   * @return the position of the next available byte or empty if there is no next available byte
   */
  public OptionalLong findNextLoadedByte(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    if (getBlock(pos).isPresent()) {
      return OptionalLong.of(pos);
    }

    return blocks.stream().mapToLong(Block::getStart).filter(startPos -> pos < startPos).min();
  }

  /**
   * Given a position, return the position of the next byte that IS NOT present in the BlockStore to
   * the right of the given position.
   *
   * @param pos a byte position
   * @return the position of the next byte NOT present in the BlockStore or empty if all bytes are
   *     present
   * @throws IOException if an I/O error occurs
   */
  public OptionalLong findNextMissingByte(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    long nextMissingByte = pos;
    Optional<Block> nextBlock;
    while ((nextBlock = getBlock(nextMissingByte)).isPresent()) {
      nextMissingByte = nextBlock.get().getEnd() + 1;
    }

    return nextMissingByte <= getLastObjectByte()
        ? OptionalLong.of(nextMissingByte)
        : OptionalLong.empty();
  }

  /**
   * Add a Block to the BlockStore.
   *
   * @param block the block to add to the BlockStore
   */
  public void add(Block block) {
    Preconditions.checkNotNull(block, "`block` must not be null");

    this.blocks.add(block);
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
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
    blocks.forEach(this::safeClose);
  }
}
