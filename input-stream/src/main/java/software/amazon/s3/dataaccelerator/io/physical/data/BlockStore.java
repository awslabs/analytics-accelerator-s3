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
package software.amazon.s3.dataaccelerator.io.physical.data;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.dataaccelerator.common.Preconditions;
import software.amazon.s3.dataaccelerator.util.S3URI;

/** A BlockStore, which is a collection of Blocks. */
public class BlockStore implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStore.class);

  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final List<Block> blocks;

  /**
   * Constructs a new instance of a BlockStore.
   *
   * @param s3URI the object's S3 URI
   * @param metadataStore the metadata cache
   */
  public BlockStore(S3URI s3URI, MetadataStore metadataStore) {
    Preconditions.checkNotNull(s3URI, "`s3URI` must not be null");
    Preconditions.checkNotNull(metadataStore, "`metadataStore` must not be null");

    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
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

    return blocks.stream().filter(b -> b.contains(pos)).findFirst();
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
   */
  public OptionalLong findNextMissingByte(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    long nextMissingByte = pos;

    while (getBlock(nextMissingByte).isPresent()) {
      nextMissingByte = getBlock(nextMissingByte).get().getEnd() + 1;
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
    return this.metadataStore.get(s3URI).getContentLength() - 1;
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
    LOG.info("Closing BlockStore {}", s3URI);
    blocks.forEach(this::safeClose);
  }
}
