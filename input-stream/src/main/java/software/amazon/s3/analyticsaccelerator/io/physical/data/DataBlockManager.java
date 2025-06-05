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
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.reader.StreamReader;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class DataBlockManager implements Closeable {
  private final ObjectKey objectKey;
  private final ObjectMetadata metadata;
  private final PhysicalIOConfiguration configuration;
  private final StreamReader streamReader;
  private final DataBlockStore blockStore;

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object we are reading
   * @param configuration the physicalIO configuration
   * @param streamReader the object responsible for reading from S3 into blocks
   */
  public DataBlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectMetadata metadata,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull StreamReader streamReader) {
    this.objectKey = objectKey;
    this.metadata = metadata;
    this.configuration = configuration;
    this.streamReader = streamReader;
    this.blockStore = new DataBlockStore(configuration);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    if (getBlock(pos).isPresent()) return;

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
  public synchronized void makeRangeAvailable(long pos, long len, ReadMode readMode) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    long endPos = pos + len - 1;

    // Find missing blocks for given range
    List<Integer> missingBlockIndexes = blockStore.getMissingBlockIndexesInRange(pos, endPos);

    // Return if all blocks are in store
    if (missingBlockIndexes.isEmpty()) return;

    List<DataBlock> blocksToFill = new ArrayList<>();
    for (int blockIndex : missingBlockIndexes) {
      final Range range =
          new Range(
              blockIndex * configuration.getReadBufferSize(),
              Math.min((blockIndex + 1) * configuration.getReadBufferSize(), getLastObjectByte()));
      BlockKey blockKey = new BlockKey(objectKey, range);
      DataBlock block = new DataBlock(blockKey, 0);
      blocksToFill.add(block);
    }

    streamReader.read(blocksToFill, readMode);
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
  }

  private synchronized Optional<DataBlock> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /** Closes the {@link DataBlockManager} and frees up all resources it holds */
  @Override
  public void close() throws IOException {
    blockStore.close();
    streamReader.close();
  }
}
