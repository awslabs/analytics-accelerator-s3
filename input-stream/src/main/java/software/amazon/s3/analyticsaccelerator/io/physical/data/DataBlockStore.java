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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * A container that manages a collection of {@link DataBlock} instances. Each {@code DataBlock}
 * corresponds to a fixed-size chunk of data based on the configured block size. This class provides
 * methods to retrieve, add, and track missing blocks within a specified data range.
 */
public class DataBlockStore implements Closeable {
  private final PhysicalIOConfiguration configuration;
  // It is safe to use Integer as key since maximum single file size is 5TB in S3
  // and if we assume that block size will be 8KB, total number of blocks is within range
  // 5 TB / 8 KB = (5 * 1024^4) / 8192 ≈ 671,088,640 blocks
  // Max int value = 2,147,483,647
  private final Map<Integer, DataBlock> blocks;

  /**
   * Creates a new {@link DataBlockStore} with the specified configuration.
   *
   * @param configuration the {@link PhysicalIOConfiguration} used to define block size and other
   *     I/O settings
   */
  public DataBlockStore(@NonNull PhysicalIOConfiguration configuration) {
    this.configuration = configuration;
    blocks = new ConcurrentHashMap<>();
  }

  /**
   * Retrieves the {@link DataBlock} containing the byte at the specified position, if it exists.
   *
   * @param pos the byte offset to locate
   * @return an {@link Optional} containing the {@code DataBlock} if found, or empty if not present
   */
  public Optional<DataBlock> getBlock(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    return getBlockByIndex(getPositionIndex(pos));
  }

  private Optional<DataBlock> getBlockByIndex(int index) {
    return Optional.ofNullable(blocks.get(index));
  }

  /**
   * Adds a new {@link DataBlock} to the store if a block at the corresponding index doesn't already
   * exist.
   *
   * @param block the {@code DataBlock} to add
   */
  public void add(DataBlock block) {
    this.blocks.putIfAbsent(getBlockIndex(block), block);
  }

  /**
   * Returns the list of block indexes that are missing for the given byte range.
   *
   * @param startPos the starting byte position (inclusive)
   * @param endPos the ending byte position (inclusive)
   * @return a list of missing block indexes within the specified range
   */
  public List<Integer> getMissingBlockIndexesInRange(long startPos, long endPos) {
    return getMissingBlockIndexesInRange(getPositionIndex(startPos), getPositionIndex(endPos));
  }

  // TODO Consider using Range, otherwise add Preconditions to check start and end indexes
  private List<Integer> getMissingBlockIndexesInRange(int startIndex, int endIndex) {
    List<Integer> missingBlockIndexes = new ArrayList<>();

    for (int i = startIndex; i <= endIndex; i++) {
      if (!blocks.containsKey(i)) missingBlockIndexes.add(i);
    }
    return missingBlockIndexes;
  }

  private int getBlockIndex(DataBlock block) {
    return getPositionIndex(block.getBlockKey().getRange().getStart());
  }

  private int getPositionIndex(long pos) {
    return (int) (pos / this.configuration.getReadBufferSize());
  }

  /**
   * Closes all {@link DataBlock} instances in the store and clears the internal map. This should be
   * called to release any underlying resources or memory.
   *
   * @throws IOException if an I/O error occurs during block closure
   */
  @Override
  public void close() throws IOException {
    // TODO Memory Manager
    for (DataBlock block : blocks.values()) {
      block.close();
    }
    blocks.clear();
  }
}
