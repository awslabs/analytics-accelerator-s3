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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.reader.StreamReader;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class DataBlockManager implements Closeable {
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
  private final DataBlockStore blockStore;

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
  public DataBlockManager(
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
    this.streamReader =
        new StreamReader(objectClient, objectKey, threadPool, openStreamInformation);
    this.blockStore = new DataBlockStore(indexCache, aggregatingMetrics, configuration);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) {
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
      DataBlock block = new DataBlock(blockKey, 0, this.indexCache, this.aggregatingMetrics);
      blockStore.add(block);
      blocksToFill.add(block);
    }

    streamReader.read(blocksToFill, readMode);
  }

  /**
   * Retrieves all {@link DataBlock}s that cover the specified byte range {@code [pos, pos + len)}.
   *
   * @param pos the starting byte position of the desired range (inclusive)
   * @param len the number of bytes to include in the range
   * @return a list of {@link DataBlock}s that together cover the specified range
   */
  public synchronized List<DataBlock> getBlocks(long pos, long len) {
    // TODO This method assumes that all required blocks are already present in the BlockStore.
    // If any block is missing, code will throw exception. We need to handle this case
    int startBlockIndex = getPositionIndex(pos);
    int endBlockIndex = getPositionIndex(Math.min(pos + len - 1, getLastObjectByte()));

    List<DataBlock> blocks = new ArrayList<>();
    for (int index = startBlockIndex; index <= endBlockIndex; index++) {
      blocks.add(blockStore.getBlockByIndex(index).get());
    }
    return blocks;
  }

  private int getPositionIndex(long pos) {
    return (int) (pos / this.configuration.getReadBufferSize());
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
  }

  /**
   * Retrieves the {@link DataBlock} containing the given position, if it exists in the block store.
   *
   * @param pos the byte position within the object to look up
   * @return an {@link Optional} containing the {@link DataBlock} if present; otherwise, {@link
   *     Optional#empty()}
   */
  public synchronized Optional<DataBlock> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Checks whether the {@link DataBlockStore} currently holds any blocks.
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

  /** Closes the {@link DataBlockManager} and frees up all resources it holds */
  @Override
  public void close() {}
}
