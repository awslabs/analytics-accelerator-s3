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
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.*;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {

  @Setter private byte[] data;
  @Getter private final BlockKey blockKey;
  @Getter private final long generation;

  private final BlobStoreIndexCache indexCache;
  private final CountDownLatch dataReadyLatch = new CountDownLatch(1);

  private static final Logger LOG = LoggerFactory.getLogger(Block.class);

  /**
   * Constructs a Block data.
   *
   * @param blockKey the objectkey and range of the object
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param indexCache blobstore index cache
   */
  public Block(
      @NonNull BlockKey blockKey, long generation, @NonNull BlobStoreIndexCache indexCache) {

    long start = blockKey.getRange().getStart();
    long end = blockKey.getRange().getEnd();
    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);

    this.generation = generation;
    this.blockKey = blockKey;
    this.indexCache = indexCache;
  }

  /** @return if data is loaded */
  public boolean isDataLoaded() {
    return dataReadyLatch.getCount() == 0;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    try {
      dataReadyLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Error while reading pos");
      throw new RuntimeException(e);
    }

    if (data == null) {
      throw new RuntimeException("Data is null");
    }

    indexCache.recordAccess(blockKey);
    return Byte.toUnsignedInt(this.data[posToOffset(pos)]);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte @NonNull [] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    try {
      dataReadyLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Error while reading len");
      throw new RuntimeException(e);
    }

    if (data == null) {
      throw new RuntimeException("Data is null");
    }

    indexCache.recordAccess(blockKey);
    int contentOffset = posToOffset(pos);
    int available = this.data.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = this.data[contentOffset + i];
    }

    return bytesToCopy;
  }

  /**
   * Does this block contain the position?
   *
   * @param pos the position
   * @return true if the byte at the position is contained by this block
   */
  public boolean contains(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    return this.blockKey.getRange().contains(pos);
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - this.blockKey.getRange().getStart());
  }

  /** Callback method when data load is completed */
  public void onDataLoaded() {
    dataReadyLatch.countDown();
  }

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {}
}
