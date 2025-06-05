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
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;

/** Block object stores the data of a stream */
public class DataBlock implements Closeable {
  /**
   * The data of the block, set after construction via {@link #setData(byte[])}. Accessed only after
   * ensuring readiness via {@link #awaitData()}.
   */
  @Nullable private byte[] data;

  @Getter private final BlockKey blockKey;
  @Getter private final long generation;
  private final CountDownLatch dataReadyLatch = new CountDownLatch(1);

  /**
   * Constructs a DataBlock object
   *
   * @param blockKey the objectKey and range of the object
   * @param generation generation of the block in a sequential read pattern
   */
  public DataBlock(@NonNull BlockKey blockKey, long generation) {
    long start = blockKey.getRange().getStart();
    long end = blockKey.getRange().getEnd();
    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);

    this.blockKey = blockKey;
    this.generation = generation;
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
    awaitData();
    int contentOffset = posToOffset(pos);
    return Byte.toUnsignedInt(this.data[contentOffset]);
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

    awaitData();

    int contentOffset = posToOffset(pos);
    int available = this.data.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    if (bytesToCopy >= 0) System.arraycopy(this.data, contentOffset, buf, off, bytesToCopy);

    return bytesToCopy;
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

  /**
   * Method to set data and reduce the dataReadyLatch to signal that data is ready
   *
   * @param data data of the block
   */
  public void setData(final byte[] data) {
    this.data = data;
    dataReadyLatch.countDown();
  }

  /** Method to wait until data is fully loaded */
  private void awaitData() throws IOException {
    try {
      dataReadyLatch.await();
    } catch (InterruptedException e) {
      throw new IOException("Failed to read data", e);
    }

    if (data == null) throw new IOException("Failed to read data");
  }

  /** Closes the {@link DataBlock} and frees up all resources it holds */
  @Override
  public void close() throws IOException {
    this.data = null;
  }
}
