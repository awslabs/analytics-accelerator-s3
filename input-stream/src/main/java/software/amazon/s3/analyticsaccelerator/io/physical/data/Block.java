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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.retry.DefaultRetryStrategyImpl;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryPolicy;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryStrategy;

/**
 * Represents a block of data from an object stream, identified by a {@link BlockKey} and a
 * generation number. The block's data is set asynchronously and becomes accessible only after it
 * has been marked ready.
 */
public class Block implements Closeable {
  /**
   * The underlying byte array containing this block's data. It is set asynchronously via {@link
   * #setData(byte[])} and should only be accessed through read methods after {@link #awaitData()}
   * confirms readiness.
   *
   * <p>This field is marked {@code @Nullable} because the data is not initialized at construction
   * time, which would otherwise cause static code analysis to fail.
   */
  @Nullable private byte[] data;

  @Getter private final BlockKey blockKey;
  @Getter private final long generation;

  private final BlobStoreIndexCache indexCache;
  private final Metrics aggregatingMetrics;
  private final long readTimeout;
  private final int retryCount;
  private final RetryStrategy retryStrategy;
  private final OpenStreamInformation openStreamInformation;

  /**
   * A synchronization aid that allows threads to wait until the block's data is available.
   *
   * <p>This latch is initialized with a count of 1 and is used to coordinate access to the {@code
   * data} field. When a {@link Block} is created, its {@code data} is not immediately available—it
   * must be set asynchronously via {@link #setData(byte[])}. Until that happens, any thread
   * attempting to read from this block will call {@link #awaitData()}, which internally waits on
   * this latch.
   *
   * <p>Once {@code setData(byte[])} is invoked, it sets the internal data and decrements the latch,
   * unblocking all threads waiting for the data to become available. This ensures safe and
   * race-free access to the data by multiple readers, without using explicit locks.
   *
   * <p>The latch is effectively used as a one-time gate: it transitions from closed to open exactly
   * once, after which all future readers proceed without blocking.
   */
  private final CountDownLatch dataReadyLatch = new CountDownLatch(1);

  /**
   * Constructs a {@link Block} object
   *
   * @param blockKey the key identifying the object and byte range
   * @param generation the generation number of this block in a sequential read pattern
   * @param indexCache blobstore index cache
   * @param aggregatingMetrics blobstore metrics
   * @param readTimeout read timeout in milliseconds
   * @param retryCount number of retries
   * @param openStreamInformation contains stream information
   */
  public Block(
      @NonNull BlockKey blockKey,
      long generation,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull Metrics aggregatingMetrics,
      long readTimeout,
      int retryCount,
      @NonNull OpenStreamInformation openStreamInformation) {
    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);

    this.blockKey = blockKey;
    this.generation = generation;
    this.indexCache = indexCache;
    this.aggregatingMetrics = aggregatingMetrics;
    this.readTimeout = readTimeout;
    this.retryCount = retryCount;
    this.openStreamInformation = openStreamInformation;
    this.retryStrategy = createRetryStrategy();
  }

  /**
   * Helper to construct retryStrategy
   *
   * @return a {@link RetryStrategy} to retry when timeouts are set
   * @throws RuntimeException if all retries fails and an error occurs
   */
  @SuppressWarnings("unchecked")
  private RetryStrategy createRetryStrategy() {
    RetryStrategy base = new DefaultRetryStrategyImpl();
    RetryStrategy provided = this.openStreamInformation.getRetryStrategy();

    if (provided != null) {
      base = base.merge(provided);
    }

    if (this.readTimeout > 0) {
      RetryPolicy timeoutPolicy =
          RetryPolicy.builder()
              .handle(InterruptedException.class, TimeoutException.class, IOException.class)
              .withMaxRetries(this.retryCount)
              .build();
      base = base.amend(timeoutPolicy);
    }
    return base;
  }

  /**
   * Reads a single byte at the specified absolute position in the object.
   *
   * @param pos the absolute position within the object
   * @return the unsigned byte value at the given position, as an int in [0, 255]
   * @throws IOException if the data is not ready or the position is invalid
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    awaitDataWithRetry();
    indexCache.recordAccess(this.blockKey);
    int contentOffset = posToOffset(pos);
    return Byte.toUnsignedInt(this.data[contentOffset]);
  }

  /**
   * Reads up to {@code len} bytes from the block starting at the given object position and writes
   * them into the provided buffer starting at {@code off}.
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

    awaitDataWithRetry();

    indexCache.recordAccess(this.blockKey);
    int contentOffset = posToOffset(pos);
    int available = this.data.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    if (bytesToCopy >= 0) System.arraycopy(this.data, contentOffset, buf, off, bytesToCopy);

    return bytesToCopy;
  }

  /**
   * Checks if data of the block is ready
   *
   * @return true if data is ready, false otherwise
   */
  public boolean isDataReady() {
    return dataReadyLatch.getCount() == 0;
  }

  /**
   * Converts an absolute object position to an offset within this block's data.
   *
   * @param pos the absolute position in the object
   * @return the relative offset within this block's byte array
   */
  private int posToOffset(long pos) {
    return (int) (pos - this.blockKey.getRange().getStart());
  }

  /**
   * Sets the data for this block and signals that the data is ready for reading. This method should
   * be called exactly once per block.
   *
   * @param data the byte array representing the block's data
   */
  public void setData(final byte[] data) {
    this.data = data;
    this.aggregatingMetrics.add(MetricKey.MEMORY_USAGE, data.length);
    this.indexCache.put(this.blockKey, this.blockKey.getRange().getLength());
    dataReadyLatch.countDown();
  }

  private void awaitDataWithRetry() throws IOException {
    this.retryStrategy.get(
        () -> {
          awaitData();
          return null;
        });
  }

  /**
   * Waits for the block's data to become available. This method blocks until {@link
   * #setData(byte[])} is called.
   *
   * @throws IOException if the thread is interrupted or data is not set
   */
  private void awaitData() throws IOException {
    try {
      if (!dataReadyLatch.await(readTimeout, TimeUnit.MILLISECONDS)) {
        throw new IOException(
            "Error while reading data. Request timed out after "
                + readTimeout
                + "ms while waiting for block data");
      }
    } catch (InterruptedException e) {
      throw new IOException("Error while reading data. Read interrupted while waiting for data", e);
    }

    if (data == null)
      throw new IOException("Error while reading data. Block data is null after successful await");
  }

  /**
   * The method which returns the length of the block.
   *
   * @return the length of the block
   */
  public int getLength() {
    return this.blockKey.getRange().getLength();
  }

  /** Releases the resources held by this block by clearing the internal data buffer. */
  @Override
  public void close() throws IOException {
    this.data = null;
  }
}
