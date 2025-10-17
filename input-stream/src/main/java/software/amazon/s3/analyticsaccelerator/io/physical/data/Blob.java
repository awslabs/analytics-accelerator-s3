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
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** A Blob representing an object. */
public class Blob implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Blob.class);
  private static final String OPERATION_EXECUTE = "blob.execute";

  private final ObjectKey objectKey;
  private final BlockManager blockManager;
  private final ObjectMetadata metadata;
  private final Telemetry telemetry;

  /**
   * The ReentrantReadWriteLock manages concurrent access between read operations and eviction:<br>
   *
   * <p>Read Lock (used in read() and read(byte[], int, int, long) methods):<br>
   * - Prevents block eviction while reading is in progress<br>
   * - Multiple threads can concurrently read data<br>
   * - Ensures blocks being read cannot be evicted<br>
   *
   * <p>Write Lock (used in asyncCleanup()):<br>
   * - Exclusive lock used during block eviction/cleanup<br>
   * - Blocks any ongoing reads during cleanup<br>
   * - Ensures no threads are reading blocks while they're being evicted<br>
   *
   * <p>This locking strategy ensures that blocks aren't evicted while being read, while still
   * allowing new blocks to be read concurrently.
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Construct a new Blob.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object
   * @param blockManager the BlockManager for this object
   * @param telemetry an instance of {@link Telemetry} to use
   */
  public Blob(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectMetadata metadata,
      @NonNull BlockManager blockManager,
      @NonNull Telemetry telemetry) {

    this.objectKey = objectKey;
    this.metadata = metadata;
    this.blockManager = blockManager;
    this.telemetry = telemetry;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");

    try {
      lock.readLock().lock();
      blockManager.makePositionAvailable(pos, ReadMode.SYNC);
      Optional<Block> block = blockManager.getBlock(pos);
      return block
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      String.format(
                          "This block object key %s (for position %s) should have been available.",
                          objectKey.getS3URI(), pos)))
          .read(pos);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Reads data into the provided buffer.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    return read(buf, off, len, pos, ReadMode.SYNC);
  }

  /**
   * Reads data into the provided buffer and accepts a readMode.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @param readMode mode to define the read type
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte[] buf, int off, int len, long pos, ReadMode readMode) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    try {
      lock.readLock().lock();
      blockManager.makeRangeAvailable(pos, len, readMode);

      long nextPosition = pos;
      int numBytesRead = 0;

      while (numBytesRead < len && nextPosition < contentLength()) {
        final long nextPositionFinal = nextPosition;

        Block nextBlock =
            blockManager
                .getBlock(nextPosition)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            String.format(
                                "This block object key %s (for position %s) should have been available.",
                                objectKey.getS3URI(), nextPositionFinal)));

        int bytesRead = nextBlock.read(buf, off + numBytesRead, len - numBytesRead, nextPosition);

        if (bytesRead == -1) {
          return numBytesRead;
        }
        numBytesRead = numBytesRead + bytesRead;
        nextPosition += bytesRead;
      }

      return numBytesRead;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Execute an IOPlan.
   *
   * @param plan the IOPlan to execute
   * @param readMode the readMode for which this IoPlan is being executed
   * @return the status of execution
   */
  public IOPlanExecution execute(IOPlan plan, ReadMode readMode) throws IOException {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                .attribute(StreamAttributes.ioPlan(plan))
                .build(),
        () -> {
          try {
            for (Range range : plan.getPrefetchRanges()) {
              this.blockManager.makeRangeAvailable(range.getStart(), range.getLength(), readMode);
            }

            return IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build();
          } catch (Exception e) {
            LOG.error("Failed to submit IOPlan to PhysicalIO", e);
            return IOPlanExecution.builder().state(IOPlanState.FAILED).build();
          }
        });
  }

  /** clean up blob */
  public final void asyncCleanup() {
    if (blockManager.isBlockStoreEmpty()) {
      return;
    }
    try {
      lock.writeLock().lock();
      blockManager.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private long contentLength() {
    return metadata.getContentLength();
  }

  @Override
  public void close() {
    this.blockManager.close();
  }
}
