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
package software.amazon.s3.analyticsaccelerator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * A SeekableInputStream is like a conventional InputStream but equipped with two additional
 * operations: {@link #seek(long) seek} and {@link #getPos() getPos}. Typically, seekable streams
 * are used for random data access (i.e, data access that is not strictly sequential or requires
 * backwards seeks).
 *
 * <p>Implementations should implement {@link #close() close} to release resources.
 */
public abstract class SeekableInputStream extends InputStream {
  /**
   * Sets the offset, measured from the beginning of this stream, at which the next read occurs. The
   * offset may be set beyond the end of the file. Setting the offset beyond the end of the file
   * does not change the file length. The file length will change only by writing after the offset
   * has been set beyond the end of the file.
   *
   * @param pos the offset position, measured in bytes from the beginning of the file, at which to
   *     set the file pointer.
   * @exception IOException if {@code pos} is less than {@code 0} or if an I/O error occurs.
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Returns the current position in the stream.
   *
   * @return the position in the stream
   */
  public abstract long getPos();

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param n the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   * @throws IOException if an error occurs while reading the file
   */
  public abstract int readTail(byte[] buf, int off, int n) throws IOException;

  /**
   * Reads the list of provided ranges in parallel. Byte buffers are created using the allocate
   * method, and may be direct or non-direct depending on the implementation of the allocate method.
   * When a provided range has been fully read, the associated future for it is completed.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @param release release the buffer back to buffer pool in case of exceptions
   * @throws IOException on any IO failure
   */
  public abstract void readVectored(
      List<ObjectRange> ranges,
      final IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release)
      throws IOException;

  /**
   * Fill the provided buffer with the contents of the input source starting at {@code position} for
   * the given {@code offset} and {@code length}.
   *
   * @param position start position of the read
   * @param buffer target buffer to copy data
   * @param offset offset in the buffer to copy the data
   * @param length size of the read
   * @throws IOException if an I/O error occurs
   */
  public abstract void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException;

  /**
   * Validates the arguments for a read operation. This method is available to use in all subclasses
   * to ensure consistency.
   *
   * @param position the position to read from
   * @param buffer the buffer to read into
   * @param offset the offset in the buffer to start writing at
   * @param length the number of bytes to read
   * @throws IllegalArgumentException if the position, offset or length is negative
   * @throws NullPointerException if the buffer is null
   * @throws IndexOutOfBoundsException if the offset or length are invalid for the given buffer
   */
  protected void validatePositionedReadArgs(long position, byte[] buffer, int offset, int length) {
    Preconditions.checkNotNull(buffer, "Null destination buffer");
    Preconditions.checkArgument(length >= 0, "Length is negative");

    if (offset < 0) {
      throw new IndexOutOfBoundsException("Offset is negative");
    }

    // TODO: S3A throws an EOFException here, S3FileIO does IllegalArgumentException
    // TODO: https://github.com/awslabs/analytics-accelerator-s3/issues/84
    Preconditions.checkArgument(position >= 0, "Position is negative");
    Preconditions.checkPositionIndex(
        length,
        buffer.length - offset,
        "Too many bytes for destination buffer "
            + ": request length="
            + length
            + ", with offset ="
            + offset
            + "; buffer capacity ="
            + (buffer.length - offset));
  }
}
