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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/**
 * High throughput seekable stream used to read data from Amazon S3.
 *
 * <p>Don't share between threads. The current implementation is not thread safe in that calling
 * {@link #seek(long) seek} will modify the position of the stream and the behaviour of calling
 * {@link #seek(long) seek} and {@link #read() read} concurrently from two different threads is
 * undefined.
 */
public class S3SeekableInputStream extends SeekableInputStream {
  private final LogicalIO logicalIO;
  private final Telemetry telemetry;
  private final S3URI s3URI;
  private long position;
  private boolean closed;
  private static final int EOF = -1;

  private static final String OPERATION_READ = "stream.read";
  private static final String FLAVOR_TAIL = "tail";
  private static final String FLAVOR_BYTE = "byte";

  private static final String OPERATION_STREAM_CLOSE = "seekablestream.close";
  private final long streamBirth = System.nanoTime();

  /**
   * Given a LogicalIO, creates a new instance of {@link S3SeekableInputStream}.
   *
   * @param s3URI the object this stream is using
   * @param logicalIO already initialised LogicalIO
   * @param telemetry The {@link Telemetry} to use to report measurements.
   */
  S3SeekableInputStream(
      @NonNull S3URI s3URI, @NonNull LogicalIO logicalIO, @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.logicalIO = logicalIO;
    this.telemetry = telemetry;
    this.position = 0;
    this.closed = false;
  }

  /**
   * Reads the next byte of data from the input stream. The value byte is returned as an <code>int
   * </code> in the range <code>0</code> to <code>255</code>. If no byte is available because the
   * end of the stream has been reached, the value <code>-1</code> is returned. This method blocks
   * until input data is available, the end of the stream is detected, or an exception is thrown.
   *
   * <p>A subclass must provide an implementation of this method.
   *
   * @return the next byte of data, or <code>-1</code> if the end of the stream is reached.
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public int read() throws IOException {
    throwIfClosed("cannot read from closed stream");

    // -1 if we are past the end of the stream
    if (this.position >= getContentLength()) {
      return EOF;
    }

    // Delegate to the LogicalIO and advance the position by 1
    return this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.variant(FLAVOR_BYTE))
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(this.getPos(), this.getPos()))
                .build(),
        () -> {
          int byteRead = this.logicalIO.read(this.position);
          advancePosition(1);
          return byteRead;
        });
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into an array of bytes. An
   * attempt is made to read as many as <code>len</code> bytes, but a smaller number may be read.
   * The number of bytes actually read is returned as an integer.
   *
   * <p>This method blocks until input data is available, end of file is detected, or an exception
   * is thrown.
   *
   * <p>If no byte is available because the stream is at end of file, the value <code>-1</code> is
   * returned; otherwise, at least one byte is read and stored into <code>b</code>.
   *
   * <p>The first byte read is stored into element <code>b[off]</code>, the next one into <code>
   * b[off+1]</code>, and so on. The number of bytes read is, at most, equal to <code>len</code>.
   * Let <i>k</i> be the number of bytes actually read; these bytes will be stored in elements
   * <code>b[off]</code> through <code>b[off+</code><i>k</i><code>-1]</code>, leaving elements
   * <code>b[off+</code><i>k</i><code>]</code> through <code>b[off+len-1]</code> unaffected.
   *
   * <p>In every case, elements <code>b[0]</code> through <code>b[off]</code> and elements <code>
   * b[off+len]</code> through <code>b[b.length-1]</code> are unaffected.
   *
   * @param buffer the buffer into which the data is read.
   * @param offset the start offset in array <code>b</code> at which the data is written.
   * @param length the maximum number of bytes to read.
   * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more
   *     data because the end of the stream has been reached.
   * @exception IOException If the first byte cannot be read for any reason other than end of file,
   *     or if the input stream has been closed, or if some other I/O error occurs.
   * @exception NullPointerException If <code>b</code> is <code>null</code>.
   * @exception IndexOutOfBoundsException If <code>off</code> is negative, <code>len</code> is
   *     negative, or <code>len</code> is greater than <code>b.length - off</code>
   * @see java.io.InputStream#read()
   */
  @Override
  public int read(byte @NonNull [] buffer, int offset, int length) throws IOException {
    throwIfClosed("cannot read from closed stream");
    validatePositionedReadArgs(position, buffer, offset, length);

    if (length == 0) {
      return 0;
    } else if (this.position >= getContentLength()) {
      return EOF;
    }

    return this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.etag(this.logicalIO.metadata().getEtag()))
                .attribute(StreamAttributes.range(position, position + length - 1))
                .build(),
        () -> {
          // Delegate to the LogicalIO and advance the position accordingly
          int bytesRead = this.logicalIO.read(buffer, offset, length, position);
          return advancePosition(bytesRead);
        });
  }

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
  @Override
  public void seek(long pos) throws IOException {
    // TODO: S3A throws an EOFException here, S3FileIO does IllegalArgumentException
    // TODO: https://github.com/awslabs/analytics-accelerator-s3/issues/84
    Preconditions.checkArgument(pos >= 0, "position must be non-negative");
    throwIfClosed("cannot seek on closed stream");

    // As we are seeking lazily, we support seek beyond the stream size .
    this.position = pos;
  }

  /**
   * Returns the current position in the stream.
   *
   * @return the position in the stream
   */
  @Override
  public long getPos() {
    return this.position;
  }

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buffer buffer to read data into
   * @param offset start position in buffer at which data is written
   * @param length the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   */
  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    throwIfClosed("cannot read from closed stream");
    validatePositionedReadArgs(position, buffer, offset, length);

    if (length == 0) {
      return 0;
    }

    return this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.variant(FLAVOR_TAIL))
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.etag(this.logicalIO.metadata().getEtag()))
                .attribute(
                    StreamAttributes.range(getContentLength() - length, getContentLength() - 1))
                .build(),
        () -> logicalIO.readTail(buffer, offset, length));
  }

  @Override
  public void readVectored(
      List<ObjectRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)
      throws IOException {
    Preconditions.checkNotNull(ranges, "ranges must not be null");
    Preconditions.checkNotNull(allocate, "allocate must not be null");

    logicalIO.readVectored(ranges, allocate);
  }

  /**
   * Releases all resources associated with the {@link S3SeekableInputStream}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_STREAM_CLOSE)
                .attribute(
                    StreamAttributes.streamRelativeTimestamp(System.nanoTime() - streamBirth))
                .build(),
        this.logicalIO::close);

    // Flush telemetry after a stream closes to have full coverage of all operations of this stream
    this.telemetry.flush();
    this.closed = true;
  }

  /**
   * Returns the length of the byte content of the stream.
   *
   * @return the length of the byte content of the stream.
   * @throws IOException if an I/O error occurs
   */
  private long getContentLength() throws IOException {
    return this.logicalIO.metadata().getContentLength();
  }

  /**
   * Advance the position based on the bytes read. If `bytesRead` is negative, we do not advance
   *
   * @param bytesRead - result of {@link LogicalIO#read}
   * @return bytesRead.
   */
  private int advancePosition(int bytesRead) {
    if (bytesRead >= 0) {
      this.position += bytesRead;
    }
    return bytesRead;
  }

  private void throwIfClosed(String msg) throws IOException {
    if (closed) {
      throw new IOException(msg);
    }
  }
}
