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
   */
  public abstract int readTail(byte[] buf, int off, int n) throws IOException;
}
