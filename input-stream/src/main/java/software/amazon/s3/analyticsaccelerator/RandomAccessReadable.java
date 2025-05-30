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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;

/** An InputStream-like entity implementing blocking random-access reads. */
public interface RandomAccessReadable extends Closeable {
  /**
   * Returns object metadata.
   *
   * @return the metadata of the object.
   * @throws IOException if an I/O error occurs
   */
  ObjectMetadata metadata() throws IOException;

  /**
   * Reads a byte from the underlying object
   *
   * @param pos the position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an error occurs while reading the file
   */
  int read(long pos) throws IOException;

  /**
   * Reads request data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an error occurs while reading the file
   */
  int read(byte[] buf, int off, int len, long pos) throws IOException;

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   * @throws IOException if an error occurs while reading the file
   */
  int readTail(byte[] buf, int off, int len) throws IOException;

  /**
   * Fetches the list of provided ranges in parallel. Byte buffers are created using the allocate
   * method, and may be direct or non-direct depending on the implementation of the allocate method.
   * When a provided range has been fully read, the associated future for it is completed.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException on any IO failure
   */
  void readVectored(List<ObjectRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException;
}
