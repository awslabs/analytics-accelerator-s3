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
package com.amazon.connector.s3.model;

import com.amazon.connector.s3.SeekableInputStream;
import java.nio.ByteBuffer;
import org.junit.platform.commons.util.Preconditions;

/**
 * An in-memory implementation of a seekable input stream. It is used to implement reference tests.
 * The main advantage of having a super-simple implementation of the behaviour we want from a stream
 * is that it is easy to argue about its correctness.
 */
public class InMemorySeekableStream extends SeekableInputStream {

  private ByteBuffer data;
  private long position;
  private int contentLength;

  /**
   * Creates an in memory seekable stream backed by a byte buffer.
   *
   * @param data the underlying byte buffer where data will be fetched from
   */
  public InMemorySeekableStream(byte[] data) {
    this.data = ByteBuffer.wrap(data);
    this.contentLength = data.length;
  }

  @Override
  public void seek(long pos) {
    Preconditions.condition(pos >= 0, "Position should be non-negative");
    this.position = pos;
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public int readTail(byte[] buf, int off, int n) {
    // Save position of stream
    long prevPosition = this.position;

    long tailStart = contentLength - n;
    data.position((int) tailStart);
    data.get(buf, off, n);

    // Reset position
    this.position = prevPosition;
    data.position((int) this.position);

    return n;
  }

  @Override
  public int read() {
    if (this.position >= this.contentLength) {
      return -1;
    }
    data.position((int) this.position);
    this.position++;

    return Byte.toUnsignedInt(this.data.get());
  }

  @Override
  public int read(byte[] buffer, int offset, int len) {
    if (this.position >= this.contentLength) {
      return -1;
    }

    data.position((int) this.position);

    // Only ever ask till end of byte buffer to prevent BufferUnderFlowExceptions.
    if (this.position + len >= this.contentLength) {
      len = (int) (this.contentLength - this.position);
    }

    data.get(buffer, offset, len);
    this.position += len;

    return len;
  }
}
