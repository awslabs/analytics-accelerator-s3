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
package software.amazon.s3.analyticsaccelerator.common;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.Value;

/**
 * This class is a container for information required when doing asynchronous reads. It allows for a
 * future to be passed in, which is completed when the data requested has been read into the
 * ByteBuffer.
 */
@Value
public class ObjectRange {
  CompletableFuture<ByteBuffer> byteBuffer;
  long offset;
  int length;

  /**
   * Constructor to create object range.
   *
   * @param byteBuffer future to be completed when read is completed.
   * @param offset position to start the read from
   * @param length length of the read
   */
  public ObjectRange(CompletableFuture<ByteBuffer> byteBuffer, long offset, int length) {
    Preconditions.checkNotNull(byteBuffer);
    Preconditions.checkArgument(offset >= 0);
    Preconditions.checkArgument(length >= 0);

    this.byteBuffer = byteBuffer;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public String toString() {
    return String.format(
        "offset: %d, length: %d, completable-future: %s", offset, length, byteBuffer.toString());
  }
}
