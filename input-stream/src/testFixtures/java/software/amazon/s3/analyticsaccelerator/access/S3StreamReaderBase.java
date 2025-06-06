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
package software.amazon.s3.analyticsaccelerator.access;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Base class for all readers from S3 */
@Getter
public abstract class S3StreamReaderBase implements Closeable {
  @NonNull private final S3URI baseUri;
  private final int bufferSize;
  /**
   * Creates a new instance of {@link S3StreamReaderBase}
   *
   * @param baseUri base URI for all object
   * @param bufferSize buffer size
   */
  protected S3StreamReaderBase(@NonNull S3URI baseUri, int bufferSize) {
    this.baseUri = baseUri;
    this.bufferSize = bufferSize;
  }

  /**
   * Replays the specified pattern on top of the stream
   *
   * @param s3Object S3 Object to read
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   * @param openStreamInformation contains the open stream information
   */
  public abstract void readPattern(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum,
      @NonNull OpenStreamInformation openStreamInformation)
      throws IOException;

  /**
   * Drains the stream associated with a given option based on the specified position and length.
   * Updates the supplied checksum, if passed
   *
   * @param inputStream the {@link InputStream}
   * @param s3Object {@link S3Object}
   * @param checksum optional checksum, to update
   * @param length of data to read
   * @throws IOException thrown on IO error
   */
  protected void drainStream(
      @NonNull InputStream inputStream,
      @NonNull S3Object s3Object,
      @NonNull Optional<Crc32CChecksum> checksum,
      long length)
      throws IOException {
    // Set buffer size to min between the object size and whatever the config specifies
    int bufferSize = (int) Math.min(this.bufferSize, s3Object.getSize());
    byte[] buffer = new byte[bufferSize];
    // Adjust the size to not exceed the object size
    length = Math.min(length, s3Object.getSize());

    // Now just read sequentially
    long totalReadBytes = 0;
    int readBytes = 0;
    do {
      // determine the size to read - it's either the buffer size or less, if we are getting to the
      // end
      int readSize = (int) Math.min(bufferSize, length - totalReadBytes);
      if (readSize <= 0) {
        break;
      }
      readBytes = inputStream.read(buffer, 0, readSize);
      if (readBytes > 0) {
        // update the total size read
        totalReadBytes += readBytes;
        // update the checksum, if passed
        checksum.ifPresent(c -> c.update(buffer, 0, readSize));
      }

    } while (totalReadBytes < length);

    // Verify that we have drained the whole thing
    if (totalReadBytes != length) {
      throw new IllegalStateException("Read " + totalReadBytes + " bytes but expected " + length);
    }
  }
}
