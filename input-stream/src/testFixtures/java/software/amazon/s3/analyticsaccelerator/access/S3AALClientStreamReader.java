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

import java.io.IOException;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Client stream reader based on DAT */
public class S3AALClientStreamReader extends S3StreamReaderBase {
  @NonNull @Getter private final S3SdkObjectClient sdkObjectClient;
  @NonNull @Getter private final S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

  /**
   * Creates an instance of {@link S3AALClientStreamReader}
   *
   * @param s3AsyncClient an instance of {@link S3AsyncClient}
   * @param configuration {@link S3SeekableInputStreamConfiguration}
   * @param baseUri base URI for all objects
   * @param bufferSize buffer size
   */
  public S3AALClientStreamReader(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull S3SeekableInputStreamConfiguration configuration,
      @NonNull S3URI baseUri,
      int bufferSize) {
    super(baseUri, bufferSize);
    // Create the SDK client, ensure it doesn't close the underlying client
    this.sdkObjectClient = new S3SdkObjectClient(s3AsyncClient, false);
    s3SeekableInputStreamFactory = new S3SeekableInputStreamFactory(sdkObjectClient, configuration);
  }

  /**
   * Creates the read stream for a given object
   *
   * @param s3Object {@link S3Object} to create the stream for
   * @param openStreamInformation contains the open stream information
   * @return read stream
   */
  public S3SeekableInputStream createReadStream(
      @NonNull S3Object s3Object, @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {
    S3URI s3URI = s3Object.getObjectUri(this.getBaseUri());
    return this.getS3SeekableInputStreamFactory().createStream(s3URI, openStreamInformation);
  }

  /**
   * Reads the specified pattern
   *
   * @param s3Object S3 Object to read
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   * @param openStreamInformation contains the open stream information
   */
  @Override
  public void readPattern(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum,
      @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {
    try (S3SeekableInputStream inputStream =
        this.createReadStream(s3Object, openStreamInformation)) {
      readPattern(s3Object, inputStream, streamReadPattern, checksum);
    }
  }

  /**
   * Reads the specified pattern
   *
   * @param s3Object S3 Object to read
   * @param inputStream read stream
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   */
  public void readPattern(
      @NonNull S3Object s3Object,
      @NonNull S3SeekableInputStream inputStream,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum)
      throws IOException {
    // Replay the pattern through a set of seeks and drains
    // Apply seeks
    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      // Seek to the start
      inputStream.seek(streamRead.getStart());
      // drain bytes of size length
      drainStream(inputStream, s3Object, checksum, streamRead.getLength());
    }
  }

  /**
   * Closes the reader
   *
   * @throws IOException if IO error occurs
   */
  @Override
  public void close() throws IOException {
    // close the factory and the client
    this.s3SeekableInputStreamFactory.close();
    this.sdkObjectClient.close();
  }
}
