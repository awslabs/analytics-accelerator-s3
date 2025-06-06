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
import java.io.InputStream;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** A naive stream reader based on the {@link S3AsyncClient} */
public class S3AsyncClientStreamReader extends S3StreamReaderBase {
  @NonNull @Getter private final S3AsyncClient s3AsyncClient;

  /**
   * Creates an instance of {@link S3AsyncClientStreamReader}
   *
   * @param s3AsyncClient an instance of {@link S3AsyncClient}
   * @param baseUri base URI for all objects
   * @param bufferSize buffer size
   */
  public S3AsyncClientStreamReader(
      @NonNull S3AsyncClient s3AsyncClient, @NonNull S3URI baseUri, int bufferSize) {
    super(baseUri, bufferSize);
    this.s3AsyncClient = s3AsyncClient;
  }

  /**
   * Reads the specified pattern
   *
   * @param s3Object S3 Object to read
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   */
  @Override
  public void readPattern(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum,
      @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {
    S3URI s3URI = s3Object.getObjectUri(this.getBaseUri());

    // Replay the pattern through series of GETs
    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      // Build base request with common parameters
      GetObjectRequest.Builder requestBuilder =
          GetObjectRequest.builder()
              .bucket(s3URI.getBucket())
              .key(s3URI.getKey())
              .range(
                  String.format(
                      "bytes=%s-%s",
                      streamRead.getStart(), streamRead.getStart() + streamRead.getLength() - 1));

      // Add encryption parameters if present
      if (openStreamInformation.getEncryptionSecrets() != null
          && openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().isPresent()) {
        String customerKey =
            openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().get();
        String customerKeyMd5 =
            openStreamInformation.getEncryptionSecrets().getSsecCustomerKeyMd5();
        requestBuilder
            .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
            .sseCustomerKey(customerKey)
            .sseCustomerKeyMD5(customerKeyMd5);
      }

      // Issue a ranged GET and get InputStream
      InputStream inputStream =
          s3AsyncClient
              .getObject(requestBuilder.build(), AsyncResponseTransformer.toBlockingInputStream())
              .join();
      // drain  bytes
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
    // do nothing - we do not take ownership of the stream
  }
}
