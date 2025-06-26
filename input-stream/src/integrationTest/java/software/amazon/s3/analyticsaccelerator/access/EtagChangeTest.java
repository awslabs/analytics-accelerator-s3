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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static software.amazon.s3.analyticsaccelerator.access.ChecksumAssertions.assertChecksums;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

import java.io.IOException;
import java.util.*;
import lombok.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class EtagChangeTest extends IntegrationTestBase {

  private static final int DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testChangingEtagFailsStream(S3ClientKind s3ClientKind) throws IOException {
    testChangingEtagMidStream(
        s3ClientKind, S3Object.RANDOM_16MB, AALInputStreamConfigurationKind.DEFAULT);
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testChangingEtagReturnsCachedObject(S3ClientKind s3ClientKind) throws IOException {

    // A longer cache timeout ensures the block remains in the cache after the first access,
    // so a repeated access uses the cached block, instead of making a new GET.
    Map<String, String> configMap = new HashMap<>();
    configMap.put("physicalio.memory.cleanup.frequency", "30000");
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");

    testChangingEtagAfterStreamPassesAndReturnsCachedObject(
        s3ClientKind,
        S3Object.CSV_20MB,
        S3SeekableInputStreamConfiguration.fromConfiguration(config));
  }

  /**
   * Checks to make sure we throw an error and fail the stream while reading a stream and the etag
   * changes during the read. We then do another complete read to ensure that previous failed states
   * don't affect future streams.
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param AALInputStreamConfigurationKind configuration kind
   */
  protected void testChangingEtagMidStream(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind)
      throws IOException {
    int bufferSize = (int) s3Object.getSize();
    byte[] buffer = new byte[bufferSize];

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {

      S3URI s3URI =
          s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
      S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();
      S3SeekableInputStream stream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      // Read first 100 bytes
      readAndAssert(stream, buffer, 0, 100);

      // Read next 100 bytes
      readAndAssert(stream, buffer, 100, 100);

      // Change the file
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(generateRandomBytes(bufferSize)))
          .join();

      // read the next bytes and fail.
      IOException ex =
          assertThrows(
              IOException.class,
              () -> readAndAssert(stream, buffer, 200, DEFAULT_READ_AHEAD_BYTES));
      S3Exception s3Exception =
          assertInstanceOf(S3Exception.class, ex.getCause(), "Cause should be S3Exception");
      assertEquals(412, s3Exception.statusCode(), "Expected Precondition Failed (412) status code");
      System.out.println("Failed because of etag changing, starting a new read");

      // Now reading the object till close should be successful
      StreamReadPattern streamReadPattern =
          StreamReadPatternKind.SEQUENTIAL.getStreamReadPattern(s3Object);
      Crc32CChecksum checksum = new Crc32CChecksum();
      assertDoesNotThrow(
          () ->
              executeReadPatternOnAAL(
                  s3Object,
                  s3AALClientStreamReader,
                  streamReadPattern,
                  Optional.of(checksum),
                  OpenStreamInformation.DEFAULT));
      assert (checksum.getChecksumBytes().length > 0);
    }
  }
  /**
   * Used to read and assert helps when we want to run it in a lambda.
   *
   * @param stream input stream
   * @param buffer buffer to populate
   * @param offset start pos
   * @param len how much to read
   * @throws IOException
   */
  private void readAndAssert(S3SeekableInputStream stream, byte[] buffer, int offset, int len)
      throws IOException {
    int readBytes = stream.read(buffer, offset, len);
    assertEquals(readBytes, len);
  }

  /**
   * Tests to make sure if we have read our whole object we pass and return our cached data even if
   * the etag has changed after the read is complete
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param configuration stream configuration
   * @throws IOException
   */
  protected void testChangingEtagAfterStreamPassesAndReturnsCachedObject(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull S3SeekableInputStreamConfiguration configuration)
      throws IOException {
    int bufferSize = (int) s3Object.getSize();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, configuration)) {
      S3SeekableInputStream stream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum checksum = calculateCRC32C(stream, bufferSize);

      S3URI s3URI =
          s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
      S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();

      // Change the file
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(generateRandomBytes(bufferSize)))
          .join();

      S3SeekableInputStream cacheStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum cachedChecksum = calculateCRC32C(cacheStream, bufferSize);

      // Assert checksums
      assertChecksums(checksum, cachedChecksum);
    }
  }
}
