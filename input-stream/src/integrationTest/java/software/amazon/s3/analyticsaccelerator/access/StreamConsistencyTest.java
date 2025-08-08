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
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import lombok.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Tests stream consistency across different metadata TTL and object change scenarios */
public class StreamConsistencyTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("singleStreamMetadataExpiresTests")
  void testStreamWhenMetadataTTLExpires(
      S3ClientKind s3ClientKind, S3Object s3Object, String ttlTime)
      throws IOException, InterruptedException {
    boolean shouldThrow412 = s3Object.getSize() >= 8 * ONE_MB;
    testStreamBehaviorAfterMetadataExpiry(s3ClientKind, s3Object, shouldThrow412, ttlTime);
  }

  @ParameterizedTest
  @MethodSource("singleStreamValidMetadataTests")
  void testStreamWhenMetadataTTLDoesNotExpire(
      S3ClientKind s3ClientKind, S3Object s3Object, String ttlTime) throws IOException {
    testStreamCachingBehaviorWithValidMetadata(s3ClientKind, s3Object, ttlTime);
  }

  @ParameterizedTest
  @MethodSource("crossStreamTests")
  void testMultipleStreamWhenMetadataTTLExpires(
      S3ClientKind s3ClientKind, S3Object s3Object, String ttlTime)
      throws IOException, InterruptedException {
    testNewStreamDetectsObjectUpdateAfterMetadataExpiry(s3ClientKind, s3Object, ttlTime);
  }

  /**
   * Test single stream consistency when metadata expires during stream lifetime
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param shouldThrow412 whether the test should expect a 412
   * @param ttlTime TTL time in milliseconds
   */
  protected void testStreamBehaviorAfterMetadataExpiry(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      boolean shouldThrow412,
      String ttlTime)
      throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("physicalio.metadatastore.ttl", ttlTime);
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    S3URI s3URI =
        s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
    S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      S3SeekableInputStream stream1 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      byte[] data1 = new byte[1000];
      int bytesRead1 = stream1.read(data1);
      assertTrue(bytesRead1 > 0, "Should read some data");

      int ttlMs = Integer.parseInt(ttlTime);
      Thread.sleep(ttlMs + 50); // Wait for metadata TTL expiry

      byte[] newData = generateRandomBytes((int) s3Object.getSize());
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(newData))
          .join();

      long seekPosition = s3Object.getSize() - 1000;
      stream1.seek(seekPosition);

      if (shouldThrow412) {
        // Large objects: should throw 412 when accessing uncached blocks
        IOException ex =
            assertThrows(
                IOException.class,
                () -> {
                  byte[] data2 = new byte[1000];
                  int bytesRead2 = stream1.read(data2);
                  assertTrue(bytesRead2 > 0, "Should read data before exception");
                });
        S3Exception s3Exception =
            assertInstanceOf(
                S3Exception.class, ex.getCause(), "IOException should be caused by S3Exception");
        assertEquals(
            412, s3Exception.statusCode(), "Expected Precondition Failed (412) status code");
      } else {
        byte[] data2 = new byte[1000];
        assertDoesNotThrow(
            () -> stream1.read(data2), "Should read from prefetched cache without 412 error");
      }

      stream1.close();
    }
  }

  /**
   * Test single stream caching behavior when metadata remains valid
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param ttlTime TTL time in milliseconds
   */
  protected void testStreamCachingBehaviorWithValidMetadata(
      @NonNull S3ClientKind s3ClientKind, @NonNull S3Object s3Object, String ttlTime)
      throws IOException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("physicalio.metadatastore.ttl", ttlTime);
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      // Create single stream - metadata is attached during stream creation
      S3SeekableInputStream stream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      // Read from beginning - triggers initial GET request
      byte[] buffer1 = new byte[1000];
      int bytesRead1 = stream.read(buffer1);
      assertTrue(bytesRead1 > 0, "Should read data successfully");

      int getRequestsAfterFirstRead =
          (int)
              s3AALClientStreamReader
                  .getS3SeekableInputStreamFactory()
                  .getMetrics()
                  .get(MetricKey.GET_REQUEST_COUNT);

      // Read from end of file - tests caching behavior
      stream.seek(s3Object.getSize() - 1000);
      byte[] buffer2 = new byte[1000];
      int bytesRead2 = stream.read(buffer2);
      assertTrue(bytesRead2 > 0, "Should read data from end of file successfully");

      int getRequestsAfterEndRead =
          (int)
              s3AALClientStreamReader
                  .getS3SeekableInputStreamFactory()
                  .getMetrics()
                  .get(MetricKey.GET_REQUEST_COUNT);

      stream.close();

      // Verify only 1 HEAD request was made (during stream creation)
      assertEquals(
          1,
          (int)
              s3AALClientStreamReader
                  .getS3SeekableInputStreamFactory()
                  .getMetrics()
                  .get(MetricKey.HEAD_REQUEST_COUNT),
          "Should make only 1 HEAD request during stream creation");

      // Verify GET request behavior based on object size
      if (s3Object.getSize() < 8 * ONE_MB) {
        // Small objects: Fully cached after first read, no additional GET for end read
        assertEquals(
            getRequestsAfterFirstRead,
            getRequestsAfterEndRead,
            "Small objects should not make additional GET requests - end data is cached");
      } else {
        // Large objects: Only first 8MB cached, need additional GET for end read
        assertEquals(
            getRequestsAfterFirstRead + 1,
            getRequestsAfterEndRead,
            "Large objects should make exactly 1 additional GET request for end data");
      }
    }
  }

  /**
   * Test cross-stream consistency when object changes after metadata expiry
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param ttlTime TTL time in milliseconds
   */
  protected void testNewStreamDetectsObjectUpdateAfterMetadataExpiry(
      @NonNull S3ClientKind s3ClientKind, @NonNull S3Object s3Object, String ttlTime)
      throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("physicalio.metadatastore.ttl", ttlTime);
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    S3URI s3URI =
        s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
    S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();
    int bufferSize = (int) s3Object.getSize();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      // Capture original data before any changes
      byte[] originalDataOnS3 = new byte[bufferSize];
      try (S3SeekableInputStream captureStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT)) {
        captureStream.readFully(0, originalDataOnS3, 0, bufferSize);
      }

      S3SeekableInputStream stream1 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum originalChecksum = calculateCRC32C(stream1, bufferSize);
      stream1.close();

      int ttlMs = Integer.parseInt(ttlTime);
      Thread.sleep(ttlMs + 50); // Wait for TTL expiry

      // Update the object (change Etag)
      byte[] newData = generateRandomBytes(bufferSize);
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(newData))
          .join();

      S3SeekableInputStream stream2 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum updatedChecksum = calculateCRC32C(stream2, bufferSize);
      stream2.close();

      // Calculate expected checksums for verification
      Crc32CChecksum expectedOriginalChecksum = new Crc32CChecksum();
      expectedOriginalChecksum.update(originalDataOnS3, 0, originalDataOnS3.length);

      Crc32CChecksum expectedUpdatedChecksum = new Crc32CChecksum();
      expectedUpdatedChecksum.update(newData, 0, newData.length);

      // Verify first stream served original data correctly
      assertArrayEquals(
          expectedOriginalChecksum.getChecksumBytes(),
          originalChecksum.getChecksumBytes(),
          "First stream should serve original data from S3");

      // Verify second stream served updated data correctly
      assertArrayEquals(
          expectedUpdatedChecksum.getChecksumBytes(),
          updatedChecksum.getChecksumBytes(),
          "Second stream should serve updated data");

      // Checksums should be different (object was updated)
      assertNotEquals(
          originalChecksum.getChecksumBytes(),
          updatedChecksum.getChecksumBytes(),
          "Checksums should differ after object update");
    }
  }

  static Stream<Arguments> singleStreamMetadataExpiresTests() {
    List<Arguments> testCases = new ArrayList<>();
    for (S3ClientKind clientKind : getS3ClientKinds()) {
      // Test single stream behavior when metadata expires, different object sizes
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_4MB, "100"));
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_16MB, "100"));
    }
    return testCases.stream();
  }

  static Stream<Arguments> singleStreamValidMetadataTests() {
    List<Arguments> testCases = new ArrayList<>();
    for (S3ClientKind clientKind : getS3ClientKinds()) {
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_4MB, "5000"));
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_16MB, "5000"));
    }
    return testCases.stream();
  }

  static Stream<Arguments> crossStreamTests() {
    List<Arguments> testCases = new ArrayList<>();
    for (S3ClientKind clientKind : getS3ClientKinds()) {
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_1MB, "50"));
      testCases.add(Arguments.of(clientKind, S3Object.RANDOM_16MB, "50"));
    }
    return testCases.stream();
  }
}
