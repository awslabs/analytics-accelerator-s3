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

import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class ParquetFooterCachingIntegrationTest extends ParquetIntegrationTestBase {

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testBasicFooterPrefetch(S3ClientKind s3ClientKind) throws IOException, InterruptedException {
    S3SeekableInputStreamConfiguration streamConfig = createFooterConfig(FOOTER_PREFETCH_SIZE);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, streamConfig)) {
      S3Object file = S3Object.VALID_SMALL_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        long requestsAfterOpen =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(1, requestsAfterOpen, "Should have 1 request after open (footer prefetch)");

        // Read from middle (should trigger new request)
        stream.seek(file.getSize() / 2);
        byte[] buffer = new byte[50];
        int bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "Should read bytes");

        long requestsAfterMiddle =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(2, requestsAfterMiddle, "Should have 2 requests after middle read");

        // Read from footer area (should use cache, no new request)
        stream.seek(file.getSize() - 50);
        bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "Should read bytes");

        long requestsAfterFooter =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(
            2, requestsAfterFooter, "Should still have 2 requests - footer read uses cache");
      }
    }
  }

  /**
   * Verifies that footer is fetched once on first stream open and reused when opening the same file
   * again, confirming footer caching works across multiple stream instances without additional GET
   * requests.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testFooterCacheReuseAcrossStreams(S3ClientKind s3ClientKind) throws IOException, InterruptedException {
    S3SeekableInputStreamConfiguration streamConfig = createFooterConfig(FOOTER_PREFETCH_SIZE);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, streamConfig)) {
      S3Object file = S3Object.VALID_SMALL_PARQUET;

      // Open first stream - footer is fetched and cached
      try (S3SeekableInputStream stream1 =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        long requestsAfterFirstOpen =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(
            1, requestsAfterFirstOpen, "Should have 1 request after first open (footer prefetch)");

        // Read from footer area (should use cached footer)
        stream1.seek(file.getSize() - 50);
        byte[] buffer = new byte[50];
        int bytesRead = stream1.read(buffer);
        assertTrue(bytesRead > 0, "Should read bytes");

        long requestsAfterFirstRead =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(
            1, requestsAfterFirstRead, "Should still have 1 request - footer read uses cache");
      }

      // Open second stream to same file - should reuse cached footer (no new GET)
      try (S3SeekableInputStream stream2 =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        long requestsAfterSecondOpen =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(
            1,
            requestsAfterSecondOpen,
            "Should still have 1 request - second open uses cached footer");

        // Read from footer area in second stream (should still use cached footer)
        stream2.seek(file.getSize() - 50);
        byte[] buffer = new byte[50];
        int bytesRead = stream2.read(buffer);
        assertTrue(bytesRead > 0, "Should read bytes");

        long requestsAfterSecondRead =
            reader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.GET_REQUEST_COUNT);
        assertEquals(
            1,
            requestsAfterSecondRead,
            "Should still have 1 request - both streams use cached footer");
      }
    }
  }

  /**
   * Verifies that when footer parsing fails due to corrupted schema, the library gracefully
   * degrades by setting column mapper to null while still allowing successful read operations
   * without throwing exceptions to users.
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testCorruptedSchemaFooter(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration streamConfig = createFooterConfig(FOOTER_PREFETCH_SIZE);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, streamConfig)) {
      S3Object file = S3Object.CORRUPTED_SCHEMA_PARQUET;

      try (S3SeekableInputStream stream =
          reader.createReadStream(file, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        S3URI s3URI = getS3URI(stream);
        Object columnMappers = getStore(reader).getColumnMappers(s3URI);
        assertNull(
            columnMappers, "Column mapper should be null when footer parsing throws exception");

        // Verify first read succeeds despite null column mapper
        stream.seek(100);
        byte[] buffer = new byte[50];
        int bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "First GET should succeed despite parsing failure");

        // Verify subsequent read also succeeds (library handles null mapper gracefully)
        stream.seek(200);
        bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0, "Subsequent GET should succeed with null column mapper");
      }
    }
  }

  /**
   * Verifies that parsing failure for one malformed file doesn't affect parsing of other files,
   * ensuring error isolation and that the shared cache remains functional for subsequent valid
   * files
   */
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testMalformedFileIsolation(S3ClientKind s3ClientKind) throws Exception {
    S3SeekableInputStreamConfiguration streamConfig = createParquetConfig(false);

    try (S3AALClientStreamReader reader = getStreamReader(s3ClientKind, streamConfig)) {

      // Open malformed file - parsing fails
      S3Object malformedFile = S3Object.CORRUPTED_SCHEMA_PARQUET;
      try (S3SeekableInputStream stream1 =
          reader.createReadStream(malformedFile, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        S3URI s3URI1 = getS3URI(stream1);
        Object columnMappers1 = getStore(reader).getColumnMappers(s3URI1);
        assertNull(columnMappers1, "Malformed file should have null column mapper");

        // Verify malformed file can still be read
        stream1.seek(100);
        byte[] buffer = new byte[50];
        int bytesRead = stream1.read(buffer);
        assertTrue(bytesRead > 0, "Malformed file should still be readable");
      }

      // Open valid file - parsing should succeed despite previous failure
      S3Object validFile = S3Object.VALID_SMALL_PARQUET;
      try (S3SeekableInputStream stream2 =
          reader.createReadStream(validFile, OpenStreamInformation.DEFAULT)) {
        Thread.sleep(ASYNC_TRACKING_WAIT_MS);

        S3URI s3URI2 = getS3URI(stream2);
        Object columnMappers2 = getStore(reader).getColumnMappers(s3URI2);
        assertNotNull(
            columnMappers2, "Valid file should have column mapper despite previous failure");

        // Verify valid file can be read
        stream2.seek(100);
        byte[] buffer = new byte[50];
        int bytesRead = stream2.read(buffer);
        assertTrue(bytesRead > 0, "Valid file should be readable");
      }
    }
  }
}
