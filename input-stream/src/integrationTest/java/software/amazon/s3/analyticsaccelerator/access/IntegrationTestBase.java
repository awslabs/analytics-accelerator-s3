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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.s3.analyticsaccelerator.access.ChecksumAssertions.assertChecksums;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Base class for the integration tests */
public abstract class IntegrationTestBase extends ExecutionBase {
  @NonNull private final AtomicReference<S3ExecutionContext> s3ExecutionContext = new AtomicReference<>();

  SecureRandom random = new SecureRandom();

  @BeforeEach
  void setUp() {
    // Set up the execution context based on the environment
    this.s3ExecutionContext.set(new S3ExecutionContext(S3ExecutionConfiguration.fromEnvironment()));
  }

  @AfterEach
  void tearDown() throws IOException {
    this.s3ExecutionContext.getAndSet(null).close();
  }

  /**
   * Client kind to use by tests. In most cases this should be CRT
   *
   * @return {@link S3ClientKind}
   */
  protected S3ClientKind getClientKind() {
    return S3ClientKind.SDK_V2_CRT_ASYNC;
  }

  /**
   * Returns the currently active execution context
   *
   * @return currently active execution context
   */
  protected S3ExecutionContext getS3ExecutionContext() {
    return this.s3ExecutionContext.get();
  }

  /**
   * Applies the same read stream pattern to both S3 based and DAT based streams Calculates the
   * CRC32-C checksum on all bytes read and compares them at the end to verify the results are the
   * same
   *
   * @param s3Object S3 object to read
   * @param streamReadPatternKind stream read pattern to apply
   * @param DATInputStreamConfigurationKind configuration kind
   */
  protected void testAndCompareStreamReadPattern(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull DATInputStreamConfigurationKind DATInputStreamConfigurationKind)
      throws IOException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);

    // Read using the standard S3 async client
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(s3Object, streamReadPattern, Optional.of(directChecksum));

    // Read using the DAT S3
    Crc32CChecksum datChecksum = new Crc32CChecksum();
    executeReadPatternOnDAT(
        s3Object, streamReadPattern, DATInputStreamConfigurationKind, Optional.of(datChecksum));

    // Assert checksums
    assertChecksums(directChecksum, datChecksum);
  }

  /**
   * Checks to make sure we throw an error and fail the stream while reading a stream and the etag
   * changes during the read.
   *
   * @param s3Object S3 object to read
   * @param DATInputStreamConfigurationKind configuration kind
   */
  protected void testChangingEtagMidStream(
      @NonNull S3Object s3Object,
      @NonNull DATInputStreamConfigurationKind DATInputStreamConfigurationKind)
      throws IOException {
    int bufferSize = (int) s3Object.getSize();
    byte[] buffer = new byte[bufferSize];

    // Create the s3DATClientStreamReader - that creates the shared state
    try (S3DATClientStreamReader s3DATClientStreamReader =
        this.createS3DATClientStreamReader(getClientKind(), DATInputStreamConfigurationKind)) {

      S3URI s3URI =
          s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
      S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();
      S3SeekableInputStream stream = s3DATClientStreamReader.createReadStream(s3Object);

      int readAheadBytes =
          (int)
              DATInputStreamConfigurationKind.getValue()
                  .getPhysicalIOConfiguration()
                  .getReadAheadBytes();

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
      Exception ex =
          assertThrows(Exception.class, () -> readAndAssert(stream, buffer, 200, readAheadBytes));
      S3Exception s3Exception =
          assertInstanceOf(S3Exception.class, ex.getCause(), "Cause should be S3Exception");
      assertEquals(412, s3Exception.statusCode(), "Expected Precondition Failed (412) status code");
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
  private void readAndAssert(InputStream stream, byte[] buffer, int offset, int len)
      throws IOException {
    int readBytes = stream.read(buffer, offset, len);
    assertEquals(readBytes, len);
  }

  /**
   * Tests to make sure if we have read our whole object we pass and return our cached data even if
   * the etag has changed after the read is complete
   *
   * @param s3Object S3 object to read
   * @param DATInputStreamConfigurationKind configuration kind
   * @throws IOException
   */
  protected void testChangingEtagAfterStreamPassesAndReturnsCachedObject(
      @NonNull S3Object s3Object,
      @NonNull DATInputStreamConfigurationKind DATInputStreamConfigurationKind)
      throws IOException {
    int bufferSize = (int) s3Object.getSize();
    // Create the s3DATClientStreamReader - that creates the shared state
    try (S3DATClientStreamReader s3DATClientStreamReader =
        this.createS3DATClientStreamReader(getClientKind(), DATInputStreamConfigurationKind)) {
      S3SeekableInputStream stream = s3DATClientStreamReader.createReadStream(s3Object);
      Crc32CChecksum datChecksum = calculateCRC32C(stream, bufferSize);

      S3URI s3URI =
          s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
      S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();

      // Change the file
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(generateRandomBytes(bufferSize)))
          .join();

      S3SeekableInputStream cacheStream = s3DATClientStreamReader.createReadStream(s3Object);
      Crc32CChecksum cachedChecksum = calculateCRC32C(cacheStream, bufferSize);

      // Assert checksums
      assertChecksums(datChecksum, cachedChecksum);
    }
  }

  /**
   * Generates a byte array filled with random bytes.
   *
   * @param bufferSize how big our byte array needs to be
   * @return a populated byte array
   */
  protected byte[] generateRandomBytes(int bufferSize) {
    byte[] data = new byte[bufferSize];
    random.nextBytes(data);
    return data;
  }

  /**
   * Used to calculate a checksum based off our input stream
   *
   * @param input the input stream to generate the checksum for
   * @param bufferSize how big the input stream is
   * @return the calculated Crc32CChecksum
   * @throws IOException
   */
  protected static Crc32CChecksum calculateCRC32C(InputStream input, int bufferSize)
      throws IOException {
    Crc32CChecksum checksum = new Crc32CChecksum();
    byte[] buffer = new byte[bufferSize];
    int bytesRead;

    while ((bytesRead = input.read(buffer)) != -1) {
      checksum.update(buffer, 0, bytesRead);
    }

    return checksum;
  }

  /**
   * Tests concurrent access to DAT. This runs the specified pattern on multiple threads
   * concurrently
   *
   * @param s3Object S3 object to read
   * @param streamReadPatternKind stream read pattern to apply
   * @param DATInputStreamConfigurationKind configuration kind
   * @param concurrencyLevel concurrency level - how many threads are running at once
   * @param iterations how many iterations each thread does
   */
  protected void testDATReadConcurrency(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull DATInputStreamConfigurationKind DATInputStreamConfigurationKind,
      int concurrencyLevel,
      int iterations)
      throws IOException, InterruptedException, ExecutionException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);
    // Read using the standard S3 async client. We do this once, to calculate the checksums
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(s3Object, streamReadPattern, Optional.of(directChecksum));

    // Create the s3DATClientStreamReader - that creates the shared state
    try (S3DATClientStreamReader s3DATClientStreamReader =
        this.createS3DATClientStreamReader(getClientKind(), DATInputStreamConfigurationKind)) {
      // Create the thread pool
      ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
      Future<?>[] resultFutures = new Future<?>[concurrencyLevel];

      for (int i = 0; i < concurrencyLevel; i++) {
        resultFutures[i] =
            executorService.submit(
                () -> {
                  try {
                    // Run multiple iterations
                    for (int j = 0; j < iterations; j++) {
                      // Run DAT on the thread
                      // This will create a new stream every time, but all streams will share state
                      Crc32CChecksum datChecksum = new Crc32CChecksum();
                      executeReadPatternOnDAT(
                          s3Object,
                          s3DATClientStreamReader,
                          streamReadPattern,
                          Optional.of(datChecksum));

                      // Assert checksums
                      assertChecksums(directChecksum, datChecksum);
                    }
                  } catch (Throwable t) {
                    throw new RuntimeException(t);
                  }
                });
      }
      // wait for each future to propagate errors
      for (int i = 0; i < concurrencyLevel; i++) {
        // This should throw an exception, if a thread threw one, including assertions
        resultFutures[i].get();
      }
      // Shutdown. Wait for termination indefinitely - we expect it to always complete
      executorService.shutdown();
      assertTrue(executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS));
    }
  }

  /**
   * Stream read patterns to test on
   *
   * @return stream read patterns
   */
  static List<StreamReadPatternKind> allPatterns() {
    return Arrays.asList(StreamReadPatternKind.values());
  }

  /**
   * Sequential patterns
   *
   * @return sequential patterns
   */
  static List<StreamReadPatternKind> sequentialPatterns() {
    return Arrays.asList(StreamReadPatternKind.SEQUENTIAL);
  }

  /**
   * Skipping patterns
   *
   * @return skipping patterns
   */
  static List<StreamReadPatternKind> skippingPatterns() {
    return Arrays.asList(
        StreamReadPatternKind.SKIPPING_BACKWARD, StreamReadPatternKind.SKIPPING_FORWARD);
  }

  /**
   * Parquet patterns
   *
   * @return parquet patterns
   */
  static List<StreamReadPatternKind> parquetPatterns() {
    return Arrays.asList(
        StreamReadPatternKind.QUASI_PARQUET_ROW_GROUP,
        StreamReadPatternKind.QUASI_PARQUET_COLUMN_CHUNK);
  }

  /**
   * Configuration kinds to create the {@link S3SeekableInputStream} with.
   *
   * @return configuration kind
   */
  static List<DATInputStreamConfigurationKind> getS3SeekableInputStreamConfigurations() {
    return Arrays.asList(DATInputStreamConfigurationKind.values());
  }

  /**
   * Generates the cartesian set of the supplies argument lists
   *
   * @param objects objects
   * @param readPatterns read patterns
   * @param configurations configurations
   * @return A {@link Stream} of {@link Arguments} with the cartesian set
   */
  static Stream<Arguments> argumentsFor(
      List<S3Object> objects,
      List<StreamReadPatternKind> readPatterns,
      List<DATInputStreamConfigurationKind> configurations) {
    ArrayList<Arguments> results = new ArrayList<>();
    for (S3Object object : objects) {
      for (StreamReadPatternKind readPattern : readPatterns) {
        for (DATInputStreamConfigurationKind configuration : configurations) {
          results.add(Arguments.of(object, readPattern, configuration));
        }
      }
    }

    return results.stream();
  }
}
