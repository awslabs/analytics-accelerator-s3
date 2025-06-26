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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.s3.analyticsaccelerator.access.ChecksumAssertions.assertChecksums;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Stream;
import lombok.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * This tests concurrency and thread safety of teh shared state. While the DAT InputStream itself is
 * not thread-safe, the shared state that multiple streams access and manipulate should be.
 */
public class ConcurrencyCorrectnessTest extends IntegrationTestBase {
  private static final int CONCURRENCY_LEVEL = 3;
  private static final int CONCURRENCY_ITERATIONS = 2;

  @ParameterizedTest
  @MethodSource("sequentialReads")
  void testSequentialReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testAALReadConcurrency(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        configuration,
        CONCURRENCY_LEVEL,
        CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("skippingReads")
  void testSkippingReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testAALReadConcurrency(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        configuration,
        CONCURRENCY_LEVEL,
        CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("parquetReads")
  void testQuasiParquetReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testAALReadConcurrency(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        configuration,
        CONCURRENCY_LEVEL,
        CONCURRENCY_ITERATIONS);
  }

  /**
   * Tests concurrent access to AAL. This runs the specified pattern on multiple threads
   * concurrently
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param streamReadPatternKind stream read pattern to apply
   * @param AALInputStreamConfigurationKind configuration kind
   * @param concurrencyLevel concurrency level - how many threads are running at once
   * @param iterations how many iterations each thread does
   */
  protected void testAALReadConcurrency(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      int concurrencyLevel,
      int iterations)
      throws IOException, InterruptedException, ExecutionException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);
    // Read using the standard S3 async client. We do this once, to calculate the checksums
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        Optional.of(directChecksum),
        OpenStreamInformation.DEFAULT);

    // Create the s3DATClientStreamReader - that creates the shared state
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {
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
                      executeReadPatternOnAAL(
                          s3Object,
                          s3AALClientStreamReader,
                          streamReadPattern,
                          Optional.of(datChecksum),
                          OpenStreamInformation.DEFAULT);

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

  static Stream<Arguments> sequentialReads() {
    return argumentsFor(
        getS3ClientKinds(),
        Arrays.asList(S3Object.RANDOM_256MB, S3Object.CSV_20MB),
        sequentialPatterns(),
        concurrencyCorrectnessConfigurationKind());
  }

  static Stream<Arguments> skippingReads() {
    return argumentsFor(
        getS3ClientKinds(),
        Arrays.asList(S3Object.RANDOM_128MB, S3Object.CSV_20MB),
        Arrays.asList(StreamReadPatternKind.SKIPPING_FORWARD),
        concurrencyCorrectnessConfigurationKind());
  }

  static Stream<Arguments> parquetReads() {
    return argumentsFor(
        getS3ClientKinds(),
        Arrays.asList(S3Object.RANDOM_256MB),
        Arrays.asList(StreamReadPatternKind.QUASI_PARQUET_COLUMN_CHUNK),
        concurrencyCorrectnessConfigurationKind());
  }

  private static List<AALInputStreamConfigurationKind> concurrencyCorrectnessConfigurationKind() {
    return Arrays.asList(AALInputStreamConfigurationKind.CONCURRENCY_CORRECTNESS);
  }
}
