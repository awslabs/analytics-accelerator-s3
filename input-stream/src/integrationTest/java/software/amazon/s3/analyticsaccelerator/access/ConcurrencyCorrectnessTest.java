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
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("skippingReads")
  void testSkippingReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("parquetReads")
  void testQuasiParquetReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("etagTests")
  void testChangingEtagFailsStream(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException {
    testChangingEtagMidStream(s3Object, configuration);
  }

  @ParameterizedTest
  @MethodSource("etagTests")
  void testChangingEtagReturnsCachedObject(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException {
    testChangingEtagAfterStreamPassesAndReturnsCachedObject(s3Object, configuration);
  }

  static Stream<Arguments> sequentialReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> skippingReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        skippingPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> parquetReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        parquetPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> etagTests() {
    return argumentsFor(
        S3Object.smallObjects(), parquetPatterns(), getS3SeekableInputStreamConfigurations());
  }
}
