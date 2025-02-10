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
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests read stream behaviour with untrusted S3ClientKinds on multiple sizes and read patterns */
public class GrayFailureTest extends IntegrationTestBase {
  @ParameterizedTest
  @MethodSource("sequentialReads")
  void testSequentialReads(
      S3ClientKind clientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(clientKind, s3Object, streamReadPattern, configuration);
  }

  @ParameterizedTest
  @MethodSource("skippingReads")
  void testSkippingReads(
      S3ClientKind clientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(clientKind, s3Object, streamReadPattern, configuration);
  }

  @ParameterizedTest
  @MethodSource("parquetReads")
  void testQuasiParquetReads(
      S3ClientKind clientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(clientKind, s3Object, streamReadPattern, configuration);
  }

  static Stream<Arguments> sequentialReads() {
    return argumentsFor(
        S3ClientKind.faultyClients(),
        S3Object.smallObjects(),
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> skippingReads() {
    return argumentsFor(
        S3ClientKind.faultyClients(),
        S3Object.smallObjects(),
        skippingPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> parquetReads() {
    return argumentsFor(
        S3ClientKind.faultyClients(),
        S3Object.smallObjects(),
        parquetPatterns(),
        getS3SeekableInputStreamConfigurations());
  }
}
