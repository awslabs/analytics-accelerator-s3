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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static software.amazon.s3.analyticsaccelerator.access.ChecksumAssertions.assertChecksums;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.request.EncryptionSecrets;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

public class SSECEncryptionTest extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SSECEncryptionTest.class);

  private static final String CUSTOMER_KEY = System.getenv("CUSTOMER_KEY");

  private void checkPrerequisites() {
    String skipMessage = "Skipping tests: CUSTOMER_KEY environment variable is not set";
    if (CUSTOMER_KEY == null || CUSTOMER_KEY.trim().isEmpty()) {
      LOG.info(skipMessage);
    }
    assumeTrue(CUSTOMER_KEY != null && !CUSTOMER_KEY.trim().isEmpty(), skipMessage);
  }

  @ParameterizedTest
  @MethodSource("encryptedSequentialReads")
  void testEncryptedSequentialReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    checkPrerequisites();
    testReadPatternUsingSSECEncryption(
        s3ClientKind, s3Object, streamReadPattern, configuration, CUSTOMER_KEY);
  }

  @ParameterizedTest
  @MethodSource("encryptedParquetReads")
  void testEncryptedParquetReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    checkPrerequisites();
    testReadPatternUsingSSECEncryption(
        s3ClientKind, s3Object, streamReadPattern, configuration, CUSTOMER_KEY);
  }

  @ParameterizedTest
  @MethodSource("encryptedReadsWithWrongKey")
  void testEncryptedReadsWithWrongKey(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration) {

    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              testReadPatternUsingWrongKeyOrEmptyKey(
                  s3ClientKind, s3Object, streamReadPattern, configuration, "wrongkey");
            });

    Throwable cause = exception.getCause();
    assertTrue(cause instanceof S3Exception);
    S3Exception s3Exception = (S3Exception) cause;
    assertEquals(403, s3Exception.statusCode());
  }

  @ParameterizedTest
  @MethodSource("encryptedReadsWithWrongKey")
  void testEncryptedReadsWithEmptyKey(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration) {

    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              testReadPatternUsingWrongKeyOrEmptyKey(
                  s3ClientKind, s3Object, streamReadPattern, configuration, null);
            });

    Throwable cause = exception.getCause();
    assertTrue(cause instanceof S3Exception);
    S3Exception s3Exception = (S3Exception) cause;
    assertEquals(400, s3Exception.statusCode());
  }

  protected void testReadPatternUsingSSECEncryption(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      String customerKey)
      throws IOException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);
    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder()
            .encryptionSecrets(
                EncryptionSecrets.builder().sseCustomerKey(Optional.of(customerKey)).build())
            .build();

    // Read using the standard S3 async client
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        Optional.of(directChecksum),
        openStreamInformation);

    // Read using the AAL S3
    Crc32CChecksum aalChecksum = new Crc32CChecksum();
    executeReadPatternOnAAL(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        AALInputStreamConfigurationKind,
        Optional.of(aalChecksum),
        openStreamInformation);

    // Assert checksums
    assertChecksums(directChecksum, aalChecksum);
  }

  protected void testReadPatternUsingWrongKeyOrEmptyKey(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      String customerKey)
      throws IOException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);

    OpenStreamInformation openStreamInformation =
        customerKey == null
            ? OpenStreamInformation.DEFAULT
            : OpenStreamInformation.builder()
                .encryptionSecrets(
                    EncryptionSecrets.builder().sseCustomerKey(Optional.of(customerKey)).build())
                .build();

    // Read using the AAL S3
    Crc32CChecksum aalChecksum = new Crc32CChecksum();
    executeReadPatternOnAAL(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        AALInputStreamConfigurationKind,
        Optional.of(aalChecksum),
        openStreamInformation);
  }

  static Stream<Arguments> encryptedSequentialReads() {
    List<S3Object> readEncryptedObjects = new ArrayList<>();
    readEncryptedObjects.add(S3Object.RANDOM_SSEC_ENCRYPTED_SEQUENTIAL_1MB);

    return argumentsFor(
        getS3ClientKinds(),
        readEncryptedObjects,
        sequentialPatterns(),
        readCorrectnessConfigurationKind());
  }

  static Stream<Arguments> encryptedParquetReads() {
    List<S3Object> readEncryptedObjects = new ArrayList<>();
    readEncryptedObjects.add(S3Object.RANDOM_SSEC_ENCRYPTED_PARQUET_1MB);
    readEncryptedObjects.add(S3Object.RANDOM_SSEC_ENCRYPTED_PARQUET_10MB);

    return argumentsFor(
        getS3ClientKinds(),
        readEncryptedObjects,
        parquetPatterns(),
        readCorrectnessConfigurationKind());
  }

  static Stream<Arguments> encryptedReadsWithWrongKey() {
    List<S3Object> readEncryptedObjects = new ArrayList<>();
    readEncryptedObjects.add(S3Object.RANDOM_SSEC_ENCRYPTED_PARQUET_10MB);

    return argumentsFor(
        getS3ClientKinds(),
        readEncryptedObjects,
        sequentialPatterns(),
        readCorrectnessConfigurationKind());
  }

  private static List<AALInputStreamConfigurationKind> readCorrectnessConfigurationKind() {
    return Arrays.asList(AALInputStreamConfigurationKind.READ_CORRECTNESS);
  }
}
