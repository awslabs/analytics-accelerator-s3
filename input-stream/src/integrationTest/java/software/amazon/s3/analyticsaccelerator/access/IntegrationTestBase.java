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

import static software.amazon.s3.analyticsaccelerator.access.ChecksumAssertions.assertChecksums;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Base class for the integration tests */
public abstract class IntegrationTestBase extends ExecutionBase {
  @NonNull private final AtomicReference<S3ExecutionContext> s3ExecutionContext = new AtomicReference<>();

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);

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

  static List<S3ClientKind> clientKinds() {
    return getS3ClientKinds();
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
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param streamReadPattern stream read pattern to apply
   */
  protected void testAndCompareStreamReadPattern(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull S3AALClientStreamReader s3AALClientStreamReader)
      throws IOException {

    // Read using the standard S3 async client
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        Optional.of(directChecksum),
        OpenStreamInformation.DEFAULT);

    // Read using the AAL S3
    Crc32CChecksum aalChecksum = new Crc32CChecksum();
    executeReadPatternOnAAL(
        s3Object,
        s3AALClientStreamReader,
        streamReadPattern,
        Optional.of(aalChecksum),
        OpenStreamInformation.DEFAULT);

    // Assert checksums
    assertChecksums(directChecksum, aalChecksum);
  }

  /**
   * Applies the same read stream pattern to both S3 based and DAT based streams Calculates the
   * CRC32-C checksum on all bytes read and compares them at the end to verify the results are the
   * same
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param streamReadPatternKind stream read pattern to apply
   * @param AALInputStreamConfigurationKind configuration kind
   */
  protected void testAndCompareStreamReadPattern(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind)
      throws IOException {
    StreamReadPattern streamReadPattern = streamReadPatternKind.getStreamReadPattern(s3Object);

    // Read using the standard S3 async client
    Crc32CChecksum directChecksum = new Crc32CChecksum();
    executeReadPatternDirectly(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        Optional.of(directChecksum),
        OpenStreamInformation.DEFAULT);

    // Read using the AAL S3
    Crc32CChecksum aalChecksum = new Crc32CChecksum();
    executeReadPatternOnAAL(
        s3ClientKind,
        s3Object,
        streamReadPattern,
        AALInputStreamConfigurationKind,
        Optional.of(aalChecksum),
        OpenStreamInformation.DEFAULT);

    // Assert checksums
    assertChecksums(directChecksum, aalChecksum);
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
  static List<AALInputStreamConfigurationKind> getS3SeekableInputStreamConfigurations() {
    return Arrays.asList(AALInputStreamConfigurationKind.DEFAULT);
  }

  /**
   * S3 Client kinds
   *
   * @return list of S3ClientKind to use for testing.
   */
  static List<S3ClientKind> getS3ClientKinds() {
    return S3ClientKind.trustedClients();
  }

  /**
   * Generates the cartesian set of the supplies argument lists
   *
   * @param clients clients
   * @param objects objects
   * @param readPatterns read patterns
   * @param configurations configurations
   * @return A {@link Stream} of {@link Arguments} with the cartesian set
   */
  static Stream<Arguments> argumentsFor(
      List<S3ClientKind> clients,
      List<S3Object> objects,
      List<StreamReadPatternKind> readPatterns,
      List<AALInputStreamConfigurationKind> configurations) {
    ArrayList<Arguments> results = new ArrayList<>();
    for (S3ClientKind client : clients) {
      for (S3Object object : objects) {
        for (StreamReadPatternKind readPattern : readPatterns) {
          for (AALInputStreamConfigurationKind configuration : configurations) {
            results.add(Arguments.of(client, object, readPattern, configuration));
          }
        }
      }
    }

    return results.stream();
  }
}
