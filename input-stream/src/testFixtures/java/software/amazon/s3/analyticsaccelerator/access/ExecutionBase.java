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
import java.util.Optional;
import lombok.NonNull;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * This class is a base for performance (JMH) and integration tests and contains common
 * functionality to create and drain streams, replay read patterns etc
 */
public abstract class ExecutionBase {
  /**
   * Returns the current {@link S3ExecutionContext}
   *
   * @return {@link S3ExecutionContext}
   */
  protected abstract S3ExecutionContext getS3ExecutionContext();

  /**
   * Creates an instance of {@link S3AsyncClientStreamReader} that uses {@link
   * software.amazon.awssdk.services.s3.S3AsyncClient} to read from S3
   *
   * @param s3ClientKind S3 Client kind
   * @return an instance of {@link S3AsyncClientStreamReader}
   */
  protected S3AsyncClientStreamReader createS3AsyncClientStreamReader(
      @NonNull S3ClientKind s3ClientKind) {
    return new S3AsyncClientStreamReader(
        s3ClientKind.getS3Client(this.getS3ExecutionContext()),
        this.getS3ExecutionContext().getConfiguration().getBaseUri(),
        this.getS3ExecutionContext().getConfiguration().getBufferSizeBytes());
  }

  /**
   * Creates an instance of {@link S3AALClientStreamReader} that uses AAL to read from S3
   *
   * @param s3ClientKind S3 Client kind
   * @param AALInputStreamConfigurationKind {@link S3SeekableInputStreamConfiguration} kind
   * @return an instance of {@link S3AALClientStreamReader}
   */
  protected S3AALClientStreamReader createS3AALClientStreamReader(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind) {
    return new S3AALClientStreamReader(
        s3ClientKind.getS3Client(this.getS3ExecutionContext()),
        AALInputStreamConfigurationKind.getValue(),
        this.getS3ExecutionContext().getConfiguration().getBaseUri(),
        this.getS3ExecutionContext().getConfiguration().getBufferSizeBytes());
  }

  /**
   * Creates an instance of {@link S3AALClientStreamReader} that uses AAL to read from S3
   *
   * @param s3ClientKind S3 Client kind
   * @param s3SeekableInputStreamConfiguration {@link S3SeekableInputStreamConfiguration}
   * @return an instance of {@link S3AALClientStreamReader}
   */
  protected S3AALClientStreamReader createS3AALClientStreamReader(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3SeekableInputStreamConfiguration s3SeekableInputStreamConfiguration) {
    return new S3AALClientStreamReader(
        s3ClientKind.getS3Client(this.getS3ExecutionContext()),
        s3SeekableInputStreamConfiguration,
        this.getS3ExecutionContext().getConfiguration().getBaseUri(),
        this.getS3ExecutionContext().getConfiguration().getBufferSizeBytes());
  }

  /**
   * Executes a pattern directly on an S3 Client.
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object {@link } S3 Object to run the pattern on
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @param openStreamInformation contains the open stream information
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternDirectly(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPattern streamReadPattern,
      Optional<Crc32CChecksum> checksum,
      OpenStreamInformation openStreamInformation)
      throws IOException {
    // Direct Read Pattern execution shouldn't read using the faulty client but it should use a
    // trusted client.
    s3ClientKind =
        s3ClientKind == S3ClientKind.FAULTY_S3_CLIENT
            ? S3ClientKind.SDK_V2_JAVA_ASYNC
            : s3ClientKind;
    try (S3AsyncClientStreamReader s3AsyncClientStreamReader =
        this.createS3AsyncClientStreamReader(s3ClientKind)) {
      s3AsyncClientStreamReader.readPattern(
          s3Object, streamReadPattern, checksum, openStreamInformation);
    }
  }

  /**
   * Executes a pattern on AAL
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object {@link S3Object} S3 Object to run the pattern on
   * @param AALInputStreamConfigurationKind DAT configuration
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @param openStreamInformation contains the open stream information
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternOnAAL(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPattern streamReadPattern,
      AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      Optional<Crc32CChecksum> checksum,
      OpenStreamInformation openStreamInformation)
      throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {
      executeReadPatternOnAAL(
          s3Object, s3AALClientStreamReader, streamReadPattern, checksum, openStreamInformation);
    }
  }

  /**
   * Executes a pattern on AAL
   *
   * @param s3Object {@link S3Object} S3 Object to run the pattern on
   * @param s3AALClientStreamReader DAT stream reader
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @param openStreamInformation contains the open stream information
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternOnAAL(
      S3Object s3Object,
      S3AALClientStreamReader s3AALClientStreamReader,
      StreamReadPattern streamReadPattern,
      Optional<Crc32CChecksum> checksum,
      OpenStreamInformation openStreamInformation)
      throws IOException {
    s3AALClientStreamReader.readPattern(
        s3Object, streamReadPattern, checksum, openStreamInformation);
  }
}
