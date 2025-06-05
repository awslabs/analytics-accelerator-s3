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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;

public class ReadVectoredTest extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ReadVectoredTest.class);
  private static final Consumer<ByteBuffer> LOG_BYTE_BUFFER_RELEASED =
      (buffer) -> {
        LOG.debug("Release buffer of length {}: {}", buffer.limit(), buffer);
      };

  @ParameterizedTest
  @MethodSource("vectoredReads")
  void testVectoredReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    // Run with non-direct buffers
    testReadVectored(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocate);
    // Run with direct buffers
    testReadVectored(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocateDirect);
  }

  @Test
  void testEmptyRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.DEFAULT)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      List<ObjectRange> objectRanges = new ArrayList<>();

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      assertEquals(0, objectRanges.size());
    }
  }

  @Test
  void testEoFRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.DEFAULT)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      List<ObjectRange> objectRanges = new ArrayList<>();

      objectRanges.add(
          new ObjectRange(new CompletableFuture<>(), SizeConstants.ONE_GB_IN_BYTES + 1, 500));

      assertThrows(
          EOFException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));
    }
  }

  @Test
  void testNullRange() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.DEFAULT)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(null);

      assertThrows(
          NullPointerException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));
    }
  }

  @Test
  void testNullRangeList() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.DEFAULT)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      assertThrows(
          NullPointerException.class,
          () -> s3SeekableInputStream.readVectored(null, allocate, LOG_BYTE_BUFFER_RELEASED));
    }
  }

  @Test
  void testOverlappingRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.DEFAULT)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 4000, 500));
      // overlaps with the first range
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 900, 500));

      assertThrows(
          IllegalArgumentException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));
    }
  }

  @Test
  void testSomeRangesFail() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.FAULTY_S3_CLIENT, AALInputStreamConfigurationKind.NO_RETRY)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1GB);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 100 * ONE_MB, 500));
      // overlaps with the first range
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 500 * ONE_MB, 500));

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      // First range should throw as the fault client will block the first GET to the object.
      // Subsequent ranges
      // will complete.
      assertThrows(CompletionException.class, () -> objectRanges.get(0).getByteBuffer().join());
      assertDoesNotThrow(() -> objectRanges.get(1).getByteBuffer().join());
      assertDoesNotThrow(() -> objectRanges.get(2).getByteBuffer().join());
    }
  }

  static Stream<Arguments> vectoredReads() {
    List<S3Object> readVectoredObjects = new ArrayList<>();
    readVectoredObjects.add(S3Object.RANDOM_1GB);
    readVectoredObjects.add(S3Object.CSV_20MB);

    return argumentsFor(
        getS3ClientKinds(),
        readVectoredObjects,
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }
}
