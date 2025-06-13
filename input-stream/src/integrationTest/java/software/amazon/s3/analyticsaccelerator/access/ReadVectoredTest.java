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
import lombok.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

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

  @ParameterizedTest
  @MethodSource("vectoredReads")
  void testVectoredReadsInSingleBlock(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {

    testReadVectoredInSingleBlock(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocate);

    testReadVectoredInSingleBlock(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocateDirect);
  }

  @ParameterizedTest
  @MethodSource("vectoredReads")
  void testVectoredReadsForSequentialRanges(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {

    testReadVectoredForSequentialRanges(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocate);

    testReadVectoredForSequentialRanges(
        s3ClientKind, s3Object, streamReadPattern, configuration, ByteBuffer::allocateDirect);
  }

  @Test
  void testEmptyRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      assertEquals(0, objectRanges.size());

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          0);
    }
  }

  @Test
  void testEoFRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();

      objectRanges.add(
          new ObjectRange(new CompletableFuture<>(), SizeConstants.ONE_GB_IN_BYTES + 1, 500));

      assertThrows(
          EOFException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          0);
    }
  }

  @Test
  void testNullRange() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(null);

      assertThrows(
          NullPointerException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          0);
    }
  }

  @Test
  void testOverlappingRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 4000, 500));
      // overlaps with the first range
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 900, 500));

      assertThrows(
          IllegalArgumentException.class,
          () ->
              s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED));

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          0);
    }
  }

  @Test
  void testSomeRangesFail() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.FAULTY_S3_CLIENT, AALInputStreamConfigurationKind.NO_RETRY)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 100 * ONE_MB, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 500 * ONE_MB, 500));

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      assertThrows(CompletionException.class, () -> objectRanges.get(0).getByteBuffer().join());
      assertDoesNotThrow(() -> objectRanges.get(1).getByteBuffer().join());
      assertDoesNotThrow(() -> objectRanges.get(2).getByteBuffer().join());

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          3);
    }
  }

  static Stream<Arguments> vectoredReads() {
    List<S3Object> readVectoredObjects = new ArrayList<>();
    readVectoredObjects.add(S3Object.RANDOM_1GB);

    List<S3ClientKind> s3ClientKinds = new ArrayList<>();
    s3ClientKinds.add(S3ClientKind.SDK_V2_JAVA_ASYNC);

    return argumentsFor(
        s3ClientKinds,
        readVectoredObjects,
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  /**
   * This test verifies that the data in the buffers is the same when a file is read through
   * readVectored() vs stream.read(buf[], off, len).
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param streamReadPatternKind stream read pattern to apply
   * @param AALInputStreamConfigurationKind configuration kind
   * @param allocate method to allocate the buffer, can be direct or non-direct
   * @throws IOException on any IOException
   */
  protected void testReadVectored(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 50, ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 2 * ONE_MB, 800));

      // a range that should be within a single block
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 200 * ONE_MB, 8 * ONE_MB));

      // a range that spans multiple ranges
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 260 * ONE_MB, 24 * ONE_MB));

      s3SeekableInputStream.readVectored(
          objectRanges,
          allocate,
          (buffer) -> {
            LOG.debug("Release buffer of length {}: {}", buffer.limit(), buffer);
          });

      // Join on the buffers to ensure the vectored reads happen as they happen in an async thread
      // pool.
      for (ObjectRange objectRange : objectRanges) {
        objectRange.getByteBuffer().join();
      }

      // Range [50MB - 51MB, 2MB - 2.8MB] will make 2 GET requests
      // Range [200MB - 208MB] will make a single GET as it is an 8MB block.
      // Range [260MB - 284MB] will make 3 GET requests
      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          6);

      verifyStreamContents(objectRanges, s3AALClientStreamReader, s3Object);
    }
  }

  protected void testReadVectoredInSingleBlock(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 500, 800));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 2000, 200));

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      // Join on the buffers to ensure the vectored reads happen as they happen in an async thread
      // pool.
      for (ObjectRange objectRange : objectRanges) {
        objectRange.getByteBuffer().join();
      }

      assertEquals(
          1,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }

  protected void testReadVectoredForSequentialRanges(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull StreamReadPatternKind streamReadPatternKind,
      @NonNull AALInputStreamConfigurationKind AALInputStreamConfigurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 2 * ONE_MB, 8 * ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 10 * ONE_MB, ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 12 * ONE_MB, 5 * ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 17 * ONE_MB, 4 * ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 21 * ONE_MB, 8 * ONE_MB));

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);

      // Join on the buffers to ensure the vectored reads happen as they happen in an async thread
      // pool.
      for (ObjectRange objectRange : objectRanges) {
        objectRange.getByteBuffer().join();
      }

      assertEquals(
          5,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }

  private void verifyStreamContents(
      List<ObjectRange> objectRanges,
      S3AALClientStreamReader s3AALClientStreamReader,
      S3Object s3Object)
      throws IOException {
    for (ObjectRange objectRange : objectRanges) {
      ByteBuffer byteBuffer = objectRange.getByteBuffer().join();

      S3SeekableInputStream verificationStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      verificationStream.seek(objectRange.getOffset());
      byte[] buffer = new byte[objectRange.getLength()];
      int readBytes = verificationStream.read(buffer, 0, buffer.length);

      assertEquals(readBytes, buffer.length);
      verifyBufferContentsEqual(byteBuffer, buffer);
    }
  }

  /**
   * Verify the contents of two buffers are equal
   *
   * @param buffer ByteBuffer to verify contents for
   * @param expected expected contents in byte buffer
   */
  private void verifyBufferContentsEqual(ByteBuffer buffer, byte[] expected) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(buffer.get(i), expected[i]);
    }
  }
}
