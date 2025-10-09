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
import java.util.concurrent.*;
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
      IntFunction<ByteBuffer> allocate,
      AALInputStreamConfigurationKind configurationKind)
      throws IOException {
    testReadVectored(s3ClientKind, S3Object.RANDOM_1GB, configurationKind, allocate);
  }

  @ParameterizedTest
  @MethodSource("vectoredReads")
  void testVectoredReadsInSingleBlock(
      S3ClientKind s3ClientKind,
      IntFunction<ByteBuffer> allocate,
      AALInputStreamConfigurationKind configurationKind)
      throws IOException {
    testReadVectoredInSingleBlock(s3ClientKind, S3Object.RANDOM_1GB, configurationKind, allocate);
  }

  @ParameterizedTest
  @MethodSource("vectoredReads")
  void testVectoredReadsForSequentialRanges(
      S3ClientKind s3ClientKind,
      IntFunction<ByteBuffer> allocate,
      AALInputStreamConfigurationKind configurationKind)
      throws IOException {
    testReadVectoredForSequentialRanges(
        s3ClientKind, S3Object.RANDOM_1GB, configurationKind, allocate);
  }

  @Test
  void testEmptyRanges() throws IOException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

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
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

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
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

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
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

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
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 100 * ONE_MB, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 500 * ONE_MB, 500));

      s3SeekableInputStream.readVectored(objectRanges, allocate, LOG_BYTE_BUFFER_RELEASED);
      try {
        // One of the joins must throw but we dont know which one due to asynchrony
        objectRanges.get(0).getByteBuffer().join();
        objectRanges.get(1).getByteBuffer().join();
        objectRanges.get(2).getByteBuffer().join();
      } catch (Exception e) {
        assertInstanceOf(CompletionException.class, e);
      }
      assertEquals(
          3,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }

  @Test
  void testTwoConcurrentStreams() throws IOException, ExecutionException, InterruptedException {
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.SDK_V2_JAVA_ASYNC, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      ExecutorService threadPool = Executors.newFixedThreadPool(5);

      // Do three readVectored() concurrently
      Future<?> x = threadPool.submit(() -> performReadVectored(s3AALClientStreamReader));
      Future<?> y = threadPool.submit(() -> performReadVectored(s3AALClientStreamReader));
      Future<?> z = threadPool.submit(() -> performReadVectored(s3AALClientStreamReader));

      x.get();
      y.get();
      z.get();

      assertEquals(
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT),
          3);
    }
  }

  private void performReadVectored(S3AALClientStreamReader s3AALClientStreamReader) {

    try {
      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(
              S3Object.RANDOM_1GB, OpenStreamInformation.ofDefaults());

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 700, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 100 * ONE_MB, 500));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 500 * ONE_MB, 500));

      s3SeekableInputStream.readVectored(
          objectRanges, ByteBuffer::allocate, LOG_BYTE_BUFFER_RELEASED);

      for (ObjectRange objectRange : objectRanges) {
        objectRange.getByteBuffer().join();
      }
    } catch (IOException e) {
      // Do nothing
    }
  }

  static Stream<Arguments> vectoredReads() {
    List<Arguments> testCases = new ArrayList<>();

    List<S3ClientKind> s3ClientKinds = new ArrayList<>();
    s3ClientKinds.add(S3ClientKind.SDK_V2_JAVA_ASYNC);
    s3ClientKinds.add(S3ClientKind.SDK_V2_JAVA_SYNC);

    List<AALInputStreamConfigurationKind> configurationKinds = new ArrayList<>();
    configurationKinds.add(AALInputStreamConfigurationKind.DEFAULT);
    configurationKinds.add(AALInputStreamConfigurationKind.NO_REQUEST_COALESCING);

    List<IntFunction<ByteBuffer>> allocate = new ArrayList<>();
    allocate.add(ByteBuffer::allocate);
    allocate.add(ByteBuffer::allocateDirect);

    for (S3ClientKind s3ClientKind : s3ClientKinds) {
      for (IntFunction<ByteBuffer> allocator : allocate) {
        for (AALInputStreamConfigurationKind configurationKind : configurationKinds)
          testCases.add(Arguments.of(s3ClientKind, allocator, configurationKind));
      }
    }

    return testCases.stream();
  }

  /**
   * This test verifies that the data in the buffers is the same when a file is read through
   * readVectored() vs stream.read(buf[], off, len).
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param configurationKind configuration kind
   * @param allocate method to allocate the buffer, can be direct or non-direct
   * @throws IOException on any IOException
   */
  protected void testReadVectored(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull S3Object s3Object,
      @NonNull AALInputStreamConfigurationKind configurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        getStreamReader(s3ClientKind, configurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.ofDefaults());

      List<ObjectRange> objectRanges = new ArrayList<>();
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 50, ONE_MB));
      objectRanges.add(new ObjectRange(new CompletableFuture<>(), 3 * ONE_MB, 800));

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

      // Range [50 - 1MB, 3MB - 3.8MB] will make 2 GET requests -- will not be coalesced with 1MB
      // tolerance
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
      @NonNull AALInputStreamConfigurationKind configurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.getStreamReader(s3ClientKind, configurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.ofDefaults());

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
      @NonNull AALInputStreamConfigurationKind configurationKind,
      @NonNull IntFunction<ByteBuffer> allocate)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.getStreamReader(s3ClientKind, configurationKind)) {

      S3SeekableInputStream s3SeekableInputStream =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.ofDefaults());

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

      int expectedRequests =
          (configurationKind == AALInputStreamConfigurationKind.NO_REQUEST_COALESCING)
              ? objectRanges.size()
              : 3;

      assertEquals(
          expectedRequests,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
      verifyStreamContents(objectRanges, s3AALClientStreamReader, s3Object);
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
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.ofDefaults());
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
