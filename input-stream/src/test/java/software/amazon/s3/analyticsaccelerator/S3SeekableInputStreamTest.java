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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.DefaultRequestCallbackImpl;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamTest extends S3SeekableInputStreamTestBase {
  private static final S3URI TEST_URI = S3URI.of("bucket", "key");

  @Test
  void testConstructor() {
    S3SeekableInputStream inputStream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT);
    assertNotNull(inputStream);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new S3SeekableInputStream(null, mock(LogicalIO.class), TestTelemetry.DEFAULT));

    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new S3SeekableInputStream(TEST_URI, mock(LogicalIO.class), null));

    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new S3SeekableInputStream(TEST_URI, null, TestTelemetry.DEFAULT));
  }

  @Test
  void testInitialGetPosition() throws IOException {
    // Given
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT)) {

      // When: nothing
      // Then: stream position is at 0
      assertEquals(0, stream.getPos());
    }
  }

  @Test
  void testReadAdvancesPosition() throws IOException {
    // Given
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT)) {

      // When: read() is called
      stream.read();

      // Then: position is advanced
      assertEquals(1, stream.getPos());
    }
  }

  @Test
  void testSeek() throws IOException {
    // Given
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT)) {

      // When
      stream.seek(13);

      // Then
      assertEquals(13, stream.getPos());
    }
  }

  @Test
  void testFullRead() throws IOException {
    // Given
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT)) {

      // When: all data is requested
      String dataReadOut = IoUtils.toUtf8String(stream);

      // Then: data read out is the same as data under stream
      assertEquals(TEST_DATA, dataReadOut);
    }
  }

  @Test
  void testSeekToVeryEnd() throws IOException {
    // Given
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT)) {

      // When: we seek to the last byte
      stream.seek(TEST_DATA.length() - 1);

      // Then: first read returns the last byte and the next read returns -1
      assertEquals(48, stream.read());
      assertEquals(-1, stream.read());
    }
  }

  @Test
  void testSeekAfterEnd() {
    S3SeekableInputStream stream = getTestStream();
    assertDoesNotThrow(() -> stream.seek(Long.MAX_VALUE));
    assertDoesNotThrow(() -> stream.seek(TEST_DATA.length()));
    assertEquals(TEST_DATA.length(), stream.getPos());
    assertDoesNotThrow(() -> stream.seek(TEST_DATA.length() + 10));
    assertEquals(TEST_DATA.length() + 10, stream.getPos());
  }

  @Test
  void testReadOnEmptyObject() throws IOException {
    // Given
    try (S3SeekableInputStream stream = getTestStreamWithContent("", TEST_OBJECT)) {

      // When: we read a byte from the empty object
      int readByte = stream.read();

      // Then: read returns -1
      assertEquals(-1, readByte);
    }
  }

  @Test
  void testInvalidSeek() throws IOException {
    // Given
    try (S3SeekableInputStream stream = getTestStream()) {
      // When: seek is to an invalid position then exception is thrown
      assertThrows(Exception.class, () -> stream.seek(-1));
      assertThrows(Exception.class, () -> stream.seek(Long.MIN_VALUE));
    }
  }

  @Test
  void testLogicalIOGetsClosed() throws IOException {
    // Given
    LogicalIO logicalIO = mock(LogicalIO.class);
    S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, logicalIO, TestTelemetry.DEFAULT);
    stream.close();
    // Then
    verify(logicalIO, times(1)).close();
  }

  @Test
  void testReadWithBufferNulls() throws IOException {
    try (S3SeekableInputStream stream = getTestStream()) {
      assertThrows(
          NullPointerException.class,
          () -> {
            if (stream.read(null, 1, 1) <= 0) {
              throw new IllegalStateException();
            }
          });
    }
  }

  @Test
  void testNullRangeList() throws IOException {
    try (S3SeekableInputStream stream = getTestStream()) {
      assertThrows(
          NullPointerException.class,
          () -> stream.readVectored(null, ByteBuffer::allocate, (buffer) -> {}));

      assertThrows(
          NullPointerException.class,
          () -> stream.readVectored(new ArrayList<>(), null, (buffer) -> {}));
    }
  }

  @Test
  void testReadWithBuffer() throws IOException {
    try (S3SeekableInputStream stream = getTestStream()) {

      byte[] buffer = new byte[TEST_DATA.length()];
      assertEquals(20, stream.read(buffer, 0, TEST_DATA.length()));
      assertTrue(Arrays.equals(buffer, TEST_DATA.getBytes(StandardCharsets.UTF_8)));
      assertEquals(stream.getPos(), TEST_DATA.length());

      // All data has been read, and pos is current at EOF. Next read should return -1.
      assertEquals(-1, stream.read(buffer, 0, TEST_DATA.length()));
    }
  }

  @Test
  void testReadWithBufferAndSeeks() throws IOException {
    try (S3SeekableInputStream stream = getTestStream()) {

      byte[] buffer = new byte[11];

      // Read from pos 0, check pos after read is correct
      long bytesRead = stream.read(new byte[3], 0, 3);
      assertEquals(3, bytesRead);
      assertEquals(stream.getPos(), 3);

      byte[] expectedResult =
          ByteBuffer.wrap(new byte[11])
              .put(new byte[3])
              .put(TEST_DATA.substring(3, 11).getBytes(StandardCharsets.UTF_8))
              .array();

      assertEquals(8, stream.read(buffer, 3, 8));
      assertTrue(Arrays.equals(buffer, expectedResult));
      assertEquals(stream.getPos(), 11);
      assertEquals(stream.read(), TEST_DATA.getBytes(StandardCharsets.UTF_8)[11]);

      // Check things still work after a backward seek
      stream.seek(4);
      byte[] readBuffer = new byte[7];
      assertEquals(7, stream.read(readBuffer, 0, 7));
      assertEquals(11, stream.getPos());
      assertTrue(
          Arrays.equals(readBuffer, TEST_DATA.substring(4, 11).getBytes(StandardCharsets.UTF_8)));
    }
  }

  @Test
  void testReadWithBufferOutOfBounds() throws IOException {
    try (S3SeekableInputStream stream = getTestStream()) {

      // Read beyond EOF, expect all bytes to be read and pos to be EOF.
      assertEquals(
          TEST_DATA.length(),
          stream.read(new byte[TEST_DATA.length() + 20], 0, TEST_DATA.length() + 20));
      assertEquals(20, stream.getPos());

      // Read beyond EOF after a seek, expect only num bytes read to be equal to that left in the
      // stream, and pos to be EOF.
      stream.seek(18);
      assertEquals(2, stream.read(new byte[TEST_DATA.length() + 20], 0, TEST_DATA.length() + 20));
      assertEquals(20, stream.getPos());
    }
  }

  @Test
  void testReadTailWithInvalidArgument() throws IOException {
    // Given: seekable stream
    try (S3SeekableInputStream stream = getTestStream()) {
      // When & Then: reading tail with invalid arguments, exception is thrown
      // -1 is invalid length
      assertThrows(IllegalArgumentException.class, () -> stream.readTail(new byte[3], 0, -1));
      // 100K is bigger than test data size
      assertThrows(IllegalArgumentException.class, () -> stream.readTail(new byte[103], 0, 100));
      // Requesting more data than byte buffer size
      assertThrows(IndexOutOfBoundsException.class, () -> stream.readTail(new byte[10], 0, 100));
    }
  }

  @Test
  void testReadTailHappyCase() throws IOException {
    // Given: seekable stream
    try (S3SeekableInputStream stream = getTestStream()) {

      // When: tail of length 10 is requested
      byte[] buf = new byte[11];
      int numBytesRead = stream.readTail(buf, 0, buf.length);

      // Then: 10 bytes are read, 10 is returned, 10 bytes in the buffer are the same as last 10
      // bytes
      // of test data
      assertEquals(11, numBytesRead);
      assertEquals("12345678910", new String(buf, StandardCharsets.UTF_8));
    }
  }

  @Test
  void testReadTailDoesNotAlterPosition() throws IOException {
    // Given: seekable stream
    try (S3SeekableInputStream stream = getTestStream()) {

      // When: 1) we are reading from the stream, 2) reading the tail of the stream, 3) reading more
      // from the stream
      byte[] one = new byte[5];
      byte[] two = new byte[11];
      byte[] three = new byte[5];

      int numBytesRead1 = stream.read(one, 0, one.length);
      int numBytesRead2 = stream.readTail(two, 0, two.length);
      int numBytesRead3 = stream.read(three, 0, three.length);

      // Then: read #2 did not alter the position and reads #1 and #3 return subsequent bytes
      assertEquals(5, numBytesRead1);
      assertEquals(11, numBytesRead2);
      assertEquals(5, numBytesRead3);

      assertEquals("test-", new String(one, StandardCharsets.UTF_8));
      assertEquals("data1", new String(three, StandardCharsets.UTF_8));
      assertEquals("12345678910", new String(two, StandardCharsets.UTF_8));
    }
  }

  @Test
  // Dependencies returning with a -1 read should not set the position back
  void testMinusOneIsHandledProperly() throws IOException {
    // Given: seekable stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    when(mockLogicalIO.metadata())
        .thenReturn(ObjectMetadata.builder().contentLength(200).etag("RANDOM").build());
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, mockLogicalIO, TestTelemetry.DEFAULT)) {

      // When: logical IO returns with a -1 read
      final int INITIAL_POS = 123;
      stream.seek(INITIAL_POS);
      when(mockLogicalIO.read(any(), anyInt(), anyInt(), anyLong())).thenReturn(-1);

      // Then: stream returns -1 and position did not change
      final int LEN = 5;
      byte[] b = new byte[LEN];
      assertEquals(-1, stream.read(b, 0, LEN));
      assertEquals(INITIAL_POS, stream.getPos());
    }
  }

  @Test
  void testMultiThreadUsage() throws IOException, InterruptedException {
    int filesSize = 8 * ONE_MB;
    StringBuilder sb = new StringBuilder(filesSize);
    sb.append(StringUtils.repeat("0", 8 * ONE_MB));
    S3URI s3URI = S3URI.of("test", "test");

    FakeObjectClient fakeObjectClient = new FakeObjectClient(sb.toString());
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            threadPool);

    AtomicReference<Throwable> thrown = new AtomicReference<>();

    // Create 20 threads to start multiple SeekableInputStream to read last and first 4 bytes
    ArrayList<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      threads.add(
          new Thread(
              () -> {
                try {
                  PhysicalIO physicalIO =
                      new PhysicalIOImpl(
                          s3URI,
                          metadataStore,
                          blobStore,
                          TestTelemetry.DEFAULT,
                          OpenStreamInformation.DEFAULT,
                          executorService,
                          PhysicalIOConfiguration.DEFAULT);
                  LogicalIO logicalIO =
                      new ParquetLogicalIOImpl(
                          TEST_OBJECT,
                          physicalIO,
                          TestTelemetry.DEFAULT,
                          LogicalIOConfiguration.DEFAULT,
                          new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
                          new DefaultRequestCallbackImpl());
                  try (SeekableInputStream stream =
                      new S3SeekableInputStream(TEST_URI, logicalIO, TestTelemetry.DEFAULT)) {
                    byte[] buffer = new byte[4];
                    long readBytes = stream.readTail(buffer, 0, 4);
                    assertEquals(4, readBytes);
                    readBytes = stream.read(buffer, 0, 4);
                    assertEquals(4, readBytes);
                  }
                } catch (Throwable e) {
                  thrown.set(e);
                }
              }));
    }
    // Start all the threads
    threads.forEach(Thread::start);
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (Throwable e) {
        fail("Unexpected exception", e);
      }
    }
    if (thrown.get() != null) {
      fail("Unexpected exception", thrown.get());
    }
  }

  @Test
  public void testReadOnClosedStream() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    seekableInputStream.close();
    SpotBugsLambdaWorkaround.assertReadResult(IOException.class, seekableInputStream::read, -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IOException.class, () -> seekableInputStream.read(new byte[8]), -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IOException.class, () -> seekableInputStream.read(new byte[8], 0, 8), -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IOException.class, () -> seekableInputStream.readTail(new byte[8], 0, 8), -1);
  }

  @Test
  public void testSeekOnClosedStream() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    seekableInputStream.close();
    assertThrows(IOException.class, () -> seekableInputStream.seek(0));
  }

  @Test
  public void testZeroLengthRead() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    assertEquals(0, seekableInputStream.read(new byte[0], 0, 0));
    assertEquals(0, seekableInputStream.readTail(new byte[0], 0, 0));
  }

  @Test
  public void testInsufficientBuffer() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    SpotBugsLambdaWorkaround.assertReadResult(
        IndexOutOfBoundsException.class, () -> seekableInputStream.read(new byte[8], 1, 8), -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IndexOutOfBoundsException.class, () -> seekableInputStream.read(new byte[0], 0, 8), -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IndexOutOfBoundsException.class, () -> seekableInputStream.readTail(new byte[0], 0, 8), -1);
    SpotBugsLambdaWorkaround.assertReadResult(
        IndexOutOfBoundsException.class, () -> seekableInputStream.readTail(new byte[0], 0, 8), -1);
    assertThrows(
        IndexOutOfBoundsException.class, () -> seekableInputStream.readFully(0, new byte[0], 0, 8));
  }

  @Test
  void testReadFullyWithInvalidArgument() throws IOException {
    // Given: seekable stream
    try (S3SeekableInputStream stream = getTestStream()) {
      // When & Then: reading with invalid arguments, exception is thrown
      // -1 is invalid position
      assertThrows(IllegalArgumentException.class, () -> stream.readFully(-1, new byte[10], 0, 5));
      // -1 is invalid length
      assertThrows(IllegalArgumentException.class, () -> stream.readFully(0, new byte[10], 0, -1));
      // Requesting more data than byte buffer size
      assertThrows(IndexOutOfBoundsException.class, () -> stream.readFully(0, new byte[5], 0, 10));
    }
  }

  @Test
  void testReadFullyHappyCase() throws IOException {
    // Given: seekable stream
    try (S3SeekableInputStream stream = getTestStream()) {
      // When: reading 5 bytes from position 3
      byte[] buf = new byte[5];
      stream.readFully(3, buf, 0, 5);

      // Then: buffer contains the expected 5 bytes from position 3
      byte[] expected = TEST_DATA.substring(3, 8).getBytes(StandardCharsets.UTF_8);
      assertArrayEquals(expected, buf);

      // Position should remain unchanged after readFully
      assertEquals(0, stream.getPos());
    }
  }

  @Test
  void testReadFullyDoesNotAlterPosition() throws IOException {
    // Given: seekable stream with data "test-data12345678910"
    try (S3SeekableInputStream stream = getTestStream()) {
      // When:
      // 1) Reading first 5 bytes from position 0 (should be "test-")
      // 2) Reading 5 bytes from position 10 using readFully (should be "23456")
      // 3) Reading next 5 bytes from current position (should be "data1")
      byte[] one = new byte[5];
      byte[] two = new byte[5];
      byte[] three = new byte[5];

      int numBytesRead1 = stream.read(one, 0, one.length);
      stream.readFully(10, two, 0, two.length);
      int numBytesRead3 = stream.read(three, 0, three.length);

      // Then: readFully did not alter the position and reads #1 and #3 return subsequent bytes
      // First read should return 5 bytes
      assertEquals(5, numBytesRead1);
      // Third read should also return 5 bytes, continuing from where first read left off
      assertEquals(5, numBytesRead3);

      // Verify the actual content of each buffer
      assertEquals("test-", new String(one, StandardCharsets.UTF_8));
      assertEquals("data1", new String(three, StandardCharsets.UTF_8));
      assertEquals("23456", new String(two, StandardCharsets.UTF_8));

      // Verify the stream position is at 10 (5 + 5) after all reads
      assertEquals(10, stream.getPos());
    }
  }

  @Test
  public void testReadFullyOnClosedStream() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    seekableInputStream.close();
    assertThrows(IOException.class, () -> seekableInputStream.readFully(0, new byte[8], 0, 8));
  }

  @Test
  public void testZeroLengthReadFully() throws IOException {
    S3SeekableInputStream seekableInputStream = getTestStream();
    assertDoesNotThrow(() -> seekableInputStream.readFully(0, new byte[0], 0, 0));
  }

  @Test
  void testReadFullyThrowsWhenInsufficientBytes() throws IOException {
    // Given: seekable stream with TEST_DATA (20 bytes)
    try (S3SeekableInputStream stream = getTestStream()) {
      // When & Then: trying to read beyond available data should throw IOException
      byte[] buffer = new byte[10];

      // Try to read 10 bytes starting at position 15 (only 5 bytes available)
      assertThrows(IOException.class, () -> stream.readFully(15, buffer, 0, 10));

      // Verify stream position remains unchanged after failed readFully
      assertEquals(0, stream.getPos());
    }
  }

  private S3SeekableInputStream getTestStream() {
    return new S3SeekableInputStream(TEST_URI, fakeLogicalIO, TestTelemetry.DEFAULT);
  }

  private S3SeekableInputStream getTestStreamWithContent(String content, S3URI s3URI)
      throws IOException {
    FakeObjectClient fakeObjectClient = new FakeObjectClient(content);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            threadPool);

    return new S3SeekableInputStream(
        s3URI,
        new ParquetLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(
                s3URI,
                metadataStore,
                blobStore,
                TestTelemetry.DEFAULT,
                OpenStreamInformation.DEFAULT,
                executorService,
                PhysicalIOConfiguration.DEFAULT),
            TestTelemetry.DEFAULT,
            LogicalIOConfiguration.DEFAULT,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
            new DefaultRequestCallbackImpl()),
        TestTelemetry.DEFAULT);
  }

  @Test
  public void testStreamMetadataConsistencyAfterTtlExpiry()
      throws IOException, InterruptedException {
    // Test that stream's metadata calls return consistent etag even after TTL expires
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(5).build();

    FakeObjectClient fakeObjectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class), threadPool);

    LogicalIO logicalIO =
        new ParquetLogicalIOImpl(
            TEST_URI,
            new PhysicalIOImpl(
                TEST_URI,
                metadataStore,
                blobStore,
                TestTelemetry.DEFAULT,
                OpenStreamInformation.DEFAULT,
                executorService,
                PhysicalIOConfiguration.DEFAULT),
            TestTelemetry.DEFAULT,
            LogicalIOConfiguration.DEFAULT,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
            new DefaultRequestCallbackImpl());

    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, logicalIO, TestTelemetry.DEFAULT)) {
      // Verify stream is created
      assertNotNull(stream);

      // First call to get etag
      String firstEtag = logicalIO.metadata().getEtag();

      Thread.sleep(10); // Wait for TTL expiry

      // Second call should return same etag (stream-level consistency)
      String secondEtag = logicalIO.metadata().getEtag();
      assertEquals(
          firstEtag,
          secondEtag,
          "Stream should maintain consistent etag even after metadata TTL expires");
    }
  }
}
