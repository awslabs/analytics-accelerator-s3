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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
@SuppressWarnings("unchecked")
public class BlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final long DEFAULT_READ_TIMEOUT = 120_000;
  private static final int DEFAULT_READ_RETRY_COUNT = 20;
  private static final byte[] TEST_DATA_BYTES = "test-data".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testConstructor() throws IOException {

    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));
    Block block =
        new Block(
            blockKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    assertNotNull(block);
  }

  @Test
  @DisplayName("Test read method with valid position")
  void testReadWithValidPosition() throws IOException {
    // Setup
    final String TEST_DATA = "test-data";
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);

    Metrics mockMetrics = mock(Metrics.class);
    BlobStoreIndexCache mockIndexCache = new BlobStoreIndexCache(PhysicalIOConfiguration.DEFAULT);
    mockIndexCache = spy(mockIndexCache);

    Block block =
        new Block(
            blockKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mockMetrics,
            mockIndexCache,
            OpenStreamInformation.DEFAULT);

    // Test when data is not in cache
    when(mockIndexCache.contains(blockKey)).thenReturn(false);
    int result = block.read(0);

    // Verify
    verify(mockIndexCache, times(2)).put(blockKey, blockKey.getRange().getLength());
    verify(mockIndexCache, times(0)).getIfPresent(blockKey);
    assertEquals(Byte.toUnsignedInt(testData[0]), result);

    // Test when data is in cache
    when(mockIndexCache.contains(blockKey)).thenReturn(true);
    result = block.read(1);

    // Verify
    verify(mockIndexCache).getIfPresent(blockKey);
    assertEquals(Byte.toUnsignedInt(testData[1]), result);
  }

  @Test
  public void testSingleByteReadReturnsCorrectByte() throws IOException {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA_BYTES.length));
    Block block =
        new Block(
            blockKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);

    // When: bytes are requested from the block
    int r1 = block.read(0);
    int r2 = block.read(TEST_DATA.length() - 1);
    int r3 = block.read(4);

    // Then: they are the correct bytes
    assertEquals(116, r1); // 't' = 116
    assertEquals(97, r2); // 'a' = 97
    assertEquals(45, r3); // '-' = 45
  }

  @Test
  public void testBufferedReadReturnsCorrectBytes() throws IOException {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));
    Block block =
        new Block(
            blockKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);

    // When: bytes are requested from the block
    byte[] b1 = new byte[4];
    int r1 = block.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    int r2 = block.read(b2, 0, b2.length, 5);

    // Then: they are the correct bytes
    assertEquals(4, r1);
    assertEquals("test", new String(b1, StandardCharsets.UTF_8));

    assertEquals(4, r2);
    assertEquals("data", new String(b2, StandardCharsets.UTF_8));
  }

  @Test
  void testNulls() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                null,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                blockKey,
                null,
                TestTelemetry.DEFAULT,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                blockKey,
                fakeObjectClient,
                null,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                null));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                blockKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                null,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                null));
  }

  @Test
  void testBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                new BlockKey(objectKey, new Range(-1, TEST_DATA.length())),
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                new BlockKey(objectKey, new Range(0, -5)),
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                new BlockKey(objectKey, new Range(20, 1)),
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                new BlockKey(objectKey, new Range(0, 5)),
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                -1,
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                new BlockKey(objectKey, new Range(-5, 0)),
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                TEST_DATA.length(),
                ReadMode.SYNC,
                PhysicalIOConfiguration.DEFAULT,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT));
  }

  @SneakyThrows
  @Test
  void testReadBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    byte[] b = new byte[4];
    Block block =
        new Block(
            new BlockKey(objectKey, new Range(0, TEST_DATA.length())),
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    assertThrows(IllegalArgumentException.class, () -> block.read(-10));
    assertThrows(NullPointerException.class, () -> block.read(null, 0, 3, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, -5, 3, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, 0, -5, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, 10, 3, 1));
  }

  @SneakyThrows
  @Test
  void testContains() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            new BlockKey(objectKey, new Range(0, TEST_DATA.length())),
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    assertTrue(block.contains(0));
    assertFalse(block.contains(TEST_DATA.length() + 1));
  }

  @SneakyThrows
  @Test
  void testContainsBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            new BlockKey(objectKey, new Range(0, TEST_DATA.length())),
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    assertThrows(IllegalArgumentException.class, () -> block.contains(-1));
  }

  @Test
  public void testGetRequestCallbackCalled() throws IOException {
    final String TEST_DATA = "test-data";
    RequestCallback mockCallback = mock(RequestCallback.class);
    OpenStreamInformation openStreamInfo =
        OpenStreamInformation.builder().requestCallback(mockCallback).build();

    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));

    new Block(
        blockKey,
        fakeObjectClient,
        TestTelemetry.DEFAULT,
        0,
        ReadMode.SYNC,
        PhysicalIOConfiguration.DEFAULT,
        mock(Metrics.class),
        mock(BlobStoreIndexCache.class),
        openStreamInfo);

    verify(mockCallback, times(1)).onGetRequest();
  }

  @Test
  void testReadTimeoutAndRetry() throws IOException {
    final String TEST_DATA = "test-data";
    ObjectKey stuckObjectKey =
        ObjectKey.builder().s3URI(S3URI.of("stuck-client", "bar")).etag(ETAG).build();
    ObjectClient fakeStuckObjectClient = new FakeStuckObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(stuckObjectKey, new Range(0, TEST_DATA.length()));
    Block block =
        new Block(
            blockKey,
            fakeStuckObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    assertThrows(IOException.class, () -> block.read(4));
  }

  @SneakyThrows
  @Test
  void testClose() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            new BlockKey(objectKey, new Range(0, TEST_DATA.length())),
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT);
    block.close();
    block.close();
  }
}
