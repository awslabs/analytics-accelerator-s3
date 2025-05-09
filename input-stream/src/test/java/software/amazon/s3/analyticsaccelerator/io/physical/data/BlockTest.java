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
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.Cache;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.FakeStuckObjectClient;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RandomString";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final long DEFAULT_READ_TIMEOUT = 120_000;
  private static final int DEFAULT_READ_RETRY_COUNT = 20;

  @Test
  public void testSingleByteReadReturnsCorrectByte() throws IOException {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);

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
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);

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
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                null,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                objectKey,
                null,
                TestTelemetry.DEFAULT,
                0,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                null,
                0,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                TEST_DATA.length(),
                0,
                null,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
  }

  @Test
  void testBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                -1,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                -5,
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                20,
                1,
                0,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                5,
                -1,
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                objectKey,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                -5,
                0,
                TEST_DATA.length(),
                ReadMode.SYNC,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_READ_RETRY_COUNT));
  }

  @SneakyThrows
  @Test
  void testReadBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    byte[] b = new byte[4];
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);
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
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);
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
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);
    assertThrows(IllegalArgumentException.class, () -> block.contains(-1));
  }

  @Test
  void testReadTimeoutAndRetry() throws IOException {
    final String TEST_DATA = "test-data";
    ObjectKey stuckObjectKey =
        ObjectKey.builder().s3URI(S3URI.of("stuck-client", "bar")).etag(ETAG).build();
    ObjectClient fakeStuckObjectClient = new FakeStuckObjectClient(TEST_DATA);

    Block block =
        new Block(
            stuckObjectKey,
            fakeStuckObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);
    assertThrows(IOException.class, () -> block.read(4));
  }

  @SneakyThrows
  @Test
  void testClose() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT);
    block.close();
    block.close();
  }

  @Test
  void testCacheHitForTailMetadata() throws IOException {
    String TEST_DATA = "test-data";
    ObjectClient mockObjectClient = mock(ObjectClient.class);
    Cache mockCache = mock(Cache.class);

    //    simulate cache hit
    when(mockCache.get(any())).thenReturn(TEST_DATA.getBytes(StandardCharsets.UTF_8));

    Block block =
        new Block(
            objectKey,
            mockObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            0,
            true,
            mockCache,
            null);

    byte[] buffer = new byte[TEST_DATA.length()];
    block.read(buffer, 0, buffer.length, 0);

    assertEquals(TEST_DATA, new String(buffer, StandardCharsets.UTF_8));

    verify(mockCache).get(any());

    verify(mockObjectClient, never()).getObject(any(), any());
  }

  @Test
  void testCacheMissForTailMetadata() throws IOException {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Cache mockCache = mock(Cache.class);

    //    simulate cache miss
    when(mockCache.get(any())).thenReturn(null);

    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            0,
            true,
            mockCache,
            null);

    byte[] buffer = new byte[TEST_DATA.length()];
    block.read(buffer, 0, buffer.length, 0);

    assertEquals(TEST_DATA, new String(buffer, StandardCharsets.UTF_8));

    verify(mockCache).get(any());

    verify(mockCache).set(any(), any());
  }
}
