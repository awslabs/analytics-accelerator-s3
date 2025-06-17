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
public class BlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();

  @Test
  public void testConstructor() throws IOException {
    Block block = getDefaultBlockBuilder().build();
    assertNotNull(block);
  }

  @Test
  @DisplayName("Test read method with valid position")
  void testReadWithValidPosition() throws IOException {
    // Setup
    final String TEST_DATA = "test-data";
    byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);

    BlobStoreIndexCache mockIndexCache = new BlobStoreIndexCache(PhysicalIOConfiguration.DEFAULT);
    mockIndexCache = spy(mockIndexCache);
    Block block = getDefaultBlockBuilder().indexCache(mockIndexCache).build();
    BlockKey blockKey = block.getBlockKey();

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
    Block block = getDefaultBlockBuilder().build();

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
    Block block = getDefaultBlockBuilder().build();

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
  public void testEmptyFieldsInBuilderReturnDefaults() throws IOException {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));

    Block block =
        Block.builder()
            .blockKey(blockKey)
            .objectClient(fakeObjectClient)
            .readMode(ReadMode.SYNC)
            .aggregatingMetrics(mock(Metrics.class))
            .indexCache(mock(BlobStoreIndexCache.class))
            .openStreamInformation(OpenStreamInformation.DEFAULT)
            .build();

    assertEquals(0, block.getGeneration());
    assertEquals(PhysicalIOConfiguration.DEFAULT, block.getConfiguration());
  }

  @Test
  void testNulls() {
    assertThrows(NullPointerException.class, () -> getDefaultBlockBuilder().blockKey(null).build());

    assertThrows(
        NullPointerException.class, () -> getDefaultBlockBuilder().objectClient(null).build());
    assertThrows(
        NullPointerException.class, () -> getDefaultBlockBuilder().telemetry(null).build());
    assertThrows(NullPointerException.class, () -> getDefaultBlockBuilder().readMode(null).build());
    assertThrows(
        NullPointerException.class, () -> getDefaultBlockBuilder().configuration(null).build());
    assertThrows(
        NullPointerException.class,
        () -> getDefaultBlockBuilder().aggregatingMetrics(null).build());
    assertThrows(
        NullPointerException.class, () -> getDefaultBlockBuilder().indexCache(null).build());
    assertThrows(
        NullPointerException.class,
        () -> getDefaultBlockBuilder().openStreamInformation(null).build());
  }

  @Test
  void testBoundaries() {
    final String TEST_DATA = "test-data";
    assertThrows(
        IllegalArgumentException.class,
        () ->
            getDefaultBlockBuilder()
                .blockKey(new BlockKey(objectKey, new Range(-1, TEST_DATA.length())))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> getDefaultBlockBuilder().blockKey(new BlockKey(objectKey, new Range(0, -5))).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> getDefaultBlockBuilder().blockKey(new BlockKey(objectKey, new Range(10, 1))).build());
    assertThrows(
        IllegalArgumentException.class, () -> getDefaultBlockBuilder().generation(-1).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> getDefaultBlockBuilder().blockKey(new BlockKey(objectKey, new Range(-5, 0))).build());
  }

  @SneakyThrows
  @Test
  void testReadBoundaries() {
    byte[] b = new byte[4];
    Block block = getDefaultBlockBuilder().build();

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
    Block block = getDefaultBlockBuilder().build();
    assertTrue(block.contains(0));
    assertFalse(block.contains(TEST_DATA.length() + 1));
  }

  @SneakyThrows
  @Test
  void testContainsBoundaries() {
    Block block = getDefaultBlockBuilder().build();
    assertThrows(IllegalArgumentException.class, () -> block.contains(-1));
  }

  @Test
  void testReadTimeoutAndRetry() throws IOException {
    final String TEST_DATA = "test-data";
    ObjectKey stuckObjectKey =
        ObjectKey.builder().s3URI(S3URI.of("stuck-client", "bar")).etag(ETAG).build();
    ObjectClient fakeStuckObjectClient = new FakeStuckObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(stuckObjectKey, new Range(0, TEST_DATA.length()));
    Block block =
        getDefaultBlockBuilder().blockKey(blockKey).objectClient(fakeStuckObjectClient).build();
    assertThrows(IOException.class, () -> block.read(4));
  }

  @SneakyThrows
  @Test
  void testClose() {
    Block block = getDefaultBlockBuilder().build();
    block.close();
    block.close();
  }

  // This method will encapsulate test-common logic. Being a builder
  // it lets each test overwrite relevant fields as needed.
  private Block.BlockBuilder getDefaultBlockBuilder() {

    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));

    return Block.builder()
        .blockKey(blockKey)
        .objectClient(fakeObjectClient)
        .telemetry(TestTelemetry.DEFAULT)
        .readMode(ReadMode.SYNC)
        .configuration(PhysicalIOConfiguration.DEFAULT)
        .aggregatingMetrics(mock(Metrics.class))
        .indexCache(mock(BlobStoreIndexCache.class))
        .openStreamInformation(OpenStreamInformation.DEFAULT);
  }
}
