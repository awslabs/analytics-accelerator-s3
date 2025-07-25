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
import static software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState.SUBMITTED;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlobTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final String TEST_DATA = "test-data-0123456789";
  private static final int OBJECT_SIZE = 100;
  ObjectMetadata mockMetadataStore =
      ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
  private final ExecutorService threadPool = Executors.newFixedThreadPool(30);

  @Test
  void testCreateBoundaries() {
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    assertThrows(
        NullPointerException.class,
        () -> new Blob(null, mockMetadataStore, mock(BlockManager.class), TestTelemetry.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new Blob(objectKey, null, mock(BlockManager.class), TestTelemetry.DEFAULT));

    assertThrows(
        NullPointerException.class,
        () -> new Blob(objectKey, mockMetadataStore, null, TestTelemetry.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new Blob(objectKey, mockMetadataStore, mock(BlockManager.class), null));
  }

  @Test
  public void testSingleByteReadReturnsCorrectByte() throws IOException {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: single byte reads are performed
    int r1 = blob.read(0);
    int r2 = blob.read(5);
    int r3 = blob.read(10);
    int r4 = blob.read(TEST_DATA.length() - 1);

    // Then: correct bytes are returned
    assertEquals(116, r1); // 't' = 116
    assertEquals(100, r2); // 'd' = 100
    assertEquals(48, r3); // '0' = 48
    assertEquals(57, r4); // '9' = 57
  }

  @Test
  public void testBufferedReadReturnsCorrectByte() throws IOException {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: buffered reads are performed
    byte[] b1 = new byte[4];
    blob.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    blob.read(b2, 0, b2.length, 5);

    // Then: correct bytes are returned
    assertEquals("test", new String(b1, StandardCharsets.UTF_8));
    assertEquals("data", new String(b2, StandardCharsets.UTF_8));
  }

  @Test
  public void testBufferedReadTestOverlappingRanges() throws IOException {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: buffered reads are performed
    byte[] b1 = new byte[4];
    blob.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    blob.read(b2, 0, b2.length, 2);

    // Then: correct bytes are returned
    assertEquals("test", new String(b1, StandardCharsets.UTF_8));
    assertEquals("st-d", new String(b2, StandardCharsets.UTF_8));
  }

  @Test
  public void testBufferedReadValidatesArguments() {
    // Given: test Blob
    Blob blob = getTestBlob("abc");

    // When & Then: read is called with illegal arguments, IllegalArgumentException is thrown
    byte[] b = new byte[4];
    assertThrows(IllegalArgumentException.class, () -> blob.read(-100));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, b.length, -100));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, b.length, b.length + 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, -1, b.length, 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, -1, 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, b.length + 1, b.length, 1));
  }

  @Test
  public void testExecuteSubmitsCorrectRanges() throws IOException {
    // Given: test blob and an IOPlan
    BlockManager blockManager = mock(BlockManager.class);
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 100));
    ranges.add(new Range(999, 1000));
    IOPlan ioPlan = new IOPlan(ranges);

    // When: the IOPlan is executed
    IOPlanExecution execution = blob.execute(ioPlan, ReadMode.COLUMN_PREFETCH);

    // Then: correct ranges are submitted
    assertEquals(SUBMITTED, execution.getState());
    verify(blockManager).makeRangeAvailable(0, 101, ReadMode.COLUMN_PREFETCH);
    verify(blockManager).makeRangeAvailable(999, 2, ReadMode.COLUMN_PREFETCH);
  }

  @Test
  public void testCloseClosesBlockManager() {
    // Given: test blob
    BlockManager blockManager = mock(BlockManager.class);
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    // When: blob is closed
    blob.close();

    // Then:
    verify(blockManager, times(1)).close();
  }

  private Blob getTestBlob(String data) {
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(data.length()).etag(ETAG).build();
    FakeObjectClient fakeObjectClient = new FakeObjectClient(data);
    BlockManager blockManager =
        new BlockManager(
            objectKey,
            fakeObjectClient,
            mockMetadataStore,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT,
            threadPool);

    return new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);
  }

  @Test
  public void testAsyncCleanupWithEmptyBlockStore() {
    // Given: test blob with empty block store
    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.isBlockStoreEmpty()).thenReturn(true);
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    // When: asyncCleanup is called
    blob.asyncCleanup();

    // Then: cleanup is not called since store is empty
    verify(blockManager, never()).cleanUp();
  }

  @Test
  public void testAsyncCleanupWithNonEmptyBlockStore() {
    // Given: test blob with non-empty block store
    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.isBlockStoreEmpty()).thenReturn(false);
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    // When: asyncCleanup is called
    blob.asyncCleanup();

    // Then: cleanup is called
    verify(blockManager, times(1)).cleanUp();
  }

  @Test
  public void testReadEntireContentLength() throws IOException {
    // Given: test blob with known content
    String data = "test123";
    Blob blob = getTestBlob(data);

    // When: reading entire content
    byte[] buffer = new byte[data.length()];
    int bytesRead = blob.read(buffer, 0, buffer.length, 0);

    // Then: all bytes are read correctly
    assertEquals(data.length(), bytesRead);
    assertEquals(data, new String(buffer, StandardCharsets.UTF_8));
  }

  @Test
  public void testReadBeyondContentLength() throws IOException {
    // Given: test blob with known content
    String data = "test";
    Blob blob = getTestBlob(data);

    // When: attempting to read beyond content length
    byte[] buffer = new byte[10];
    int bytesRead = blob.read(buffer, 0, buffer.length, 0);

    // Then: only available bytes are read
    assertEquals(data.length(), bytesRead);
  }

  @Test
  public void testExecuteWithFailure() throws IOException {
    // Given: test blob with block manager that throws exception
    BlockManager blockManager = mock(BlockManager.class);
    doThrow(new RuntimeException("Simulated failure"))
        .when(blockManager)
        .makeRangeAvailable(anyLong(), anyLong(), any(ReadMode.class));

    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);
    List<Range> ranges = Collections.singletonList(new Range(0, 100));
    IOPlan ioPlan = new IOPlan(ranges);

    // When: executing plan that will fail
    IOPlanExecution execution = blob.execute(ioPlan, ReadMode.COLUMN_PREFETCH);

    // Then: execution state is FAILED
    assertEquals(IOPlanState.FAILED, execution.getState());
  }

  @Test
  public void testReadWithMissingBlock() {
    // Given: test blob with block manager that returns empty block
    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.getBlock(anyLong())).thenReturn(Optional.empty());
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    // When & Then: reading with missing block throws exception
    byte[] buffer = new byte[10];
    assertThrows(IllegalStateException.class, () -> blob.read(buffer, 0, buffer.length, 0));
  }

  @Test
  public void testPositionReadWithMissingBlock() {
    // Given: test blob with block manager that returns empty block
    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.getBlock(anyLong())).thenReturn(Optional.empty());
    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    assertThrows(IllegalStateException.class, () -> blob.read(0));
  }

  @Test
  public void testReadWithPartialBlockRead() throws IOException {
    // Given: test blob with block that returns partial data
    Block mockBlock = mock(Block.class);
    when(mockBlock.read(any(byte[].class), anyInt(), anyInt(), anyLong()))
        .thenReturn(-1); // Simulate end of stream

    BlockManager blockManager = mock(BlockManager.class);
    when(blockManager.getBlock(anyLong())).thenReturn(Optional.of(mockBlock));

    Blob blob = new Blob(objectKey, mockMetadataStore, blockManager, TestTelemetry.DEFAULT);

    // When: reading from block
    byte[] buffer = new byte[10];
    int bytesRead = blob.read(buffer, 0, buffer.length, 0);

    // Then: returns number of bytes actually read
    assertEquals(0, bytesRead);
  }
}
