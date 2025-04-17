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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.stats.CacheStats;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RandomString";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final int OBJECT_SIZE = 100;
  private static final long DEFAULT_READ_TIMEOUT = 120_000;
  private static final int DEFAULT_READ_RETRY_COUNT = 20;

  @SneakyThrows
  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    FakeObjectClient fakeObjectClient = new FakeObjectClient("test-data");
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    // When: a new block is added
    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            3,
            5,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getStart(), 3);
    assertEquals(b.get().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__findNextMissingByteCorrect() throws IOException {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    int size = X_TIMES_16.getBytes(StandardCharsets.UTF_8).length;
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(size).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            2,
            3,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));
    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            5,
            10,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));
    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            12,
            15,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));

    // When & Then: we query for the next missing byte, the result is correct
    assertEquals(OptionalLong.of(0), blockStore.findNextMissingByte(0));
    assertEquals(OptionalLong.of(1), blockStore.findNextMissingByte(1));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(2));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(3));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(4));
    assertEquals(OptionalLong.of(11), blockStore.findNextMissingByte(5));
    assertEquals(OptionalLong.of(11), blockStore.findNextMissingByte(11));
    assertEquals(OptionalLong.empty(), blockStore.findNextMissingByte(14));
  }

  @SneakyThrows
  @Test
  public void test__blockStore__findNextAvailableByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            2,
            3,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));
    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            5,
            10,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));
    blockStore.add(
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            12,
            15,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0)));

    // When & Then: we query for the next available byte, the result is correct
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(0));
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(1));
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(2));
    assertEquals(OptionalLong.of(3), blockStore.findNextLoadedByte(3));
    assertEquals(OptionalLong.of(5), blockStore.findNextLoadedByte(4));
    assertEquals(OptionalLong.of(5), blockStore.findNextLoadedByte(5));
    assertEquals(OptionalLong.of(12), blockStore.findNextLoadedByte(11));
    assertEquals(OptionalLong.of(15), blockStore.findNextLoadedByte(15));
  }

  @Test
  public void test__blockStore__closesBlocks() {
    // Given: BlockStore with a block
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);
    Block block = mock(Block.class);
    blockStore.add(block);

    // When: blockStore is closed
    blockStore.close();

    // Then: underlying block is also closed
    verify(block, times(1)).close();
  }

  @Test
  public void test__blockStore__closeWorksWithExceptions() {
    // Given: BlockStore with two blocks
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);
    Block b1 = mock(Block.class);
    Block b2 = mock(Block.class);
    blockStore.add(b1);
    blockStore.add(b2);

    // When: b1 throws when closed
    doThrow(new RuntimeException("something horrible")).when(b1).close();
    blockStore.close();

    // Then: 1\ blockStore.close did not throw, 2\ b2 was closed
    verify(b2, times(1)).close();
  }

  @Test
  void testBlockStoreCacheHitsAndMisses() throws IOException {
    // Given: Reset cache stats and create BlockStore
    CacheStats.resetStats();
    final String TEST_DATA = "test-data";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    // When: Add a block covering positions 3-5
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            3,
            5,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0));

    blockStore.add(block);

    // Then: Test cache hits and misses
    Optional<Block> result1 = blockStore.getBlock(4); // Should be hit
    assertTrue(result1.isPresent(), "Block should be present for position 4");
    assertEquals(1, CacheStats.getHits(), "Should record one cache hit");
    assertEquals(0, CacheStats.getMisses(), "Should have no cache misses");

    Optional<Block> result2 = blockStore.getBlock(1); // Should be miss
    assertFalse(result2.isPresent(), "Block should not be present for position 1");
    assertEquals(1, CacheStats.getHits(), "Should still have one cache hit");
    assertEquals(1, CacheStats.getMisses(), "Should record one cache miss");

    // Test hit rate
    assertEquals(0.5, CacheStats.getHitRate(), "Hit rate should be 0.5 with one hit and one miss");
  }

  @Test
  void testBlockStoreCacheMultipleBlocks() throws IOException {
    CacheStats.resetStats();
    final String TEST_DATA = "test-data";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    // Add multiple blocks with gaps
    Block block1 =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            2,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0));

    Block block2 =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            4,
            6,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0));

    blockStore.add(block1);
    blockStore.add(block2);

    // Test various positions
    blockStore.getBlock(1); // hit
    blockStore.getBlock(3); // miss
    blockStore.getBlock(5); // hit
    blockStore.getBlock(7); // miss
    blockStore.getBlock(2); // hit

    assertEquals(3, CacheStats.getHits(), "Should record three cache hits");
    assertEquals(2, CacheStats.getMisses(), "Should record two cache misses");
    assertEquals(
        0.6, CacheStats.getHitRate(), "Hit rate should be 0.6 with three hits and two misses");
  }

  @Test
  void testBlockStoreCacheStatsReset() throws IOException {
    CacheStats.resetStats();
    final String TEST_DATA = "test-data";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    // Add a block and generate some stats
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            2,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0));

    blockStore.add(block);
    blockStore.getBlock(1); // hit
    blockStore.getBlock(3); // miss

    assertEquals(1, CacheStats.getHits(), "Should have one hit");
    assertEquals(1, CacheStats.getMisses(), "Should have one miss");

    // Reset stats
    CacheStats.resetStats();

    assertEquals(0, CacheStats.getHits(), "Hits should be reset to zero");
    assertEquals(0, CacheStats.getMisses(), "Misses should be reset to zero");
    assertEquals(0.0, CacheStats.getHitRate(), "Hit rate should be zero after reset");
  }

  @Test
  void testBlockStoreCacheConcurrent() throws InterruptedException, IOException {
    CacheStats.resetStats();
    final String TEST_DATA = "test-data";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();
    BlockStore blockStore = new BlockStore(objectKey, mockMetadataStore);

    // Add a block covering positions 3-5
    Block block =
        new Block(
            objectKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            3,
            5,
            0,
            ReadMode.SYNC,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_READ_RETRY_COUNT,
            new AtomicLong(0));

    blockStore.add(block);

    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    List<Future<?>> futures = new ArrayList<>();
    AtomicReference<Exception> testException = new AtomicReference<>();

    // Have multiple threads query both hit and miss positions
    for (int i = 0; i < threadCount; i++) {
      final int pos = (i % 2 == 0) ? 4 : 1; // alternate between hit and miss
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  blockStore.getBlock(pos);
                } catch (Exception e) {
                  testException.set(e);
                } finally {
                  latch.countDown();
                }
              });
      futures.add(future);
    }

    // Wait for all threads to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Timeout waiting for threads");

    // Check futures for completion and exceptions
    for (Future<?> future : futures) {
      try {
        future.get(1, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        fail("Future execution failed: " + e.getCause().getMessage());
      } catch (TimeoutException e) {
        fail("Future timed out: " + e.getMessage());
      }
    }

    // Check for any exceptions from threads
    if (testException.get() != null) {
      fail("Concurrent execution failed: " + testException.get().getMessage());
    }

    executor.shutdown();
    assertTrue(
        executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate properly");

    assertEquals(5, CacheStats.getHits(), "Should have recorded hits for half the threads");
    assertEquals(5, CacheStats.getMisses(), "Should have recorded misses for half the threads");
    assertEquals(0.5, CacheStats.getHitRate(), "Hit rate should be 0.5 with equal hits and misses");
  }
}
