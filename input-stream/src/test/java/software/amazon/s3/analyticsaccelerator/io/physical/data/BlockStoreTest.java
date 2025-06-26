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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressWarnings("unchecked")
@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final int OBJECT_SIZE = 100;
  private static final long DEFAULT_READ_TIMEOUT = 120_000;
  private static final int DEFAULT_READ_RETRY_COUNT = 20;

  private BlobStoreIndexCache mockIndexCache;
  private Metrics mockMetrics;
  private PhysicalIOConfiguration configuration;
  private BlockStore blockStore;

  /** Sets up the test environment before each test. */
  @BeforeEach
  public void setUp() {
    mockIndexCache = mock(BlobStoreIndexCache.class);
    mockMetrics = mock(Metrics.class);
    configuration = PhysicalIOConfiguration.DEFAULT;
    blockStore = new BlockStore(mockIndexCache, mockMetrics, configuration);
  }

  @Test
  public void constructor_nullIndexCache_throws() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new BlockStore(null, mockMetrics, configuration);
        });
  }

  @Test
  public void constructor_nullMetrics_throws() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new BlockStore(mockIndexCache, null, configuration);
        });
  }

  @Test
  public void constructor_nullConfiguration_throws() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new BlockStore(mockIndexCache, mockMetrics, null);
        });
  }

  @Test
  public void constructor_allNonNull_succeeds() {
    new BlockStore(mockIndexCache, mockMetrics, configuration);
  }

  @SneakyThrows
  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    BlockKey blockKey = new BlockKey(objectKey, new Range(3, 5));

    // When: a new block is added
    blockStore.add(
        new Block(
            blockKey,
            0,
            mock(BlobStoreIndexCache.class),
            mock(Metrics.class),
            DEFAULT_READ_TIMEOUT));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getBlockKey().getRange().getStart(), 3);
    assertEquals(b.get().getBlockKey().getRange().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__closesBlocks() throws IOException {
    // Given: BlockStore with a block
    Block block = mock(Block.class);
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    when(block.getBlockKey()).thenReturn(blockKey);
    blockStore.add(block);

    // When: blockStore is closed
    blockStore.close();

    // Then: underlying block is also closed
    verify(block, times(1)).close();
  }

  @Test
  public void test__blockStore__closeWorksWithExceptions() throws IOException {
    // Given: BlockStore with two blocks
    Block b1 = mock(Block.class);
    Block b2 = mock(Block.class);

    // Set up the blocks with different indices
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 8191));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(8192, 16383));
    when(b1.getBlockKey()).thenReturn(blockKey1);
    when(b2.getBlockKey()).thenReturn(blockKey2);

    blockStore.add(b1);
    blockStore.add(b2);

    // When: b1 throws when closed
    doThrow(new RuntimeException("something horrible")).when(b1).close();
    blockStore.close();

    // Then: 1\ blockStore.close did not throw, 2\ b2 was closed
    verify(b2, times(1)).close();
  }

  @Test
  public void test__blockStore__getBlockByIndex() {
    // Given: BlockStore with a block at a specific index
    BlockKey blockKey =
        new BlockKey(objectKey, new Range(8192, 16383)); // Assuming readBufferSize is 8KB
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    // When: getBlockByIndex is called with the correct index
    Optional<Block> result =
        blockStore.getBlockByIndex(1); // Index 1 corresponds to range 8192-16383

    // Then: The correct block is returned
    assertTrue(result.isPresent());
    assertEquals(block, result.get());

    // When: getBlockByIndex is called with a non-existent index
    Optional<Block> nonExistentResult = blockStore.getBlockByIndex(2);

    // Then: Empty optional is returned
    assertFalse(nonExistentResult.isPresent());
  }

  @Test
  public void test__blockStore__getBlockByIndex_negativeIndex() {
    // When: getBlockByIndex is called with a negative index
    // Then: IllegalArgumentException is thrown
    assertThrows(IllegalArgumentException.class, () -> blockStore.getBlockByIndex(-1));
  }

  @Test
  public void test__blockStore__getBlock_negativePosition() {
    // When: getBlock is called with a negative position
    // Then: IllegalArgumentException is thrown
    assertThrows(IllegalArgumentException.class, () -> blockStore.getBlock(-1));
  }

  @Test
  public void test__blockStore__add_duplicateBlock() {
    // Given: A block already in the store
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block1 = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    Block block2 = new Block(blockKey, 1, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);

    // When: The first block is added
    blockStore.add(block1);

    // And: A second block with the same index is added
    blockStore.add(block2);

    // Then: The first block remains in the store
    Optional<Block> result = blockStore.getBlockByIndex(0);
    assertTrue(result.isPresent());
    assertEquals(0, result.get().getGeneration());
  }

  @Test
  public void test__blockStore__remove() {
    // Given: A block in the store
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    // When: The block is removed
    blockStore.remove(block);

    // Then: The block is no longer in the store
    Optional<Block> result = blockStore.getBlockByIndex(0);
    assertFalse(result.isPresent());

    // And: Memory usage metrics are updated
    verify(mockMetrics).reduce(eq(MetricKey.MEMORY_USAGE), eq(8192L)); // Range length is 8192
  }

  @Test
  public void test__blockStore__remove_nonExistentBlock() {
    // Given: A block not in the store
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);

    // When: An attempt is made to remove the block
    blockStore.remove(block);

    // Then: No metrics are updated
    verify(mockMetrics, never()).reduce(any(), anyLong());
  }

  @Test
  public void test__blockStore__getMissingBlockIndexesInRange() {
    // Given: A BlockStore with blocks at indexes 0 and 2 (8KB block size)
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 8191)); // Index 0
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(16384, 24575)); // Index 2

    Block block1 = new Block(blockKey1, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    Block block2 = new Block(blockKey2, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);

    blockStore.add(block1);
    blockStore.add(block2);

    // When: Missing blocks are requested for range covering indexes 0-2
    List<Integer> missingBlocks =
        blockStore.getMissingBlockIndexesInRange(new Range(0, 24575), true);

    // Then: Only index 1 is reported as missing (indexes 0 and 2 exist)
    assertEquals(1, missingBlocks.size());
    assertTrue(missingBlocks.contains(1));

    // And: Metrics are updated correctly (2 hits, 1 miss)
    verify(mockMetrics, times(2)).add(eq(MetricKey.CACHE_HIT), eq(1L));
    verify(mockMetrics, times(1)).add(eq(MetricKey.CACHE_MISS), eq(1L));
  }

  @Test
  public void test__blockStore__getMissingBlockIndexesInRange_noMeasure() {
    // Given: A BlockStore with a block at index 0
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    // When: Missing blocks are requested with measure=false
    blockStore.getMissingBlockIndexesInRange(new Range(0, 16383), false);

    // Then: No metrics are updated
    verify(mockMetrics, never()).add(any(), anyLong());
  }

  @Test
  public void test__blockStore__cleanUp() {
    // Given: A BlockStore with two blocks
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 8191));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(8192, 16383));

    Block block1 = mock(Block.class);
    Block block2 = mock(Block.class);

    when(block1.getBlockKey()).thenReturn(blockKey1);
    when(block2.getBlockKey()).thenReturn(blockKey2);
    when(block1.isDataReady()).thenReturn(true);
    when(block2.isDataReady()).thenReturn(true);

    // First block is not in index cache, second block is
    when(mockIndexCache.contains(blockKey1)).thenReturn(false);
    when(mockIndexCache.contains(blockKey2)).thenReturn(true);

    blockStore.add(block1);
    blockStore.add(block2);

    // When: cleanUp is called
    blockStore.cleanUp();

    // Then: Only the first block is removed (range length is 8192)
    verify(mockMetrics).reduce(eq(MetricKey.MEMORY_USAGE), eq(8192L));

    // And: The first block is no longer in the store
    Optional<Block> removedBlock = blockStore.getBlockByIndex(0);
    assertFalse(removedBlock.isPresent());

    // And: The second block remains
    Optional<Block> remainingBlock = blockStore.getBlockByIndex(1);
    assertTrue(remainingBlock.isPresent());
    assertEquals(block2, remainingBlock.get());
  }

  @Test
  public void test__blockStore__isEmpty() {
    // Given: An empty BlockStore
    // Then: isEmpty returns true
    assertTrue(blockStore.isEmpty());

    // When: A block is added
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    // Then: isEmpty returns false
    assertFalse(blockStore.isEmpty());

    // When: The block is removed
    blockStore.remove(block);

    // Then: isEmpty returns true again
    assertTrue(blockStore.isEmpty());
  }

  @Test
  public void test__blockStore__concurrentAddRemove() throws InterruptedException {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      executor.submit(
          () -> {
            BlockKey blockKey =
                new BlockKey(objectKey, new Range(index * 8192L, (index + 1) * 8192L - 1));
            Block block =
                new Block(blockKey, index, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
            blockStore.add(block);
            blockStore.remove(block);
            latch.countDown();
          });
    }

    boolean completed = latch.await(10, TimeUnit.SECONDS);
    assertTrue(completed, "Timeout waiting for concurrent add/remove operations to complete");
    assertTrue(blockStore.isEmpty());

    executor.shutdownNow();
  }

  @Test
  public void test__blockStore__getBlock_atRangeBoundaries() {
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    // At start of range
    Optional<Block> startBlock = blockStore.getBlock(0);
    assertTrue(startBlock.isPresent());
    assertEquals(block, startBlock.get());

    // At end of range
    Optional<Block> endBlock = blockStore.getBlock(8191);
    assertTrue(endBlock.isPresent());
    assertEquals(block, endBlock.get());
  }

  @Test
  public void test__blockStore__add_nullBlock() {
    assertThrows(NullPointerException.class, () -> blockStore.add(null));
  }

  @Test
  public void test__blockStore__remove_nullBlock() {
    // Should not throw exception
    blockStore.remove(null);
    // Optionally verify no metrics or actions triggered
    verify(mockMetrics, never()).reduce(any(), anyLong());
  }

  @Test
  public void test__blockStore__getBlock_positionOutsideAnyBlock() {
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 8191));
    Block block = new Block(blockKey, 0, mockIndexCache, mockMetrics, DEFAULT_READ_TIMEOUT);
    blockStore.add(block);

    Optional<Block> outsideBlock = blockStore.getBlock(100_000);
    assertFalse(outsideBlock.isPresent());
  }

  @Test
  public void test__blockStore__cleanUp_leavesDataNotReadyBlocks() {
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 8191));
    Block block1 = mock(Block.class);
    when(block1.getBlockKey()).thenReturn(blockKey1);
    when(block1.isDataReady()).thenReturn(false);

    when(mockIndexCache.contains(blockKey1)).thenReturn(false);

    blockStore.add(block1);

    blockStore.cleanUp();

    // Should still be present since isDataReady is false
    Optional<Block> result = blockStore.getBlockByIndex(0);
    assertTrue(result.isPresent());

    verify(mockMetrics, never()).reduce(eq(MetricKey.MEMORY_USAGE), anyLong());
  }

  @Test
  public void test__blockStore__getMissingBlockIndexesInRange_startGreaterThanEnd() {
    assertThrows(
        IllegalArgumentException.class,
        () -> blockStore.getMissingBlockIndexesInRange(new Range(10_000, 5_000), true));
  }

  @Test
  public void test__blockStore__close_multipleBlocksThrowExceptions() throws IOException {
    Block b1 = mock(Block.class);
    Block b2 = mock(Block.class);

    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 8191));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(8192, 16383));
    when(b1.getBlockKey()).thenReturn(blockKey1);
    when(b2.getBlockKey()).thenReturn(blockKey2);

    blockStore.add(b1);
    blockStore.add(b2);

    doThrow(new RuntimeException("error1")).when(b1).close();
    doThrow(new RuntimeException("error2")).when(b2).close();

    // Should not throw exception
    blockStore.close();

    verify(b1).close();
    verify(b2).close();
  }
}
