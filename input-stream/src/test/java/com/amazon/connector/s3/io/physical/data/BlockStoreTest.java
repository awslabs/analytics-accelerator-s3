package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    FakeObjectClient fakeObjectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore =
        new BlockStore(
            TEST_URI, metadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));

    // When: a new block is added
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 3, 5, 0, ReadMode.SYNC));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getStart(), 3);
    assertEquals(b.get().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__findNextMissingByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore =
        new BlockStore(
            TEST_URI, metadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));

    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 2, 3, 0, ReadMode.SYNC));
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 5, 10, 0, ReadMode.SYNC));
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 12, 15, 0, ReadMode.SYNC));

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

  @Test
  public void test__blockStore__findNextAvailableByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore =
        new BlockStore(
            TEST_URI, metadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));

    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 2, 3, 0, ReadMode.SYNC));
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 5, 10, 0, ReadMode.SYNC));
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 12, 15, 0, ReadMode.SYNC));

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
    BlockStore blockStore =
        new BlockStore(
            TEST_URI,
            mock(MetadataStore.class),
            PhysicalIOConfiguration.DEFAULT,
            mock(MemoryTracker.class));
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
    BlockStore blockStore =
        new BlockStore(
            TEST_URI,
            mock(MetadataStore.class),
            PhysicalIOConfiguration.DEFAULT,
            mock(MemoryTracker.class));
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
  public void testMemoryTrackedOnEviction() {
    // Given: BlockStore with a block
    MemoryTracker memoryTracker = mock(MemoryTracker.class);
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().blobStoreCapacity(3).build();

    // Cache which will do evictions synchronously to aid testing.
    Cache<Integer, Block> blockCache =
        Caffeine.newBuilder()
            .maximumSize(configuration.getBlobStoreCapacity())
            .expireAfterWrite(Duration.ofMillis(configuration.getCacheEvictionTimeMillis()))
            .executor(Runnable::run)
            .removalListener(
                (Integer key, Block block, RemovalCause cause) ->
                    memoryTracker.freeMemory(block.getLength()))
            .build();

    BlockStore blockStore =
        new BlockStore(
            TEST_URI, mock(MetadataStore.class), configuration, memoryTracker, blockCache);

    byte[] content = new byte[101];
    FakeObjectClient fakeObjectClient =
        new FakeObjectClient(new String(content, StandardCharsets.UTF_8));
    Block block =
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 100, 200, 0, ReadMode.SYNC);

    blockStore.add(block);
    blockStore.add(block);
    blockStore.add(block);

    verify(memoryTracker, times(3)).incrementMemoryUsed(101);

    // Cache is of size 3, check that an element is evicted when a fourth element is added.
    blockStore.add(block);
    verify(memoryTracker, times(1)).freeMemory(101);

    // When: blockStore is closed
    blockStore.close();
    // All allocated
    verify(memoryTracker, times(4)).freeMemory(101);
  }
}
