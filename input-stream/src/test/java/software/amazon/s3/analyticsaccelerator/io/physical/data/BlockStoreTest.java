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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressWarnings("unchecked")
public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
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
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));

    BlockKey blockKey = new BlockKey(objectKey, new Range(3, 5));

    // When: a new block is added
    blockStore.add(
        blockKey,
        new Block(
            blockKey,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getBlockKey().getRange().getStart(), 3);
    assertEquals(b.get().getBlockKey().getRange().getEnd(), 5);
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
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(2, 3));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(5, 10));
    BlockKey blockKey3 = new BlockKey(objectKey, new Range(12, 15));
    blockStore.add(
        blockKey1,
        new Block(
            blockKey1,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));
    blockStore.add(
        blockKey2,
        new Block(
            blockKey2,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));
    blockStore.add(
        blockKey3,
        new Block(
            blockKey3,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));

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
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));

    BlockKey blockKey1 = new BlockKey(objectKey, new Range(2, 3));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(5, 10));
    BlockKey blockKey3 = new BlockKey(objectKey, new Range(12, 15));
    blockStore.add(
        blockKey1,
        new Block(
            blockKey1,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));
    blockStore.add(
        blockKey2,
        new Block(
            blockKey2,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));
    blockStore.add(
        blockKey3,
        new Block(
            blockKey3,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            mock(BlobStoreIndexCache.class),
            OpenStreamInformation.DEFAULT));

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
    BlockKey blockKey = new BlockKey(objectKey, new Range(0, 5));
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    Block block = mock(Block.class);
    blockStore.add(blockKey, block);

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
    BlockKey blockKey1 = new BlockKey(objectKey, new Range(0, 5));
    BlockKey blockKey2 = new BlockKey(objectKey, new Range(0, 6));
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    Block b1 = mock(Block.class);
    Block b2 = mock(Block.class);
    blockStore.add(blockKey1, b1);
    blockStore.add(blockKey2, b2);

    // When: b1 throws when closed
    doThrow(new RuntimeException("something horrible")).when(b1).close();
    blockStore.close();

    // Then: 1\ blockStore.close did not throw, 2\ b2 was closed
    verify(b2, times(1)).close();
  }
}
