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

import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressWarnings("unchecked")
public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RandomString";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final int OBJECT_SIZE = 100;

  @SneakyThrows
  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));

    BlockKey blockKey = new BlockKey(objectKey, new Range(3, 5));

    // When: a new block is added
    blockStore.add(new Block(blockKey, 0, mock(BlobStoreIndexCache.class)));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getBlockKey().getRange().getStart(), 3);
    assertEquals(b.get().getBlockKey().getRange().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__closesBlocks() {
    // Given: BlockStore with a block
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
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
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
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
}
