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
import static org.mockito.Mockito.mock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
@SuppressWarnings("unchecked")
public class IOPlannerTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();

  @Test
  void testCreateBoundaries() {
    assertThrows(NullPointerException.class, () -> new IOPlanner(null));
  }

  @Test
  void testPlanReadBoundaries() throws IOException {
    // Given: an empty BlockStore
    final int OBJECT_SIZE = 10_000;
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(-5, 10, 100));
    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(10, 5, 100));
    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(5, 5, 2));
  }

  @Test
  public void testPlanReadNoopWhenBlockStoreEmpty() throws IOException {
    // Given: an empty BlockStore
    final int OBJECT_SIZE = 10_000;
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range
    List<Range> missingRanges = ioPlanner.planRead(10, 100, OBJECT_SIZE - 1);

    // Then: it just falls through
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(10, 100));

    assertEquals(expected, missingRanges);
  }

  @Test
  public void testPlanReadDoesNotDoubleRead() throws IOException {
    // Given: a BlockStore with a (100,200) block in it
    final int OBJECT_SIZE = 10_000;
    byte[] content = new byte[OBJECT_SIZE];
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    FakeObjectClient fakeObjectClient =
        new FakeObjectClient(new String(content, StandardCharsets.UTF_8));
    BlockKey blockKey = new BlockKey(objectKey, new Range(100, 200));
    Block block =
        Block.builder()
            .blockKey(blockKey)
            .objectClient(fakeObjectClient)
            .readMode(ReadMode.SYNC)
            .aggregatingMetrics(mock(Metrics.class))
            .indexCache(mock(BlobStoreIndexCache.class))
            .openStreamInformation(OpenStreamInformation.DEFAULT)
            .build();
    blockStore.add(blockKey, block);
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we only request (0, 99) and (201, 400)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 99));
    expected.add(new Range(201, 400));

    assertEquals(expected, missingRanges);
  }

  @Test
  public void testPlanReadRegressionSingleByteObject() throws IOException {
    // Given: a single byte object and an empty block store
    final int OBJECT_SIZE = 1;
    ObjectMetadata mockMetadataStore =
        ObjectMetadata.builder().contentLength(OBJECT_SIZE).etag(ETAG).build();
    BlockStore blockStore =
        new BlockStore(
            objectKey, mockMetadataStore, mock(Metrics.class), mock(BlobStoreIndexCache.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we request the single byte range (0, 0)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 0));

    assertEquals(expected, missingRanges);
  }
}
