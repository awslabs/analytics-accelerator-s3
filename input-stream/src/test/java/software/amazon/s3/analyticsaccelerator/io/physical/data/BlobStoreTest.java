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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.stats.MemoryUsageStats;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlobStoreTest {
  private static final String TEST_DATA = "test-data";
  private static final String ETAG = "random";
  private static final ObjectMetadata objectMetadata =
      ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();

  private static final ObjectKey objectKey =
      ObjectKey.builder().s3URI(S3URI.of("test", "test")).etag(ETAG).build();

  private BlobStore blobStore;

  @BeforeEach
  void setUp() throws IOException {
    ObjectClient objectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build());
    blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
  }

  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () -> new BlobStore(null, mock(Telemetry.class), mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new BlobStore(null, mock(Telemetry.class), mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new BlobStore(mock(ObjectClient.class), null, mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new BlobStore(mock(ObjectClient.class), mock(Telemetry.class), null));
  }

  @Test
  public void testGetReturnsReadableBlob() throws IOException {
    // When: a Blob is asked for
    Blob blob = blobStore.get(objectKey, objectMetadata, mock(StreamContext.class));

    // Then:
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);
    assertEquals(TEST_DATA, new String(b, StandardCharsets.UTF_8));
    assertEquals(1, blobStore.blobCount());
  }

  @Test
  void testEvictKey_ExistingKey() {
    // Setup
    blobStore.get(objectKey, objectMetadata, mock(StreamContext.class));

    // Test
    boolean result = blobStore.evictKey(objectKey);

    // Verify
    assertTrue(result, "Evicting existing key should return true");
    assertEquals(0, blobStore.blobCount(), "Cache should be empty after eviction");
  }

  @Test
  void testEvictKey_NonExistingKey() {
    // Test
    boolean result = blobStore.evictKey(objectKey);

    // Verify
    assertFalse(result, "Evicting non-existing key should return false");
    assertEquals(0, blobStore.blobCount(), "Cache should remain empty");
  }

  @Test
  void testBlobEvictionAndMemoryTracking() throws IOException {
    // Setup
    final int BLOB_STORE_CAPACITY = 2;
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().blobStoreCapacity(BLOB_STORE_CAPACITY).build();

    ObjectClient objectClient = new FakeObjectClient(TEST_DATA);
    BlobStore blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, config);

    // Reset memory stats before test
    MemoryUsageStats.resetStats();

    // Create multiple ObjectKeys
    ObjectKey key1 = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    ObjectKey key2 = ObjectKey.builder().s3URI(S3URI.of("test", "test2")).etag(ETAG).build();
    ObjectKey key3 = ObjectKey.builder().s3URI(S3URI.of("test", "test3")).etag(ETAG).build();

    // When: Add blobs up to capacity
    Blob blob1 = blobStore.get(key1, objectMetadata, mock(StreamContext.class));
    Blob blob2 = blobStore.get(key2, objectMetadata, mock(StreamContext.class));

    // Force data loading
    byte[] data = new byte[TEST_DATA.length()];
    blob1.read(data, 0, data.length, 0);
    blob2.read(data, 0, data.length, 0);

    // Record initial memory usage
    long initialMemoryUsage = MemoryUsageStats.getMemoryUsageAcrossBlobMap();

    // Then: Adding one more blob should trigger eviction
    Blob blob3 = blobStore.get(key3, objectMetadata, mock(StreamContext.class));
    blob3.read(data, 0, data.length, 0);

    // Verify
    assertEquals(
        BLOB_STORE_CAPACITY, blobStore.blobCount(), "BlobStore should maintain capacity limit");

    // Verify memory usage decreased after eviction
    long finalMemoryUsage = MemoryUsageStats.getMemoryUsageAcrossBlobMap();
    System.out.println("Memory final " + finalMemoryUsage);

    assertEquals(
        finalMemoryUsage, initialMemoryUsage, "Memory usage should decrease after eviction");

    // Verify evicted blob is no longer accessible
    assertFalse(blobStore.getBlobMap().containsKey(key1), "First blob should have been evicted");
    assertTrue(blobStore.getBlobMap().containsKey(key2), "Second blob should still be present");
    assertTrue(blobStore.getBlobMap().containsKey(key3), "Third blob should be present");
  }

  @Test
  void testBlobEvictionWithMultipleBlocks() throws IOException {
    // Setup with small capacity
    final int BLOB_STORE_CAPACITY = 1;
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().blobStoreCapacity(BLOB_STORE_CAPACITY).build();

    ObjectClient objectClient = new FakeObjectClient(TEST_DATA);
    BlobStore blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, config);

    MemoryUsageStats.resetStats();

    // Create a blob with multiple blocks
    ObjectKey key1 = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();

    Blob blob1 = blobStore.get(key1, objectMetadata, mock(StreamContext.class));

    // Force creation of multiple blocks by reading different ranges
    byte[] data = new byte[3];
    blob1.read(data, 0, 3, 0);
    blob1.read(data, 0, 3, 3);
    blob1.read(data, 0, 3, 6);

    long memoryWithOneBlob = MemoryUsageStats.getMemoryUsageAcrossBlobMap();

    // Add another blob to trigger eviction
    ObjectKey key2 = ObjectKey.builder().s3URI(S3URI.of("test", "test2")).etag(ETAG).build();

    Blob blob2 = blobStore.get(key2, objectMetadata, mock(StreamContext.class));
    blob2.read(data, 0, 3, 0);

    // Verify
    assertEquals(
        BLOB_STORE_CAPACITY, blobStore.blobCount(), "BlobStore should maintain capacity limit");

    long finalMemoryUsage = MemoryUsageStats.getMemoryUsageAcrossBlobMap();
    assertEquals(
        finalMemoryUsage,
        memoryWithOneBlob,
        "Memory usage should decrease after evicting blob with multiple blocks");

    assertFalse(blobStore.getBlobMap().containsKey(key1), "First blob should have been evicted");
    assertTrue(blobStore.getBlobMap().containsKey(key2), "Second blob should be present");
  }

  @Test
  void testBlobEvictionOrder() throws IOException {
    // Setup with capacity of 3
    final int BLOB_STORE_CAPACITY = 3;
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().blobStoreCapacity(BLOB_STORE_CAPACITY).build();

    ObjectClient objectClient = new FakeObjectClient(TEST_DATA);
    BlobStore blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, config);

    MemoryUsageStats.resetStats();

    // Create and access blobs in specific order
    List<ObjectKey> keys = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test" + i)).etag(ETAG).build();
      keys.add(key);

      Blob blob = blobStore.get(key, objectMetadata, mock(StreamContext.class));
      byte[] data = new byte[TEST_DATA.length()];
      blob.read(data, 0, data.length, 0);
    }

    // Verify eviction order (first in, first out)
    assertFalse(blobStore.getBlobMap().containsKey(keys.get(0)), "First blob should be evicted");
    assertTrue(blobStore.getBlobMap().containsKey(keys.get(1)), "Second blob should remain");
    assertTrue(blobStore.getBlobMap().containsKey(keys.get(2)), "Third blob should remain");
    assertTrue(blobStore.getBlobMap().containsKey(keys.get(3)), "Fourth blob should be present");
  }
}
