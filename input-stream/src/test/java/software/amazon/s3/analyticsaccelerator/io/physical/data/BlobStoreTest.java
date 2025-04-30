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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.*;

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
    Metrics metrics = new Metrics();
    blobStore =
        new BlobStore(
            objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT, metrics);
  }

  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(ObjectClient.class),
                null,
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(ObjectClient.class), mock(Telemetry.class), null, mock(Metrics.class)));
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
  void testMemoryUsageTracking() throws IOException {
    // Given: Initial memory usage is 0
    assertEquals(0, blobStore.getMetrics().get(MetricKey.MEMORY_USAGE));

    // When: Reading data which causes memory allocation
    Blob blob = blobStore.get(objectKey, objectMetadata, mock(StreamContext.class));
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);

    // Then: Memory usage should be updated
    assertTrue(blobStore.getMetrics().get(MetricKey.MEMORY_USAGE) > 0);
    assertEquals(TEST_DATA.length(), blobStore.getMetrics().get(MetricKey.MEMORY_USAGE));
  }

  @Test
  void testCacheHitsAndMisses() throws IOException {
    // Initial verification: Cache starts empty
    assertEquals(
        0, blobStore.getMetrics().get(MetricKey.CACHE_HIT), "Cache should start with zero hits");
    assertEquals(
        0, blobStore.getMetrics().get(MetricKey.CACHE_MISS), "Cache should start with zero misses");

    // Create a blob and prepare for read
    Blob blob = blobStore.get(objectKey, objectMetadata, mock(StreamContext.class));
    byte[] b = new byte[TEST_DATA.length()];

    // First read operation
    blob.read(b, 0, b.length, 0);

    // Get metrics after first read
    long firstReadHits = blobStore.getMetrics().get(MetricKey.CACHE_HIT);
    long firstReadMisses = blobStore.getMetrics().get(MetricKey.CACHE_MISS);

    // Verify after first read
    assertTrue(firstReadHits > 0, "Should have some cache hits after first read");
    assertTrue(firstReadMisses > 0, "May have some cache misses during first read");

    // Second read operation of the same data
    blob.read(b, 0, b.length, 0);

    // Get metrics after second read
    long secondReadHits = blobStore.getMetrics().get(MetricKey.CACHE_HIT);
    long secondReadMisses = blobStore.getMetrics().get(MetricKey.CACHE_MISS);

    // Verify after second read
    assertTrue(secondReadHits > firstReadHits, "Should have more cache hits after second read");
    assertTrue(
        secondReadMisses >= firstReadMisses,
        "Should not have significantly more cache misses on second read");

    // Verify cache hit rate is improving
    double firstHitRate = (double) firstReadHits / (firstReadHits + firstReadMisses);
    double secondHitRate = (double) secondReadHits / (secondReadHits + secondReadMisses);
    assertTrue(secondHitRate > firstHitRate, "Cache hit rate should improve on subsequent reads");
  }

  @Test
  void testMemoryUsageAfterEviction() throws IOException {
    final int BLOB_STORE_CAPACITY = 2;
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().blobStoreCapacity(BLOB_STORE_CAPACITY).build();

    ObjectClient objectClient = new FakeObjectClient(TEST_DATA);
    Metrics metrics = new Metrics();
    BlobStore blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, config, metrics);

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
    long initialMemoryUsage = blobStore.getMetrics().get(MetricKey.MEMORY_USAGE);

    // Then: Adding one more blob should trigger eviction
    Blob blob3 = blobStore.get(key3, objectMetadata, mock(StreamContext.class));
    blob3.read(data, 0, data.length, 0);

    // Verify
    assertEquals(
        BLOB_STORE_CAPACITY, blobStore.blobCount(), "BlobStore should maintain capacity limit");

    // Verify memory usage decreased after eviction
    long finalMemoryUsage = blobStore.getMetrics().get(MetricKey.MEMORY_USAGE);
    System.out.println("Memory final " + finalMemoryUsage);

    assertEquals(
        initialMemoryUsage, finalMemoryUsage, "Memory usage should decrease after eviction");
  }

  @Test
  void testConcurrentMemoryUpdates() throws Exception {
    // Given: Multiple threads updating memory
    final int threadCount = 10;
    final int bytesPerThread = 100;
    final long expectedTotalMemory =
        (long) threadCount * bytesPerThread; // Cast to long before multiplication
    final CountDownLatch latch = new CountDownLatch(threadCount);

    // When: Concurrent memory updates
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      new Thread(
              () -> {
                try {
                  ObjectKey threadKey =
                      ObjectKey.builder()
                          .s3URI(S3URI.of("test", "test" + threadId))
                          .etag(ETAG)
                          .build();
                  ObjectMetadata threadMetadata =
                      ObjectMetadata.builder().contentLength(bytesPerThread).etag(ETAG).build();

                  Blob blob = blobStore.get(threadKey, threadMetadata, mock(StreamContext.class));
                  byte[] b = new byte[bytesPerThread];
                  blob.read(b, 0, b.length, 0);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                } finally {
                  latch.countDown();
                }
              })
          .start();
    }

    // Then: Wait for all threads and verify total memory
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(expectedTotalMemory, blobStore.getMetrics().get(MetricKey.MEMORY_USAGE));
  }

  @Test
  void testClose() {
    // Given: Create multiple blobs and force data loading
    ObjectKey key1 = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    ObjectKey key2 = ObjectKey.builder().s3URI(S3URI.of("test", "test2")).etag(ETAG).build();

    Blob blob1 = blobStore.get(key1, objectMetadata, mock(StreamContext.class));
    Blob blob2 = blobStore.get(key2, objectMetadata, mock(StreamContext.class));

    byte[] data = new byte[TEST_DATA.length()];
    try {

      for (int i = 0; i <= 10; i++) {
        blob1.read(data, 0, data.length, 0);
        blob2.read(data, 0, data.length, 0);
      }

    } catch (IOException e) {
      fail("Failed to read data from blobs", e);
    }

    // Record metrics before close
    long cacheHits = blobStore.getMetrics().get(MetricKey.CACHE_HIT);
    long cacheMisses = blobStore.getMetrics().get(MetricKey.CACHE_MISS);
    double expectedHitRate = MetricComputationUtils.computeCacheHitRate(cacheHits, cacheMisses);

    // When: Close the BlobStore
    blobStore.close();

    // Then: Verify the hit rate
    assertTrue(
        expectedHitRate >= 59.0 && expectedHitRate <= 60.0,
        "Hit rate should be between 59% and 60%, but was " + expectedHitRate);
  }
}
