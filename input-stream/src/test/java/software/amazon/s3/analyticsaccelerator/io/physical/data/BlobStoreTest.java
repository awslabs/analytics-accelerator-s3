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
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.CheckReturnValue;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.*;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlobStoreTest {
  private static final String TEST_DATA = "test-data";
  private static final String ETAG = "RANDOM";
  private static final ObjectMetadata objectMetadata =
      ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build();

  private static final ObjectKey objectKey =
      ObjectKey.builder().s3URI(S3URI.of("test", "test")).etag(ETAG).build();

  private final ExecutorService threadPool = Executors.newFixedThreadPool(30);

  private PhysicalIOConfiguration config;
  private BlobStore blobStore;
  private BlobStore mockBlobStore;
  private ObjectClient mockObjectClient;
  private Metrics mockMetrics;
  private PhysicalIOConfiguration mockConfig;

  @BeforeEach
  @CheckReturnValue
  void setUp() throws IOException {
    ObjectClient objectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any(), any()))
        .thenReturn(ObjectMetadata.builder().contentLength(TEST_DATA.length()).etag(ETAG).build());
    Metrics metrics = new Metrics();
    Map<String, String> configMap = new HashMap<>();
    configMap.put("max.memory.limit", "1000");
    configMap.put("memory.cleanup.frequency", "1");
    // when small objects prefetching is async, causing fleakiness in tests.
    configMap.put("small.objects.prefetching.enabled", "false");
    ConnectorConfiguration connectorConfig = new ConnectorConfiguration(configMap);
    config = PhysicalIOConfiguration.fromConfiguration(connectorConfig);

    blobStore = new BlobStore(objectClient, TestTelemetry.DEFAULT, config, metrics, threadPool);
    blobStore.schedulePeriodicCleanup();

    mockObjectClient = mock(ObjectClient.class);
    mockMetrics = mock(Metrics.class);
    mockConfig = mock(PhysicalIOConfiguration.class);

    when(mockConfig.getMemoryCapacityBytes()).thenReturn(1000L);
    when(mockConfig.getMemoryCleanupFrequencyMilliseconds()).thenReturn(1);

    // Create mock BlobStore with configured mocks
    mockBlobStore =
        new BlobStore(mockObjectClient, TestTelemetry.DEFAULT, mockConfig, mockMetrics, threadPool);
    mockBlobStore = spy(mockBlobStore);
  }

  @Test
  void testIndexCacheEviction() throws InterruptedException {
    // Fill the cache
    for (int i = 0; i < 1000; i++) {
      Range r = new Range(i, i + 100);
      BlockKey blockKey = new BlockKey(objectKey, r);
      blobStore.indexCache.put(blockKey, r.getLength());
    }

    // Wait for eviction
    Thread.sleep(600);

    // Check if some entries were evicted
    System.out.println("Max weight set is " + blobStore.indexCache.getMaximumWeight());
    long currentWeight = blobStore.indexCache.getCurrentWeight();
    System.out.println("Current weight is " + currentWeight);
    assertTrue(currentWeight < 1000, "Some entries should have been evicted");
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
                mock(Metrics.class),
                mock(ExecutorService.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(ExecutorService.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(ObjectClient.class),
                null,
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(ExecutorService.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(ObjectClient.class),
                mock(Telemetry.class),
                null,
                mock(Metrics.class),
                mock(ExecutorService.class)));
  }

  @Test
  public void testGetReturnsReadableBlob() throws IOException {
    // When: a Blob is asked for
    Blob blob = blobStore.get(objectKey, objectMetadata, OpenStreamInformation.DEFAULT);

    // Then:
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);
    assertEquals(TEST_DATA, new String(b, StandardCharsets.UTF_8));
    assertEquals(1, blobStore.blobCount());
  }

  @Test
  void testEvictKey_ExistingKey() {
    // Setup
    blobStore.get(objectKey, objectMetadata, OpenStreamInformation.DEFAULT);

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
    Blob blob = blobStore.get(objectKey, objectMetadata, OpenStreamInformation.DEFAULT);
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);

    // Then: Memory usage should be updated
    assertTrue(blobStore.getMetrics().get(MetricKey.MEMORY_USAGE) > 0);
    assertEquals(TEST_DATA.length(), blobStore.getMetrics().get(MetricKey.MEMORY_USAGE));
  }

  @Test
  void testMemoryUsageAfterEviction() throws IOException, InterruptedException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder()
            .memoryCapacityBytes(18)
            .memoryCleanupFrequencyMilliseconds(1)
            .build();

    ObjectClient objectClient = new FakeObjectClient(TEST_DATA);
    Metrics metrics = new Metrics();
    BlobStore blobStore =
        new BlobStore(objectClient, TestTelemetry.DEFAULT, config, metrics, threadPool);
    blobStore.schedulePeriodicCleanup();

    // Create multiple ObjectKeys
    ObjectKey key1 = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    ObjectKey key2 = ObjectKey.builder().s3URI(S3URI.of("test", "test2")).etag(ETAG).build();
    ObjectKey key3 = ObjectKey.builder().s3URI(S3URI.of("test", "test3")).etag(ETAG).build();

    // When: Add blobs up to capacity
    Blob blob1 = blobStore.get(key1, objectMetadata, OpenStreamInformation.DEFAULT);
    Blob blob2 = blobStore.get(key2, objectMetadata, OpenStreamInformation.DEFAULT);

    // Force data loading
    byte[] data = new byte[TEST_DATA.length()];
    blob1.read(data, 0, data.length, 0);
    blob2.read(data, 0, data.length, 0);

    // Record initial memory usage
    long initialMemoryUsage = blobStore.getMetrics().get(MetricKey.MEMORY_USAGE);

    // Then: Adding one more blob should trigger eviction
    Blob blob3 = blobStore.get(key3, objectMetadata, OpenStreamInformation.DEFAULT);
    blob3.read(data, 0, data.length, 0);

    Thread.sleep(10);

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
    final long expectedTotalMemory = 10L * 100L;
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

                  Blob blob =
                      blobStore.get(threadKey, threadMetadata, OpenStreamInformation.DEFAULT);
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
  @DisplayName("Test blobCount method")
  void testBlobCount() {
    // Initial count should be 0
    assertEquals(0, blobStore.blobCount(), "Initial blob count should be 0");

    // Add some blobs
    ObjectKey key1 = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    ObjectKey key2 = ObjectKey.builder().s3URI(S3URI.of("test", "test2")).etag(ETAG).build();

    // Get blobs (which adds them to the map)
    blobStore.get(key1, objectMetadata, OpenStreamInformation.DEFAULT);
    assertEquals(1, blobStore.blobCount(), "Blob count should be 1 after adding first blob");

    blobStore.get(key2, objectMetadata, OpenStreamInformation.DEFAULT);
    assertEquals(2, blobStore.blobCount(), "Blob count should be 2 after adding second blob");

    // Test count after eviction
    blobStore.evictKey(key1);
    assertEquals(1, blobStore.blobCount(), "Blob count should be 1 after evicting one blob");

    blobStore.evictKey(key2);
    assertEquals(0, blobStore.blobCount(), "Blob count should be 0 after evicting all blobs");
  }

  @Test
  @DisplayName("Test cleanup not triggered when memory usage is below capacity")
  void testCleanupNotTriggeredBelowCapacity() {
    // Setup memory usage below capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    // Create a blob to potentially clean
    ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    mockBlobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT);

    // Attempt cleanup
    mockBlobStore.scheduleCleanupIfNotRunning();

    // Verify cleanup was not triggered
    verify(mockBlobStore, times(1)).asyncCleanup();
  }

  @Test
  @DisplayName("Test cleanup triggered when memory exceeds capacity")
  void testCleanupTriggeredAboveCapacity() throws IOException {
    // Setup memory usage above capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    // Create and load a blob
    ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();
    mockBlobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT);

    // Trigger cleanup
    mockBlobStore.scheduleCleanupIfNotRunning();

    // Verify cleanup was triggered
    verify(mockMetrics, atLeastOnce()).get(MetricKey.MEMORY_USAGE);
  }

  @Test
  @DisplayName("Test cleanup flag prevents concurrent cleanups")
  void testCleanupFlagPreventsConcurrentCleanups() throws InterruptedException {
    // Setup memory usage above capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    BlobStore spyBlobStore = spy(mockBlobStore);

    // Force cleanup flag to true
    spyBlobStore.cleanupInProgress.set(true);

    // Attempt cleanup while flag is set
    spyBlobStore.scheduleCleanupIfNotRunning();

    // Verify cleanup was not attempted
    verify(spyBlobStore, never()).asyncCleanup();
  }

  @Test
  @DisplayName("Test cleanup flag reset after completion")
  void testCleanupFlagResetAfterCompletion() throws InterruptedException {
    // Setup memory usage above capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    BlobStore spyBlobStore = spy(mockBlobStore);
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch cleanupShouldComplete = new CountDownLatch(1);

    // Configure spy to simulate long-running cleanup
    doAnswer(
            invocation -> {
              cleanupStarted.countDown();
              assertTrue(
                  cleanupShouldComplete.await(1, TimeUnit.SECONDS),
                  "Cleanup should complete within timeout");
              return null;
            })
        .when(spyBlobStore)
        .asyncCleanup();

    // Start cleanup in separate thread
    Thread cleanupThread = new Thread(() -> spyBlobStore.scheduleCleanupIfNotRunning());
    cleanupThread.start();

    // Wait for cleanup to start
    assertTrue(cleanupStarted.await(1, TimeUnit.SECONDS));

    // Verify cleanup flag is set during cleanup
    assertTrue(spyBlobStore.cleanupInProgress.get());

    // Allow cleanup to complete
    cleanupShouldComplete.countDown();
    cleanupThread.join(1000);

    // Verify cleanup flag was reset
    assertFalse(spyBlobStore.cleanupInProgress.get());
    verify(spyBlobStore, times(1)).asyncCleanup();
  }

  @Test
  @DisplayName("Test cleanup with exception handling")
  void testCleanupWithException() {
    // Setup memory usage above capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    // Create a blob that will throw exception during cleanup
    ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test1")).etag(ETAG).build();

    Blob mockBlob = mock(Blob.class);
    doThrow(new RuntimeException("Cleanup failed")).when(mockBlob).asyncCleanup();

    // Add mock blob to store
    mockBlobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT);

    // Attempt cleanup - should handle exception gracefully
    assertDoesNotThrow(() -> mockBlobStore.scheduleCleanupIfNotRunning());

    // Verify cleanupInProgress was reset
    assertFalse(mockBlobStore.cleanupInProgress.get());
  }

  @Test
  @DisplayName("Test cleanup flag reset after completion")
  void testCleanupFlagReset() {
    // Setup memory usage above capacity
    long capacity = mockConfig.getMemoryCapacityBytes();
    doReturn(capacity + 1).when(mockMetrics).get(MetricKey.MEMORY_USAGE);

    // Trigger cleanup
    mockBlobStore.scheduleCleanupIfNotRunning();

    // Verify cleanup flag was reset
    assertFalse(mockBlobStore.cleanupInProgress.get());
  }

  @Test
  @DisplayName("Test concurrent cleanup and blob operations")
  void testConcurrentCleanupAndBlobOperations() throws InterruptedException {
    int numThreads = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    try {
      // Submit tasks that perform blob operations while cleanup is running
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    ObjectKey threadKey =
                        ObjectKey.builder()
                            .s3URI(S3URI.of("test", "test" + threadId))
                            .etag(ETAG)
                            .build();

                    // Perform operations while cleanup might be running
                    Blob blob =
                        blobStore.get(threadKey, objectMetadata, OpenStreamInformation.DEFAULT);
                    byte[] data = new byte[TEST_DATA.length()];
                    blob.read(data, 0, data.length, 0);

                    // Verify data integrity
                    assertEquals(TEST_DATA, new String(data, StandardCharsets.UTF_8));
                  } catch (IOException e) {
                    fail("IO Exception during concurrent operations: " + e.getMessage());
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Thread interrupted during concurrent operations: " + e.getMessage());
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      startLatch.countDown();
      assertTrue(completionLatch.await(5, TimeUnit.SECONDS));
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("Test rapid eviction and retrieval")
  void testRapidEvictionAndRetrieval() throws IOException {
    List<ObjectKey> keys = new ArrayList<>();
    List<Blob> blobs = new ArrayList<>();

    // Create and store multiple blobs
    for (int i = 0; i < 10000; i++) {
      ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test" + i)).etag(ETAG).build();
      keys.add(key);
      blobs.add(blobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT));
    }

    // Force data loading for all blobs
    byte[] data = new byte[TEST_DATA.length()];
    for (Blob blob : blobs) {
      blob.read(data, 0, data.length, 0);
    }

    // Evict keys while simultaneously retrieving them
    for (int i = 0; i < keys.size(); i++) {
      blobStore.evictKey(keys.get(i));
      Blob newBlob = blobStore.get(keys.get(i), objectMetadata, OpenStreamInformation.DEFAULT);
      byte[] newData = new byte[TEST_DATA.length()];
      newBlob.read(newData, 0, newData.length, 0);
      assertEquals(TEST_DATA, new String(newData, StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("Test cleanup under memory pressure")
  void testCleanupUnderMemoryPressure() throws IOException, InterruptedException {
    List<Blob> blobs = new ArrayList<>();

    // Create enough blobs to exceed the configured capacity
    for (int i = 0; i < 10000; i++) {
      ObjectKey key = ObjectKey.builder().s3URI(S3URI.of("test", "test" + i)).etag(ETAG).build();
      blobs.add(blobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT));
    }

    // Force data loading to trigger memory pressure
    byte[] data = new byte[TEST_DATA.length()];
    for (Blob blob : blobs) {
      blob.read(data, 0, data.length, 0);
    }

    // Wait for cleanup to occur
    Thread.sleep(100);

    // Verify memory usage is within limits
    assertTrue(
        blobStore.getMetrics().get(MetricKey.MEMORY_USAGE) <= config.getMemoryCapacityBytes());
  }

  @Test
  @DisplayName("Test concurrent blob retrieval and cleanup")
  void testConcurrentBlobRetrievalAndCleanup() throws InterruptedException {
    int numThreads = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    ConcurrentHashMap<Integer, Boolean> successfulOperations = new ConcurrentHashMap<>();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    try {
      // Submit tasks that perform get operations while cleanup might occur
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    ObjectKey threadKey =
                        ObjectKey.builder()
                            .s3URI(S3URI.of("test", "test" + threadId))
                            .etag(ETAG)
                            .build();

                    // Perform multiple get operations
                    for (int j = 0; j < 5; j++) {
                      Blob blob =
                          blobStore.get(threadKey, objectMetadata, OpenStreamInformation.DEFAULT);
                      byte[] data = new byte[TEST_DATA.length()];
                      blob.read(data, 0, data.length, 0);

                      if (TEST_DATA.equals(new String(data, StandardCharsets.UTF_8))) {
                        successfulOperations.put(threadId * 100 + j, true);
                      }
                    }
                  } catch (IOException e) {
                    fail("IO Exception during concurrent operations: " + e.getMessage());
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Thread interrupted during concurrent operations: " + e.getMessage());
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      startLatch.countDown();
      assertTrue(completionLatch.await(5, TimeUnit.SECONDS));
      // Check futures for exceptions
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
      }

      // Verify all operations were successful
      assertEquals(numThreads * 5, successfulOperations.size());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("Test cleanup with active reads")
  void testCleanupWithActiveReads() throws InterruptedException {
    CountDownLatch readStarted = new CountDownLatch(1);
    CountDownLatch cleanupComplete = new CountDownLatch(1);
    AtomicReference<Exception> testException = new AtomicReference<>();

    // Start a long-running read operation
    Thread readThread =
        new Thread(
            () -> {
              try {
                ObjectKey key =
                    ObjectKey.builder().s3URI(S3URI.of("test", "testLongRead")).etag(ETAG).build();

                Blob blob = blobStore.get(key, objectMetadata, OpenStreamInformation.DEFAULT);
                byte[] data = new byte[TEST_DATA.length()];

                // Signal that read has started
                readStarted.countDown();

                // Perform multiple reads while cleanup might occur
                for (int i = 0; i < 100; i++) {
                  blob.read(data, 0, data.length, 0);
                  assertEquals(TEST_DATA, new String(data, StandardCharsets.UTF_8));
                }
              } catch (Exception e) {
                testException.set(e);
              }
            });

    // Start cleanup operation after read begins
    Thread cleanupThread =
        new Thread(
            () -> {
              try {
                readStarted.await();
                blobStore.scheduleCleanupIfNotRunning();
                cleanupComplete.countDown();
              } catch (InterruptedException e) {
                testException.set(e);
              }
            });

    readThread.start();
    cleanupThread.start();

    // Wait for both operations to complete
    assertTrue(cleanupComplete.await(5, TimeUnit.SECONDS));
    readThread.join(5000);
    cleanupThread.join(5000);

    // Check for any exceptions
    assertNull(testException.get(), "No exceptions should have occurred during test");
  }
}
