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
package software.amazon.s3.analyticsaccelerator.stats;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the MemoryUsageStats class. */
class MemoryUsageStatsTest {

  @BeforeEach
  void setUp() {
    MemoryUsageStats.resetStats();
  }

  @Test
  void testInitialValue() {
    assertEquals(
        0, MemoryUsageStats.getMemoryUsageAcrossBlobMap(), "Initial memory usage should be 0");
  }

  @Test
  void testPositiveMemoryRecording() {
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(1000);
    assertEquals(
        1000,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Memory usage should be 1000 after adding 1000 bytes");
  }

  @Test
  void testNegativeMemoryRecording() {
    // First add some memory
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(1000);
    // Then subtract some
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(-500);
    assertEquals(
        500,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Memory usage should be 500 after subtracting 500 from 1000");
  }

  @Test
  void testResetStats() {
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(1000);
    MemoryUsageStats.resetStats();
    assertEquals(
        0, MemoryUsageStats.getMemoryUsageAcrossBlobMap(), "Memory usage should be 0 after reset");
  }

  @Test
  void testMultipleOperations() {
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(1000);
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(2000);
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(-500);
    assertEquals(
        2500,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Memory usage should be 2500 after multiple operations");
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    int threadCount = 10;
    int operationsPerThread = 1000;
    long bytesPerOperation = 100L;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    List<Future<?>> futures = new ArrayList<>();

    // Create multiple threads that record memory usage
    for (int i = 0; i < threadCount; i++) {
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  for (int j = 0; j < operationsPerThread; j++) {
                    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(bytesPerOperation);
                  }
                } finally {
                  latch.countDown();
                }
              });
      futures.add(future);
    }

    // Wait for all threads to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Timeout waiting for threads");

    // Check for any exceptions in the futures
    for (Future<?> future : futures) {
      try {
        future.get(1, TimeUnit.SECONDS); // timeout per future
      } catch (ExecutionException e) {
        fail("Concurrent execution failed: " + e.getCause().getMessage());
      } catch (TimeoutException e) {
        fail("Future timed out: " + e.getMessage());
      }
    }

    executor.shutdown();
    assertTrue(
        executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate properly");

    // Verify results - using long multiplication to avoid overflow
    long expectedMemoryUsage = (long) threadCount * (long) operationsPerThread * bytesPerOperation;
    assertEquals(
        expectedMemoryUsage,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Memory usage should match expected value after concurrent operations");
  }

  @Test
  void testLargeMemoryValues() {
    long largeValue = Integer.MAX_VALUE * 2L;
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(largeValue);
    assertEquals(
        largeValue,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Should handle large memory values correctly");
  }

  @Test
  void testNegativeToZero() {
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(1000);
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(-2000);
    assertTrue(
        MemoryUsageStats.getMemoryUsageAcrossBlobMap() < 0,
        "Memory usage can go negative when subtracting more than added");
  }

  @Test
  void testZeroMemoryRecording() {
    MemoryUsageStats.recordMemoryUsageAcrossBlobMap(0);
    assertEquals(
        0,
        MemoryUsageStats.getMemoryUsageAcrossBlobMap(),
        "Recording zero bytes should not change memory usage");
  }
}
