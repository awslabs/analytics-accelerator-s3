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

/** Unit tests for the CacheStats class. */
public class CacheStatsTest {

  /** Setup method */
  @BeforeEach
  public void setUp() {
    CacheStats.resetStats();
  }

  @Test
  public void testInitialValues() {
    assertEquals(0, CacheStats.getHits(), "Initial hits should be 0");
    assertEquals(0, CacheStats.getMisses(), "Initial misses should be 0");
    assertEquals(0.0, CacheStats.getHitRate(), 0.001, "Initial hit rate should be 0");
  }

  @Test
  public void testRecordHit() {
    CacheStats.recordHit();
    assertEquals(1, CacheStats.getHits(), "Hit count should be 1");
    assertEquals(0, CacheStats.getMisses(), "Miss count should remain 0");
  }

  @Test
  public void testRecordMiss() {
    CacheStats.recordMiss();
    assertEquals(0, CacheStats.getHits(), "Hit count should remain 0");
    assertEquals(1, CacheStats.getMisses(), "Miss count should be 1");
  }

  @Test
  public void testHitRate() {
    // Record 3 hits and 1 miss
    CacheStats.recordHit();
    CacheStats.recordHit();
    CacheStats.recordHit();
    CacheStats.recordMiss();

    assertEquals(0.75, CacheStats.getHitRate(), 0.001, "Hit rate should be 0.75");
  }

  @Test
  public void testResetStats() {
    // Record some hits and misses
    CacheStats.recordHit();
    CacheStats.recordMiss();

    // Reset stats
    CacheStats.resetStats();

    assertEquals(0, CacheStats.getHits(), "Hits should be reset to 0");
    assertEquals(0, CacheStats.getMisses(), "Misses should be reset to 0");
    assertEquals(0.0, CacheStats.getHitRate(), 0.001, "Hit rate should be reset to 0");
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    int threadCount = 10;
    int operationsPerThread = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    List<Future<?>> futures = new ArrayList<>();

    // Create multiple threads that record hits and misses
    for (int i = 0; i < threadCount; i++) {
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  for (int j = 0; j < operationsPerThread; j++) {
                    if (j % 2 == 0) {
                      CacheStats.recordHit();
                    } else {
                      CacheStats.recordMiss();
                    }
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
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate");

    // Verify results
    long expectedHits = (threadCount * operationsPerThread) / 2;
    long expectedMisses = (threadCount * operationsPerThread) / 2;
    assertEquals(expectedHits, CacheStats.getHits(), "Incorrect number of hits");
    assertEquals(expectedMisses, CacheStats.getMisses(), "Incorrect number of misses");
    assertEquals(0.5, CacheStats.getHitRate(), 0.001, "Incorrect hit rate");
  }

  @Test
  public void testZeroTotalOperations() {
    assertEquals(0.0, CacheStats.getHitRate(), 0.001, "Hit rate should be 0 when no operations");
  }

  @Test
  public void testAllHits() {
    for (int i = 0; i < 100; i++) {
      CacheStats.recordHit();
    }
    assertEquals(1.0, CacheStats.getHitRate(), 0.001, "Hit rate should be 1.0 for all hits");
  }

  @Test
  public void testAllMisses() {
    for (int i = 0; i < 100; i++) {
      CacheStats.recordMiss();
    }
    assertEquals(0.0, CacheStats.getHitRate(), 0.001, "Hit rate should be 0.0 for all misses");
  }
}
