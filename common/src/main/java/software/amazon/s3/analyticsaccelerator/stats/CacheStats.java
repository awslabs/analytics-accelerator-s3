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

import java.util.concurrent.atomic.AtomicLong;

/**
 * CacheStats provides methods to record and retrieve cache hit and miss statistics. This class uses
 * static methods and fields to maintain global statistics across the application.
 */
public class CacheStats {
  /** Atomic counter for cache hits. */
  private static final AtomicLong cacheHits = new AtomicLong(0);

  /** Atomic counter for cache misses. */
  private static final AtomicLong cacheMisses = new AtomicLong(0);

  /** Records a cache hit by incrementing the hit counter. */
  public static void recordHit() {
    cacheHits.incrementAndGet();
  }

  /** Records a cache miss by incrementing the miss counter. */
  public static void recordMiss() {
    cacheMisses.incrementAndGet();
  }

  /**
   * Retrieves the total number of cache hits.
   *
   * @return the number of cache hits
   */
  public static long getHits() {
    return cacheHits.get();
  }

  /**
   * Retrieves the total number of cache misses.
   *
   * @return the number of cache misses
   */
  public static long getMisses() {
    return cacheMisses.get();
  }

  /**
   * Calculates and returns the cache hit rate.
   *
   * @return the hit rate as a double between 0.0 and 1.0
   */
  public static double getHitRate() {
    long hits = cacheHits.get();
    long total = hits + cacheMisses.get();
    return total == 0 ? 0 : (double) hits / total;
  }

  /** Resets both hit-and-miss counters to zero. */
  public static void resetStats() {
    cacheHits.set(0);
    cacheMisses.set(0);
  }
}
