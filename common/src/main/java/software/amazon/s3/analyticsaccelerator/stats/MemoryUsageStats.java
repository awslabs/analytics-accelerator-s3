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
 * Tracks memory usage statistics across all blob maps in the system. This class provides
 * thread-safe methods to record, retrieve, and reset memory usage statistics.
 */
public class MemoryUsageStats {
  /** Atomic counter for tracking total memory usage across all blob maps. */
  private static final AtomicLong memoryUsageAcrossBlobMap = new AtomicLong(0);

  /**
   * Records changes in memory usage by incrementing or decrementing the total. This method is
   * thread-safe and can handle concurrent updates.
   *
   * @param bytes The number of bytes to add (positive) or subtract (negative) from the total memory
   *     usage
   */
  public static void recordMemoryUsageAcrossBlobMap(long bytes) {
    memoryUsageAcrossBlobMap.addAndGet(bytes);
  }

  /**
   * Retrieves the current total memory usage across all blob maps. This method provides a
   * thread-safe way to get the current memory usage.
   *
   * @return The current total memory usage in bytes
   */
  public static long getMemoryUsageAcrossBlobMap() {
    return memoryUsageAcrossBlobMap.get();
  }

  /**
   * Resets the memory usage statistics to zero. This method should be used with caution as it will
   * lose all current memory tracking information.
   */
  public static void resetStats() {
    memoryUsageAcrossBlobMap.set(0);
  }
}
