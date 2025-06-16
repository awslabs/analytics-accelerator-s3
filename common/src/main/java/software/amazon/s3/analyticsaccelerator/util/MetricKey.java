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
package software.amazon.s3.analyticsaccelerator.util;

/**
 * Enum representing different types of metrics that can be tracked in the system. Each metric is
 * identified by a unique name and can be used to track various performance and operational aspects.
 */
public enum MetricKey {
  /**
   * Tracks the memory usage in bytes. Used to monitor the amount of memory being consumed by the
   * blobstore.
   */
  MEMORY_USAGE("MemoryUsage"),

  /**
   * Tracks the number of successful cache hits. Incremented when requested block is found in the
   * cache.
   */
  CACHE_HIT("CacheHit"),

  /**
   * Tracks the number of cache misses. Incremented when requested block is not found in the cache
   */
  CACHE_MISS("CacheMiss"),

  /** Counts number of GET requests made. */
  GET_REQUEST_COUNT("GetRequestCount"),

  /** Counts number of HEAD requests made. */
  HEAD_REQUEST_COUNT("HeadRequestCount");

  /** The string name representation of the metric. */
  private final String name;

  /**
   * Constructs a new MetricKey with the specified name.
   *
   * @param name The string identifier for the metric
   */
  MetricKey(String name) {
    this.name = name;
  }

  /**
   * Returns the string name of the metric.
   *
   * @return The name of the metric
   */
  public String getName() {
    return name;
  }
}
