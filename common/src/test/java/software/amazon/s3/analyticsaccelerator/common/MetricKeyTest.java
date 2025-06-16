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
package software.amazon.s3.analyticsaccelerator.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

public class MetricKeyTest {

  @Test
  public void testMetricKeyNames() {
    assertEquals("MemoryUsage", MetricKey.MEMORY_USAGE.getName());
    assertEquals("CacheHit", MetricKey.CACHE_HIT.getName());
    assertEquals("CacheMiss", MetricKey.CACHE_MISS.getName());
    assertEquals("GetRequestCount", MetricKey.GET_REQUEST_COUNT.getName());
    assertEquals("HeadRequestCount", MetricKey.HEAD_REQUEST_COUNT.getName());
  }

  @Test
  public void testEnumValues() {
    MetricKey[] values = MetricKey.values();
    assertEquals(5, values.length);
    assertEquals(MetricKey.MEMORY_USAGE, values[0]);
    assertEquals(MetricKey.CACHE_HIT, values[1]);
    assertEquals(MetricKey.CACHE_MISS, values[2]);
    assertEquals(MetricKey.GET_REQUEST_COUNT, values[3]);
    assertEquals(MetricKey.HEAD_REQUEST_COUNT, values[4]);
  }
}
