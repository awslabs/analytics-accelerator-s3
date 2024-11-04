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
package software.amazon.s3.dataaccelerator.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class MetricTest {
  @Test
  void testCreateMetric() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    assertEquals("S3.GET", metric.getName());
    assertEquals(1, metric.getAttributes().size());
    assertEquals("Bar", metric.getAttributes().get("Foo").getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().clear();
        });
  }

  @Test
  void testCreateMetricBuilderWithNulls() {
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name(null).attribute("Foo", "Bar").build());
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name("S3.GET").attribute(null, "Bar").build());
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name("S3.GET").attribute("Foo", null).build());
    assertThrows(
        NullPointerException.class, () -> Metric.builder().name("S3.GET").attribute(null).build());
  }

  @Test
  void testCreateMetricWithAttributes() {
    Metric metric =
        Metric.builder()
            .name("S3.GET")
            .attribute(Attribute.of("s3.bucket", "bucket"))
            .attribute(Attribute.of("s3.key", "key"))
            .build();
    assertEquals("S3.GET", metric.getName());

    assertEquals(2, metric.getAttributes().size());
    assertEquals("bucket", metric.getAttributes().get("s3.bucket").getValue());
    assertEquals("key", metric.getAttributes().get("s3.key").getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().clear();
        });
  }

  @Test
  void testEqualsAndHashcode() {
    Metric metric1 = Metric.builder().name("S3.GET").attribute("foo", "bar").build();
    Metric metric2 = Metric.builder().name("S3.GET").attribute("foo", "bar").build();
    Metric metric3 = Metric.builder().name("S3.GET").attribute("foo1", "bar1").build();
    assertEquals(metric1, metric2);
    assertEquals(metric1.hashCode(), metric2.hashCode());

    assertNotEquals(metric3, metric2);
    assertNotEquals(metric3.hashCode(), metric2.hashCode());
  }

  @Test
  void testCreateMetricWithDuplicateAttributes() {
    Metric.MetricBuilder builder = Metric.builder().name("S3.GET").attribute("foo", "bar");
    assertThrows(IllegalArgumentException.class, () -> builder.attribute("foo", "bar"));
  }
}
