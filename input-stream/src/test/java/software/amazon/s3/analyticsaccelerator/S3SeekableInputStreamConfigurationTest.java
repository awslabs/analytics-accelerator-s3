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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.telemetry.TelemetryConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamConfigurationTest {
  private static final String TEST_PREFIX = "s3.connector";

  @Test
  void testDefaultBuilder() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder().build();
    assertEquals(PhysicalIOConfiguration.DEFAULT, configuration.getPhysicalIOConfiguration());
  }

  @Test
  void testDefault() {
    assertEquals(
        S3SeekableInputStreamConfiguration.DEFAULT,
        S3SeekableInputStreamConfiguration.builder().build());
  }

  @Test
  void testNulls() {
    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().physicalIOConfiguration(null).build());

    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().logicalIOConfiguration(null).build());

    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().telemetryConfiguration(null).build());
  }

  @Test
  void testNonDefaults() {
    PhysicalIOConfiguration physicalIOConfiguration = mock(PhysicalIOConfiguration.class);
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .physicalIOConfiguration(physicalIOConfiguration)
            .build();
    assertEquals(physicalIOConfiguration, configuration.getPhysicalIOConfiguration());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration = getConfiguration();
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(configuration);

    assertNotNull(streamConfiguration.getLogicalIOConfiguration());
    assertFalse(streamConfiguration.getLogicalIOConfiguration().isPrefetchPageIndexEnabled());
    assertEquals(20, streamConfiguration.getLogicalIOConfiguration().getPrefetchFileMetadataSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.getPrefetchingMode(),
        streamConfiguration.getLogicalIOConfiguration().getPrefetchingMode());

    assertNotNull(streamConfiguration.getPhysicalIOConfiguration());
    assertEquals(10, streamConfiguration.getPhysicalIOConfiguration().getMetadataStoreCapacity());
    assertEquals(20, streamConfiguration.getPhysicalIOConfiguration().getBlockSizeBytes());

    assertFalse(streamConfiguration.getTelemetryConfiguration().isStdOutEnabled());
    assertFalse(streamConfiguration.getTelemetryConfiguration().isLoggingEnabled());
    assertEquals("foo", streamConfiguration.getTelemetryConfiguration().getLoggingName());
    assertEquals("debug", streamConfiguration.getTelemetryConfiguration().getLoggingLevel());
  }

  @Test
  void testToString() {
    ConnectorConfiguration configuration = getConfiguration();
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(configuration);

    assertTrue(streamConfiguration.toString().contains("LogicalIO configuration"));
    assertTrue(streamConfiguration.toString().contains("PhysicalIO configuration"));
    assertTrue(streamConfiguration.toString().contains("Telemetry configuration"));
  }

  /**
   * Constructs {@link ConnectorConfiguration} object with test values.
   *
   * @return ConnectorConfiguration
   */
  public static ConnectorConfiguration getConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TEST_PREFIX + "." + LOGICAL_IO_PREFIX + ".prefetch.page.index.enabled", "false");
    properties.put(TEST_PREFIX + "." + LOGICAL_IO_PREFIX + ".prefetch.file.metadata.size", "20");
    properties.put("invalidPrefix.logicalio.predictive.prefetching.enabled", "false");
    properties.put(TEST_PREFIX + "." + PHYSICAL_IO_PREFIX + ".metadatastore.capacity", "10");
    properties.put(TEST_PREFIX + "." + PHYSICAL_IO_PREFIX + ".blocksizebytes", "20");
    properties.put(
        TEST_PREFIX + "." + TELEMETRY_PREFIX + "." + TelemetryConfiguration.STD_OUT_ENABLED_KEY,
        "false");
    properties.put(
        TEST_PREFIX + "." + TELEMETRY_PREFIX + "." + TelemetryConfiguration.LOGGING_ENABLED_KEY,
        "false");
    properties.put(
        TEST_PREFIX + "." + TELEMETRY_PREFIX + "." + TelemetryConfiguration.LOGGING_LEVEL_KEY,
        "debug");
    properties.put(
        TEST_PREFIX + "." + TELEMETRY_PREFIX + "." + TelemetryConfiguration.LOGGING_NAME_KEY,
        "foo");

    properties.put("invalidPrefix.physicalio.blobstore.capacity", "3");

    return new ConnectorConfiguration(properties, TEST_PREFIX);
  }
}
