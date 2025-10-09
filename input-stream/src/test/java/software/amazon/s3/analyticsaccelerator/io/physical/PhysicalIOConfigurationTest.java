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
package software.amazon.s3.analyticsaccelerator.io.physical;

import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfigurationTest.PHYSICAL_IO_PREFIX;

import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfigurationTest;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

public class PhysicalIOConfigurationTest {

  @Test
  void testDefaultBuilder() {
    PhysicalIOConfiguration configuration = PhysicalIOConfiguration.builder().build();
    assertEquals(PhysicalIOConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().memoryCapacityBytes(10).targetRequestSize(20).build();
    assertEquals(10, configuration.getMemoryCapacityBytes());
    assertEquals(20, configuration.getTargetRequestSize());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration =
        S3SeekableInputStreamConfigurationTest.getConfiguration();
    ConnectorConfiguration mappedConfiguration = configuration.map(PHYSICAL_IO_PREFIX);

    PhysicalIOConfiguration physicalIOConfiguration =
        PhysicalIOConfiguration.fromConfiguration(mappedConfiguration);

    assertEquals(10, physicalIOConfiguration.getMetadataStoreCapacity());
    assertEquals(20, physicalIOConfiguration.getBlockSizeBytes());
    // This should be equal to default since Property Prefix is not s3.connector.
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getMemoryCapacityBytes(),
        physicalIOConfiguration.getMemoryCapacityBytes());
  }

  @Test
  void testToString() {
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().memoryCapacityBytes(10).targetRequestSize(20).build();

    assertEquals(
        configuration.toString(),
        "PhysicalIO configuration:\n"
            + "\tmemoryCapacityBytes: 10\n"
            + "\tmemoryCleanupFrequencyMilliseconds: 5000\n"
            + "\tcacheDataTimeoutMilliseconds: 1000\n"
            + "\tmetadataCacheTtlMilliseconds: 86400000\n"
            + "\tmetadataStoreCapacity: 5000\n"
            + "\tblockSizeBytes: 8388608\n"
            + "\treadAheadBytes: 65536\n"
            + "\tsequentialPrefetchBase: 2.0\n"
            + "\tsequentialPrefetchSpeed: 1.0\n"
            + "\tsequentialPrefetchMaxSize: 134217728\n"
            + "\tblockReadTimeout: 30000\n"
            + "\tblockReadRetryCount: 20\n"
            + "\tsmallObjectsPrefetchingEnabled: true\n"
            + "\tsmallObjectSizeThreshold: 8388608\n"
            + "\tthreadPoolSize: 96\n"
            + "\treadBufferSize: 131072\n"
            + "\ttargetRequestSize: 20\n"
            + "\trequestToleranceRatio: 1.4\n"
            + "\trequestCoalesce: true\n"
            + "\trequestCoalesceTolerance: 1048576\n");
  }
}
