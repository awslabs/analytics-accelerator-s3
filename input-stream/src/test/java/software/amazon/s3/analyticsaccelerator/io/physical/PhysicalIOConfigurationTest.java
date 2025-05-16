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
        PhysicalIOConfiguration.builder().blobStoreCapacity(10).partSizeBytes(20).build();
    assertEquals(10, configuration.getBlobStoreCapacity());
    assertEquals(20, configuration.getPartSizeBytes());
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
        PhysicalIOConfiguration.DEFAULT.getBlobStoreCapacity(),
        physicalIOConfiguration.getBlobStoreCapacity());
  }

  @Test
  void testToString() {
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().blobStoreCapacity(10).partSizeBytes(20).build();

    assertEquals(
        configuration.toString(),
        "PhysicalIO configuration:\n"
            + "\tblobStoreCapacity: 10\n"
            + "\tmetadataStoreCapacity: 50\n"
            + "\tblockSizeBytes: 8388608\n"
            + "\treadAheadBytes: 65536\n"
            + "\tmaxRangeSizeBytes: 8388608\n"
            + "\tpartSizeBytes: 20\n"
            + "\tsequentialPrefetchBase: 2.0\n"
            + "\tsequentialPrefetchSpeed: 1.0\n"
            + "\tblockReadTimeout: 30000\n"
            + "\tblockReadRetryCount: 20\n"
            + "\tenableTailMetadataCaching: false\n");
  }
}
