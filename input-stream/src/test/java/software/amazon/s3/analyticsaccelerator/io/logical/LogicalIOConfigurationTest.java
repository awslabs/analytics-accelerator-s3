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
package software.amazon.s3.analyticsaccelerator.io.logical;

import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfigurationTest.LOGICAL_IO_PREFIX;

import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfigurationTest;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

public class LogicalIOConfigurationTest {

  @Test
  void testDefaultBuilder() {
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();
    assertEquals(LogicalIOConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .prefetchFooterEnabled(true)
            .prefetchFilePageIndexSize(10)
            .build();
    assertTrue(configuration.isPrefetchPageIndexEnabled());
    assertEquals(10, configuration.getPrefetchFilePageIndexSize());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration =
        S3SeekableInputStreamConfigurationTest.getConfiguration();
    ConnectorConfiguration mappedConfiguration = configuration.map(LOGICAL_IO_PREFIX);
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.fromConfiguration(mappedConfiguration);

    assertFalse(logicalIOConfiguration.isPrefetchPageIndexEnabled());
    assertEquals(20, logicalIOConfiguration.getPrefetchFileMetadataSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.getPrefetchingMode(),
        logicalIOConfiguration.getPrefetchingMode());
    assertEquals(logicalIOConfiguration.getPrefetchingMode(), PrefetchMode.ROW_GROUP);
  }

  @Test
  void testToString() {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .prefetchFooterEnabled(true)
            .prefetchFilePageIndexSize(10)
            .build();

    assertEquals(
        configuration.toString(),
        "LogicalIO configuration:\n"
            + "\tprefetchFooterEnabled: true\n"
            + "\tprefetchPageIndexEnabled: true\n"
            + "\tuseFormatSpecificIO: true\n"
            + "\tprefetchFileMetadataSize: 32768\n"
            + "\tprefetchLargeFileMetadataSize: 1048576\n"
            + "\tprefetchFilePageIndexSize: 10\n"
            + "\tprefetchLargeFilePageIndexSize: 8388608\n"
            + "\tlargeFileSize: 1073741824\n"
            + "\tparquetMetadataStoreSize: 45\n"
            + "\tmaxColumnAccessCountStoreSize: 15\n"
            + "\tparquetFormatSelectorRegex: ^.*.(parquet|par)$\n"
            + "\tcsvFormatSelectorRegex: ^.*\\.(csv|CSV)$\n"
            + "\tjsonFormatSelectorRegex: ^.*\\.(json|JSON)$\n"
            + "\ttxtFormatSelectorRegex: ^.*\\.(txt|TXT)$\n"
            + "\tprefetchingMode: ROW_GROUP\n"
            + "\tpartitionSize: 134217728\n");
  }
}
