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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;

public class ObjectFormatSelectorTest {

  @ParameterizedTest
  @ValueSource(strings = {"key.parquet", "key.par"})
  public void testDefaultConfigParquetLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key), OpenStreamInformation.DEFAULT),
        ObjectFormat.PARQUET);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.pr3", "key.par3"})
  public void testConfiguredExtensionParquetLogicalIOSelection(String key) {
    // Build with configuration that accepts ".pr3" and "par3" are parquet file extensions.
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(
            LogicalIOConfiguration.builder().parquetFormatSelectorRegex("^.*.(pr3|par3)$").build());

    assertEquals(
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key), OpenStreamInformation.DEFAULT),
        ObjectFormat.PARQUET);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.csv", "key.CSV"})
  public void testDefaultConfigCSVLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        ObjectFormat.CSV,
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key), OpenStreamInformation.DEFAULT));
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.data", "key.csvdata"})
  public void testConfiguredExtensionCSVLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(
            LogicalIOConfiguration.builder().csvFormatSelectorRegex("^.*.(data|csvdata)$").build());

    assertEquals(
        ObjectFormat.CSV,
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key), OpenStreamInformation.DEFAULT));
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.jar", "key.txt", "key.parque", "key.pa", "key.cs"})
  public void testNonParquetLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key), OpenStreamInformation.DEFAULT),
        ObjectFormat.DEFAULT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.parquet", "key.par", "key.csv", "key.CSV"})
  public void testDefaultLogicalIOSelectionWithSequentialInputPolicy(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        objectFormatSelector.getObjectFormat(
            S3URI.of("bucket", key),
            OpenStreamInformation.builder().inputPolicy(InputPolicy.Sequential).build()),
        ObjectFormat.DEFAULT);
  }
}
