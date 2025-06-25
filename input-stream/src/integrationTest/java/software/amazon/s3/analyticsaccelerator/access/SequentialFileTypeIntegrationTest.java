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
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.util.*;

class SequentialFileTypeIntegrationTest extends IntegrationTestBase {

  private final StreamReadPattern streamReadPattern =
      StreamReadPattern.builder()
          .streamRead(new StreamRead(ONE_MB, 3 * ONE_MB))
          .streamRead(new StreamRead(6 * ONE_MB, 3 * ONE_MB))
          .streamRead(new StreamRead(10 * ONE_MB, 3 * ONE_MB))
          .build();

  // Tests that when the object is below the partition size, the whole object is downloaded.
  @ParameterizedTest
  @MethodSource("sequentialReader")
  void testingSequentialReadPatterBelowPartitionSize(S3ClientKind clientKind, S3Object s3Object)
      throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(clientKind, AALInputStreamConfigurationKind.DEFAULT)) {

      testAndCompareStreamReadPattern(
          clientKind, s3Object, streamReadPattern, s3AALClientStreamReader);

      // The physicalIO is reading in 8MB chunks. So the total gets should be Math.ceil(file size /
      // 8MB).
      // For example, for a 20MB, this will be 3, [0-8MB, 8MB - 16MB, 16MB - 20MB].
      int expectedGETCount = (int) Math.ceil((double) s3Object.getSize() / (8 * ONE_MB));

      // The sequential prefetcher should download the whole file,
      assertEquals(
          expectedGETCount,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));

      // TODO: This should be fixed with the new PhysicalIO, currently the cache hit metric is
      // inaccurate.
      //  assertEquals(expectedGETCount,
      // s3AALClientStreamReader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.CACHE_HIT));
    }
  }

  // Tests that when the object is above the partition size, the object is only uploaded up to the
  // paritition.
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testingSequentialReadPatternAbovePartitionSize(S3ClientKind clientKind) throws IOException {

    long partitionSize = 16 * ONE_MB;

    Map<String, String> configMap = new HashMap<>();
    configMap.put("logicalio.partition.size", String.valueOf(partitionSize));
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");

    StreamReadPattern streamReadPattern =
        StreamReadPattern.builder()
            .streamRead(new StreamRead(ONE_MB, 3 * ONE_MB))
            .streamRead(new StreamRead(6 * ONE_MB, 3 * ONE_MB))
            .streamRead(new StreamRead(10 * ONE_MB, 3 * ONE_MB))
            .streamRead(new StreamRead(18 * ONE_MB, ONE_MB))
            .build();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            clientKind, S3SeekableInputStreamConfiguration.fromConfiguration(config))) {

      testAndCompareStreamReadPattern(
          clientKind, S3Object.CSV_20MB, streamReadPattern, s3AALClientStreamReader);

      // The sequential prefetcher will do a total of 3 GETs. It should prefetch the first 16MB of
      // the file in 2
      // 8MB GETS, and then there should be a new GET for the read at 18MB.
      assertEquals(
          3,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));

      // TODO: This should be fixed with the new PhysicalIO, currently the cache hit metric is
      // inaccurate.
      //  assertEquals(2,
      // s3AALClientStreamReader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.CACHE_HIT));
    }
  }

  static Stream<Arguments> sequentialReader() {
    List<S3Object> s3Objects = new ArrayList<>();
    s3Objects.add(S3Object.CSV_20MB);
    s3Objects.add(S3Object.TXT_16MB);

    ArrayList<Arguments> results = new ArrayList<>();

    for (S3Object s3Object : s3Objects) {
      for (S3ClientKind s3ClientKind : getS3ClientKinds()) {
        results.add(Arguments.of(s3ClientKind, s3Object));
      }
    }

    return results.stream();
  }

  @Test
  void testSequentialFilesUseSequentialImplementation() {
    List<S3Object> sequentialFiles = S3Object.getSequentialS3Objects();

    ObjectFormatSelector formatSelector = new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    for (S3Object sequentialFile : sequentialFiles) {
      S3URI s3URI =
          sequentialFile.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());

      ObjectFormat selectedFormat =
          formatSelector.getObjectFormat(s3URI, OpenStreamInformation.DEFAULT);

      assertEquals(
          ObjectFormat.SEQUENTIAL,
          selectedFormat,
          String.format(
              "File %s should be handled by Sequential implementation", sequentialFile.getName()));
    }
  }

  @Test
  void testInvalidPartitionSizeConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("logicalio.partition.size", "-1"); // negative partition size

    ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration(configMap, "");

    assertThrows(
        IllegalArgumentException.class,
        () -> S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));
  }
}
