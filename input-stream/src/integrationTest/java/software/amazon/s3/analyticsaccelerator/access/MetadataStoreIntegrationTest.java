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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

public class MetadataStoreIntegrationTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("metadataStoreTtlTests")
  void testMetadataStoreTtlBehavior(
      S3ClientKind s3ClientKind, String ttlValue, int expectedHeadRequests, boolean shouldSleep)
      throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("physicalio.metadatastore.ttl", ttlValue);
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1MB, OpenStreamInformation.DEFAULT);

      if (shouldSleep) {
        int ttlMs = Integer.parseInt(ttlValue);
        Thread.sleep(ttlMs + 100); // Wait for TTL + overhead
      }

      s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1MB, OpenStreamInformation.DEFAULT);
      s3AALClientStreamReader.createReadStream(S3Object.RANDOM_1MB, OpenStreamInformation.DEFAULT);

      assertEquals(
          expectedHeadRequests,
          (int)
              s3AALClientStreamReader
                  .getS3SeekableInputStreamFactory()
                  .getMetrics()
                  .get(MetricKey.HEAD_REQUEST_COUNT),
          "HEAD request count should match expected for TTL=" + ttlValue);
    }
  }

  static Stream<Arguments> metadataStoreTtlTests() {
    List<Arguments> testCases = new ArrayList<>();

    for (S3ClientKind clientKind : getS3ClientKinds()) {
      // Zero TTL - no caching, each stream makes HEAD request
      testCases.add(Arguments.of(clientKind, "0", 3, false));

      // Non-zero TTL - caching enabled, single HEAD request
      testCases.add(Arguments.of(clientKind, "5000", 1, false));

      // TTL expiry - cache refresh after expiration
      testCases.add(Arguments.of(clientKind, "50", 2, true));

      // TTL expiry with longer TTL - cache should also expire after TTL+overhead
      testCases.add(Arguments.of(clientKind, "200", 2, true));
    }

    return testCases.stream();
  }
}
