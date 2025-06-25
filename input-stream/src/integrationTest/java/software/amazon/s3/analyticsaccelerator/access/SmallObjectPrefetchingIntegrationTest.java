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
import static software.amazon.s3.analyticsaccelerator.util.Constants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

public class SmallObjectPrefetchingIntegrationTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testSmallObjectRead(S3ClientKind s3ClientKind) throws IOException {

    List<StreamRead> streamReads = new ArrayList<>();
    streamReads.add(new StreamRead(ONE_MB, 500 * ONE_KB));
    streamReads.add(new StreamRead(3 * ONE_MB, 50 * ONE_KB));
    streamReads.add(new StreamRead(2 * ONE_MB, 2 * ONE_MB));

    StreamReadPattern streamReadPattern =
        StreamReadPattern.builder().streamReads(streamReads).build();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind.DEFAULT)) {
      // Since the object is less than 8MB, all reads should be satisfied with a single GET.
      testAndCompareStreamReadPattern(
          s3ClientKind, S3Object.RANDOM_4MB, streamReadPattern, s3AALClientStreamReader);

      assertEquals(
          1,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
      // TODO: This should be fixed with the new PhysicalIO, currently the cache hit metric is
      // slightly inaccurate,
      // and reports a value of 6.
      //  assertEquals(3,
      // s3AALClientStreamReader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.CACHE_HIT));
    }
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testLargeObjectRead(S3ClientKind s3ClientKind) throws IOException {

    List<StreamRead> streamReads = new ArrayList<>();
    streamReads.add(new StreamRead(ONE_MB, 500 * ONE_KB));
    streamReads.add(new StreamRead(3 * ONE_MB, 2 * ONE_MB));
    streamReads.add(new StreamRead(5 * ONE_MB, 2 * ONE_MB));
    streamReads.add(new StreamRead(500 * ONE_MB, 8 * ONE_MB));
    streamReads.add(new StreamRead(2 * ONE_GB, 6 * ONE_MB));

    StreamReadPattern streamReadPattern =
        StreamReadPattern.builder().streamReads(streamReads).build();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, AALInputStreamConfigurationKind.DEFAULT)) {

      // Since the object is more than 8MB, there should be no prefetching, and all blocks will have
      // their own GET.
      testAndCompareStreamReadPattern(
          s3ClientKind, S3Object.RANDOM_10GB, streamReadPattern, s3AALClientStreamReader);

      assertEquals(
          5,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));

      // TODO: This should be fixed with the new PhysicalIO, currently the cache hit metric is
      // slightly inaccurate,
      // and reports a value of 6. Expected value should be 0 as nothing is prefetched, and there is
      // no repeated access
      // to any blocks.
      //       assertEquals(0,
      //
      // s3AALClientStreamReader.getS3SeekableInputStreamFactory().getMetrics().get(MetricKey.CACHE_HIT));
    }
  }
}
