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
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

/** Tests read stream behaviour with untrusted S3ClientKinds on multiple sizes and read patterns */
public class GrayFailureTest extends IntegrationTestBase {

  @Test
  void testFailedReadRecovers() throws IOException {

    List<StreamRead> streamReads = new ArrayList<>();
    streamReads.add(new StreamRead(5 * ONE_MB, 10 * ONE_MB));
    streamReads.add(new StreamRead(15 * ONE_MB, 4 * ONE_MB));
    streamReads.add(new StreamRead(50 * ONE_MB, 20 * ONE_MB));

    StreamReadPattern streamReadPattern =
        StreamReadPattern.builder().streamReads(streamReads).build();

    // Verifies stream contents match, and also that 7 GET requests are made.
    // For the above request pattern, we expect 6 Blocks to be created:
    // [5MB - 13MB, 13MB - 15MB] for the 5MB - 10MB read
    // [15MB, 19MB] for the 15MB - 19MB read
    // [50MB - 58MB, 58MB - 64MB, 64MB - 70MB] for the 50MB - 70MB
    // the first GET will fail and retried as we're using the faulty client, so expect a total of 7
    // GETS.
    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            S3ClientKind.FAULTY_S3_CLIENT, AALInputStreamConfigurationKind.GRAY_FAILURE)) {
      testAndCompareStreamReadPattern(
          S3ClientKind.FAULTY_S3_CLIENT,
          S3Object.RANDOM_128MB,
          streamReadPattern,
          s3AALClientStreamReader);
      assertEquals(
          7,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }
}
