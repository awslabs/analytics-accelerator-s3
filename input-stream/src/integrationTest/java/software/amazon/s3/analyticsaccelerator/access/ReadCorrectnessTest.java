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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

/** Tests read correctness on multiple sizes and read patterns */
public class ReadCorrectnessTest extends IntegrationTestBase {
  @ParameterizedTest
  @MethodSource("clientKinds")
  void testSequentialReads(S3ClientKind s3ClientKind) throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            s3ClientKind, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      testAndCompareStreamReadPattern(
          s3ClientKind,
          S3Object.RANDOM_128MB,
          StreamReadPatternKind.SEQUENTIAL.getStreamReadPattern(S3Object.RANDOM_128MB),
          s3AALClientStreamReader);

      // We read the whole file sequentially in 8MB blocks, expect 16 GETs.
      assertEquals(
          16,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testForwardSkippingReads(S3ClientKind s3ClientKind) throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            s3ClientKind, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      testAndCompareStreamReadPattern(
          s3ClientKind,
          S3Object.RANDOM_128MB,
          StreamReadPatternKind.SKIPPING_FORWARD.getStreamReadPattern(S3Object.RANDOM_16MB),
          s3AALClientStreamReader);

      // The SKIPPING forwards read pattern will read 5% of the object, and the skip the next 5%, so
      // in total
      // there should be 10 reads. In this case, the first read will be for [0 - 838859] and then
      // the next 838860
      // bytes will get skipped, and the second block will start from 1677720. The final block
      // begins at 15099480
      // and reads 838859 bytes.
      assertEquals(
          10,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testQuasiParquetReads(S3ClientKind s3ClientKind) throws IOException {

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(
            s3ClientKind, AALInputStreamConfigurationKind.READ_CORRECTNESS)) {

      testAndCompareStreamReadPattern(
          s3ClientKind,
          S3Object.RANDOM_128MB,
          StreamReadPatternKind.QUASI_PARQUET_COLUMN_CHUNK.getStreamReadPattern(
              S3Object.RANDOM_128MB),
          s3AALClientStreamReader);

      // The QUASI_PARQUET_COLUMN_CHUNK will do 2 READS at the tail of the file for the footer.
      // Since the file
      // being read is not a parquet file, there is no footer caching and so this will lead to two
      // separate GETs.
      // It then does a number of random reads to simulate reading column chunks, in this case these
      // come out to

      // [13421770 - 26843539] For the read that begins at 10%, and reads 10%. This will get split
      // into 2 GETs.
      // [33554425 - 40265309] For the read that starts at 25% and reads 5%.
      // [53687080 - 87241504] For the read that starts at 40% and reads 25%. This will get split
      // into 4 GETs.
      // [80530620 - 93952389] For the read that starts at 60% and reads 10%. This will get split
      // into 2 GETs.
      // [115762768 - 93952389] For the read that starts at 80% and reads 10%. This will get split
      // into 2 GETs.
      assertEquals(
          13,
          s3AALClientStreamReader
              .getS3SeekableInputStreamFactory()
              .getMetrics()
              .get(MetricKey.GET_REQUEST_COUNT));
    }
  }
}
