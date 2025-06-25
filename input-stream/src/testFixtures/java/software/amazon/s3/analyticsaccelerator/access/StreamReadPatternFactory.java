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

import static software.amazon.s3.analyticsaccelerator.access.SizeConstants.ONE_KB_IN_BYTES;

import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;

/** Creates common stream read patterns */
public final class StreamReadPatternFactory {
  private static final int PARQUET_FOOTER_SIZE_SIZE = 4;
  private static final int PARQUET_FOOTER_SIZE = 50 * ONE_KB_IN_BYTES;
  private static final int SKIP_INTERVALS = 10;
  /** Prevent direct instantiation */
  private StreamReadPatternFactory() {}

  /**
   * Constructs a fully sequential read pattern.
   *
   * @param s3Object s3Object {@link S3Object} to read
   * @return a sequential pattern
   */
  public static StreamReadPattern getSequentialReadPattern(@NonNull S3Object s3Object) {
    return StreamReadPattern.builder()
        .streamRead(StreamRead.builder().start(0).length(s3Object.getSize()).build())
        .build();
  }

  /**
   * Construct a read pattern that 'jumps through the object' forward by 5% increments and reads 5%
   * of the object each time. Essentially, this results half of the content being read and half of
   * the content being ignored.
   *
   * @param s3Object {@link S3Object} to read
   * @return a forward seeking read pattern
   */
  public static StreamReadPattern getForwardSeekReadPattern(@NonNull S3Object s3Object) {
    // read in 5% increments skipping 5%, so SKIP_INTERVALS seeks
    List<StreamRead> streamReads = new ArrayList<>();
    int halfIntervalSizePercent = 100 / (SKIP_INTERVALS * 2);
    for (int i = 0; i < SKIP_INTERVALS; i++) {
      streamReads.add(
          StreamRead.builder()
              .start(percent(s3Object, i * halfIntervalSizePercent * 2))
              .length(percent(s3Object, halfIntervalSizePercent))
              .build());
    }
    return StreamReadPattern.builder().streamReads(streamReads).build();
  }

  /**
   * Construct a read pattern that 'jumps through the object' forward by 20% increments and reads
   * 10% of the object each time. Essentially, this results half of the content being read and half
   * of the content being ignored.
   *
   * @param s3Object {@link S3Object} to seek
   * @return a forward seeking read pattern
   */
  public static StreamReadPattern getBackwardSeekReadPattern(S3Object s3Object) {
    return StreamReadPattern.builder()
        .streamReads(new ArrayList<>(getForwardSeekReadPattern(s3Object).getStreamReads()))
        .build();
  }

  /**
   * Define a tail dance + read 60% of the object.
   *
   * @param s3Object {@link S3Object} to read
   * @return a read pattern which is Parquet-like
   */
  public static StreamReadPattern getQuasiParquetRowGroupPattern(S3Object s3Object) {
    return StreamReadPattern.builder()
        // Footer size read
        .streamRead(
            StreamRead.builder()
                .start(s3Object.getSize() - 1 - PARQUET_FOOTER_SIZE_SIZE)
                .length(PARQUET_FOOTER_SIZE_SIZE)
                .build())
        // Footer read
        .streamRead(
            StreamRead.builder()
                .start(s3Object.getSize() - 1 - PARQUET_FOOTER_SIZE)
                .length(PARQUET_FOOTER_SIZE)
                .build())
        // read a  contiguous chunk
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 10)).length(percent(s3Object, 60)).build())
        .build();
  }

  /**
   * Define a tail dance + several column chunks
   *
   * @param s3Object {@link S3Object} to read
   * @return a read pattern which is Parquet-like
   */
  public static StreamReadPattern getQuasiParquetColumnChunkPattern(S3Object s3Object) {
    return StreamReadPattern.builder()
        // Footer size read
        .streamRead(
            StreamRead.builder()
                .start(s3Object.getSize() - 1 - PARQUET_FOOTER_SIZE_SIZE)
                .length(PARQUET_FOOTER_SIZE_SIZE)
                .build())
        // Footer read
        .streamRead(
            StreamRead.builder()
                .start(s3Object.getSize() - 1 - PARQUET_FOOTER_SIZE)
                .length(PARQUET_FOOTER_SIZE)
                .build())
        // read a few contiguous chunks
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 10)).length(percent(s3Object, 10)).build())
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 25)).length(percent(s3Object, 5)).build())
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 40)).length(percent(s3Object, 25)).build())
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 60)).length(percent(s3Object, 10)).build())
        .streamRead(
            StreamRead.builder().start(percent(s3Object, 80)).length(percent(s3Object, 10)).build())
        .build();
  }

  /**
   * @param s3Object benchmark object
   * @param percent an integer between 0 and 100 representing a percentage
   * @return an integer representing the position in the object which is roughly x% of its size.
   *     This is used to seek into specific relative positions in the object when defining read
   *     patterns.
   */
  private static long percent(S3Object s3Object, int percent) {
    return (s3Object.getSize() / 100) * percent;
  }
}
