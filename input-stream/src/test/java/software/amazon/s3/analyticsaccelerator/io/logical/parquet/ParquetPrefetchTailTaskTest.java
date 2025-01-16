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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = {"NP_NONNULL_PARAM_VIOLATION", "SIC_INNER_SHOULD_BE_STATIC_ANON"},
    justification = "We mean to pass nulls to checks, and inner classes are appropriate in tests")
public class ParquetPrefetchTailTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetPrefetchTailTask(
            TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetchTailTask(
                null, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetchTailTask(
                TEST_URI, null, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPrefetchTailTask(TEST_URI, Telemetry.NOOP, null, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetchTailTask(
                TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, null));
  }

  @Test
  void testTailPrefetch() throws IOException {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().prefetchFooterEnabled(true).build();

    HashMap<Long, List<Range>> contentSizeToRanges =
        getPrefetchRangeList(
            configuration.getPrefetchFileMetadataSize(),
            configuration.getPrefetchFilePageIndexSize(),
            configuration.getSmallObjectSizeThreshold(),
            5L * configuration.getLargeFileSize());

    for (Map.Entry<Long, List<Range>> contentLengthToRangeList : contentSizeToRanges.entrySet()) {
      PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);
      ObjectMetadata metadata =
          ObjectMetadata.builder().contentLength(contentLengthToRangeList.getKey()).build();
      when(mockedPhysicalIO.metadata()).thenReturn(metadata);

      ParquetPrefetchTailTask parquetPrefetchTailTask =
          new ParquetPrefetchTailTask(
              TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);
      parquetPrefetchTailTask.prefetchTail();

      verify(mockedPhysicalIO).execute(any(IOPlan.class));
      verify(mockedPhysicalIO)
          .execute(argThat(new IOPlanMatcher(contentLengthToRangeList.getValue())));
    }
  }

  @Test
  @SneakyThrows
  void testExceptionRemappedToCompletionException() {
    // Given: Parquet Tail Prefetching task
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    ParquetPrefetchTailTask parquetPrefetchTailTask =
        new ParquetPrefetchTailTask(
            TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When: task executes but PhysicalIO throws
    ObjectMetadata metadata = ObjectMetadata.builder().contentLength(600).build();
    when(mockedPhysicalIO.metadata()).thenReturn(metadata);
    doThrow(new IOException("Error in prefetch")).when(mockedPhysicalIO).execute(any(IOPlan.class));

    // Then: exception is re-mapped to CompletionException
    assertThrows(CompletionException.class, () -> parquetPrefetchTailTask.prefetchTail());
  }

  private HashMap<Long, List<Range>> getPrefetchRangeList(
      long footerSize, long pageIndexSize, long smallFileSize, long largeFileSize) {
    return new HashMap<Long, List<Range>>() {
      {
        put(
            1L,
            new ArrayList<Range>() {
              {
                add(new Range(0, 0));
              }
            });
        put(
            footerSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, footerSize - 1));
              }
            });
        put(
            10L + footerSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, footerSize + 9));
              }
            });
        put(
            -1L + smallFileSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, smallFileSize - 2));
              }
            });
        put(
            10L + smallFileSize,
            new ArrayList<Range>() {
              {
                add(new Range(smallFileSize + 10 - footerSize, smallFileSize + 9));
                add(
                    new Range(
                        smallFileSize + 10 - footerSize - pageIndexSize,
                        smallFileSize + 10 - footerSize - 1));
              }
            });
        put(
            largeFileSize,
            new ArrayList<Range>() {
              {
                add(new Range(largeFileSize - ONE_MB, largeFileSize - 1));
                add(new Range(largeFileSize - ONE_MB - (8 * ONE_MB), largeFileSize - ONE_MB - 1));
              }
            });
      }
    };
  }
}
