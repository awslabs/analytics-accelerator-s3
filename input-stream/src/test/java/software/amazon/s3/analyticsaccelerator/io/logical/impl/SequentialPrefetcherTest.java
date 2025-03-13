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
package software.amazon.s3.analyticsaccelerator.io.logical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class SequentialPrefetcherTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar.data");
  private PhysicalIO physicalIO;
  private LogicalIOConfiguration config;
  private ObjectMetadata metadata;

  @BeforeEach
  void setUp() {
    physicalIO = mock(PhysicalIO.class);
    config = LogicalIOConfiguration.builder().sparkPartitionSize(4096L).build();
    metadata = mock(ObjectMetadata.class);
  }

  @Test
  void testConstructor() {
    assertNotNull(new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> new SequentialPrefetcher(null, physicalIO, TestTelemetry.DEFAULT, config));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialPrefetcher(TEST_URI, null, TestTelemetry.DEFAULT, config));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialPrefetcher(TEST_URI, physicalIO, null, config));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, null));
  }

  @Test
  void testPrefetchFunctionality() throws IOException {

    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    IOPlanExecution mockExecution = mock(IOPlanExecution.class);
    when(physicalIO.execute(any(IOPlan.class))).thenReturn(mockExecution);

    SequentialPrefetcher prefetcher =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    // Initial prefetch from start
    prefetcher.prefetch(0);
    verify(physicalIO)
        .execute(
            argThat(
                plan ->
                    plan.getPrefetchRanges().get(0).getStart() == 0
                        && plan.getPrefetchRanges().get(0).getEnd() == 4095));

    // Subsequent prefetches should be ignored
    prefetcher.prefetch(100);
    prefetcher.prefetch(200);
    verify(physicalIO, times(1)).execute(any(IOPlan.class));

    // Prefetch near end of file
    reset(physicalIO);
    when(metadata.getContentLength()).thenReturn(3000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    when(physicalIO.execute(any(IOPlan.class))).thenReturn(mockExecution);

    SequentialPrefetcher prefetcherSmallFile =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    prefetcherSmallFile.prefetch(2000);
    verify(physicalIO)
        .execute(
            argThat(
                plan ->
                    plan.getPrefetchRanges().get(0).getStart() == 2000
                        && plan.getPrefetchRanges().get(0).getEnd() == 2999));
  }

  @Test
  void testPrefetchWithIOException() throws IOException {
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    when(physicalIO.execute(any(IOPlan.class)))
        .thenThrow(new IOException("Simulated IO exception"));

    SequentialPrefetcher prefetcher =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    assertThrows(IOException.class, () -> prefetcher.prefetch(0));
  }
}
