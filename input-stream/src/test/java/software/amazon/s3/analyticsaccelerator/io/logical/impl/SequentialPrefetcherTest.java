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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class SequentialPrefetcherTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar.data");

  @Test
  void testConstructor() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = LogicalIOConfiguration.builder().partitionSize(4096L).build();
    assertNotNull(new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = LogicalIOConfiguration.builder().partitionSize(4096L).build();
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
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = LogicalIOConfiguration.builder().partitionSize(4096L).build();

    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    when(physicalIO.execute(any(IOPlan.class), any(ReadMode.class)))
        .thenReturn(mock(IOPlanExecution.class));

    SequentialPrefetcher prefetcher =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    prefetcher.prefetch(0);

    ArgumentCaptor<IOPlan> ioPlanCaptor = ArgumentCaptor.forClass(IOPlan.class);
    ArgumentCaptor<ReadMode> readModeCaptor = ArgumentCaptor.forClass(ReadMode.class);
    verify(physicalIO).execute(ioPlanCaptor.capture(), readModeCaptor.capture());

    IOPlan capturedPlan = ioPlanCaptor.getValue();
    List<Range> ranges = capturedPlan.getPrefetchRanges();

    assertEquals(1, ranges.size());
    Range range = ranges.get(0);
    assertEquals(0, range.getStart());
    assertEquals(4095, range.getEnd());
    assertEquals(readModeCaptor.getValue(), ReadMode.SEQUENTIAL_FILE_PREFETCH);
  }

  @Test
  void testPrefetchNearEndOfFile() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = LogicalIOConfiguration.builder().partitionSize(4096L).build();

    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(3000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    when(physicalIO.execute(any(IOPlan.class), any(ReadMode.class)))
        .thenReturn(mock(IOPlanExecution.class));

    SequentialPrefetcher prefetcher =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    prefetcher.prefetch(2000);

    ArgumentCaptor<IOPlan> ioPlanCaptor = ArgumentCaptor.forClass(IOPlan.class);
    ArgumentCaptor<ReadMode> readModeCaptor = ArgumentCaptor.forClass(ReadMode.class);
    verify(physicalIO).execute(ioPlanCaptor.capture(), readModeCaptor.capture());

    IOPlan capturedPlan = ioPlanCaptor.getValue();
    List<Range> ranges = capturedPlan.getPrefetchRanges();

    assertEquals(1, ranges.size());
    Range range = ranges.get(0);
    assertEquals(2000, range.getStart());
    assertEquals(2999, range.getEnd());
    assertEquals(readModeCaptor.getValue(), ReadMode.SEQUENTIAL_FILE_PREFETCH);
  }

  @Test
  void testPrefetchWithIOException() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = LogicalIOConfiguration.builder().partitionSize(4096L).build();
    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);
    when(physicalIO.execute(any(IOPlan.class), any(ReadMode.class)))
        .thenThrow(new IOException("Simulated IO exception"));

    SequentialPrefetcher prefetcher =
        new SequentialPrefetcher(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config);

    // The prefetch method should not throw an exception now
    prefetcher.prefetch(0);

    // Verify that execute was called despite the exception
    verify(physicalIO).execute(any(IOPlan.class), any(ReadMode.class));
  }
}
