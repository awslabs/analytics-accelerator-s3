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
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class CSVPrefetcherTest {

  @Test
  public void testConstructor() {
    assertNotNull(
        new CSVPrefetcher(
            mock(PhysicalIO.class), mock(Telemetry.class), mock(LogicalIOConfiguration.class)));
  }

  @Test
  public void testConstructorNulls() {
    assertThrows(
        NullPointerException.class,
        () -> new CSVPrefetcher(null, mock(Telemetry.class), mock(LogicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new CSVPrefetcher(mock(PhysicalIO.class), null, mock(LogicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new CSVPrefetcher(mock(PhysicalIO.class), mock(Telemetry.class), null));
  }

  @Test
  public void testClose() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    Telemetry telemetry = mock(Telemetry.class);
    LogicalIOConfiguration config =
        LogicalIOConfiguration.builder().csvInitialReadSize(1024).csvPrefetchSize(4096).build();

    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);

    IOPlanExecution mockExecution = mock(IOPlanExecution.class);
    when(mockExecution.getState()).thenReturn(IOPlanState.SUBMITTED);
    when(physicalIO.execute(any(IOPlan.class))).thenReturn(mockExecution);

    CSVPrefetcher prefetcher = new CSVPrefetcher(physicalIO, telemetry, config);

    prefetcher.ensurePrefetchStarted(0);
    prefetcher.close();

    assertDoesNotThrow(() -> prefetcher.close());
  }

  @Test
  public void testAsyncPrefetchExecutionWithException() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    Telemetry telemetry = mock(Telemetry.class);
    LogicalIOConfiguration config =
        LogicalIOConfiguration.builder().csvInitialReadSize(1024).csvPrefetchSize(4096).build();

    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);

    IOPlanExecution mockExecution = mock(IOPlanExecution.class);
    when(mockExecution.getState()).thenReturn(IOPlanState.SUBMITTED);
    when(physicalIO.execute(any(IOPlan.class)))
        .thenReturn(mockExecution)
        .thenThrow(new IOException("Simulated IO exception"));

    CSVPrefetcher prefetcher = new CSVPrefetcher(physicalIO, telemetry, config);

    prefetcher.ensurePrefetchStarted(0);

    // Wait for the async prefetch to complete
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Verify that execute is called twice: once for initial read and once for prefetch
    verify(physicalIO, times(2)).execute(any(IOPlan.class));
  }

  @Test
  public void testPrefetchPlanExecution() throws IOException, InterruptedException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    Telemetry telemetry = mock(Telemetry.class);
    LogicalIOConfiguration config =
        LogicalIOConfiguration.builder().csvInitialReadSize(1024).csvPrefetchSize(4096).build();

    ObjectMetadata metadata = mock(ObjectMetadata.class);
    when(metadata.getContentLength()).thenReturn(10000L);
    when(physicalIO.metadata()).thenReturn(metadata);

    IOPlanExecution mockExecution = mock(IOPlanExecution.class);
    when(mockExecution.getState()).thenReturn(IOPlanState.SUBMITTED);
    when(physicalIO.execute(any(IOPlan.class))).thenReturn(mockExecution);

    CSVPrefetcher prefetcher = new CSVPrefetcher(physicalIO, telemetry, config);

    prefetcher.ensurePrefetchStarted(0);

    // Wait for the async prefetch to complete
    Thread.sleep(100);

    // Verify that execute is called twice: once for initial read and once for prefetch
    verify(physicalIO, times(2)).execute(any(IOPlan.class));

    // Verify the ranges of the IOPlans
    verify(physicalIO)
        .execute(
            argThat(
                plan ->
                    plan.getPrefetchRanges().get(0).getStart() == 0
                        && plan.getPrefetchRanges().get(0).getEnd() == 1023));
    verify(physicalIO)
        .execute(
            argThat(
                plan ->
                    plan.getPrefetchRanges().get(0).getStart() == 1024
                        && plan.getPrefetchRanges().get(0).getEnd() == 5119));
  }
}
