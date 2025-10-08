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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.FileTail;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetMetadataParsingTask;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetPredictivePrefetchingTask;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetPrefetchTailTask;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetReadTailTask;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetPrefetcherTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void testConstructor() {
    assertNotNull(
        new ParquetPrefetcher(
            mock(S3URI.class),
            mock(PhysicalIO.class),
            mock(Telemetry.class),
            mock(LogicalIOConfiguration.class),
            mock(ParquetColumnPrefetchStore.class),
            mock(RequestCallback.class)));
  }

  @Test
  public void testConstructorNulls() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                null,
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                null,
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                null,
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                null,
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                null,
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                null,
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                null,
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                null));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                null,
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(RequestCallback.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                null,
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(RequestCallback.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(RequestCallback.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                null,
                mock(ParquetColumnPrefetchStore.class),
                mock(RequestCallback.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                null,
                mock(RequestCallback.class)));
  }

  @Test
  public void testConfigurationsPrefetchRemainingColumnChunkEnabled() {
    // Given: config with metadata awareness ENABLED but predictive prefetching DISABLED
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.COLUMN_BOUND).build();

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        mock(ParquetPrefetchRemainingColumnTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            parquetPrefetchRemainingColumnTask,
            mock(ParquetPredictivePrefetchingTask.class));

    // When: prefetching a column chunk
    parquetPrefetcher.prefetchRemainingColumnChunk(100, 200).join();

    // Then: remaining columns are requested
    verify(parquetPrefetchRemainingColumnTask, times(1))
        .prefetchRemainingColumnChunk(anyLong(), anyInt());
  }

  @Test
  public void testConfigurationsPrefetchRemainingColumnChunkDisabled() {
    // Given: config that should not trigger prefetching
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.OFF).build();

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        mock(ParquetPrefetchRemainingColumnTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            parquetPrefetchRemainingColumnTask,
            mock(ParquetPredictivePrefetchingTask.class));

    // When: prefetching a column chunk
    IOPlanExecution execution = parquetPrefetcher.prefetchRemainingColumnChunk(100, 200).join();

    // Then: verify that the prefetching task is never actually triggered
    verifyNoInteractions(parquetPrefetchRemainingColumnTask);
    assertEquals(IOPlanState.SKIPPED, execution.getState());
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataDefaultConfig() {
    // Given: default LogicalIO configuration
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.ALL).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: read tail is triggered
    verify(parquetReadTailTask, times(1)).readFileTail();
    // Then: columns are stored
    verify(parquetMetadataParsingTask, times(1)).storeColumnMappers(any(FileTail.class));
    // Then: predictive reads are also triggered
    verify(parquetPredictivePrefetchingTask, times(1))
        .prefetchRecentColumns(any(ColumnMappers.class), anyList(), anyBoolean());
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataNoPredictivePrefetching() {
    // Given: config with predictive prefetching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.OFF).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is still triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: BUT predictive reads are NOT triggered
    verifyNoInteractions(parquetPredictivePrefetchingTask);
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataAllPrefetchDisabled() {
    // Given: config with all prefetching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.OFF).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: read tail is NOT triggered
    verifyNoInteractions(parquetReadTailTask);
    // Then: columns are NOT parsed and stored
    verifyNoInteractions(parquetMetadataParsingTask);
    // Then: predictive reads are NOT triggered
    verifyNoInteractions(parquetPredictivePrefetchingTask);
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataFooterCachingDisabled() {
    // Given: config with footer caching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            parquetPrefetchTailTask,
            mock(ParquetReadTailTask.class),
            mock(ParquetPrefetchRemainingColumnTask.class),
            mock(ParquetPredictivePrefetchingTask.class));

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata();

    // Then: tail prefetching is not triggered
    verifyNoInteractions(parquetPrefetchTailTask);
  }

  @Test
  public void testAddToRecentColumnListProxiesCallsToDependency() {
    // Given: default LogicalIO configuration
    LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: a column is added to recent list
    parquetPrefetcher.addToRecentColumnList(100, 0);

    // Then: it is also added within the task
    verify(parquetPredictivePrefetchingTask, times(1)).addToRecentColumnList(100, 0);
  }

  @Test
  public void testAddToRecentColumnListExceptionCaught() {
    // Given: default LogicalIO configuration
    LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: a column is added to recent list
    when(parquetPredictivePrefetchingTask.addToRecentColumnList(anyLong(), anyInt()))
        .thenThrow(new NullPointerException());

    assertDoesNotThrow(() -> parquetPrefetcher.addToRecentColumnList(100, 0));
  }

  @Test
  public void prefetchFooterAndBuildMetadataReadTailExceptionCaught() {
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.ALL).build();

    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                logicalIOConfiguration,
                mock(PhysicalIO.class),
                mock(ParquetColumnPrefetchStore.class)));

    when(parquetReadTailTask.readFileTail())
        .thenThrow(new CompletionException("Error", new IOException()));

    assertDoesNotThrow(() -> parquetPrefetcher.prefetchFooterAndBuildMetadata().join());
  }

  @Test
  public void prefetchFooterAndBuildMetadataParseMetadataExceptionCaught() throws IOException {
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.ALL).build();

    ParquetMetadataParsingTask parquetMetadataParsingTask = mock(ParquetMetadataParsingTask.class);
    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    IOPlanExecution skippedIoPlanExecution =
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build();

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            mock(ParquetPrefetchTailTask.class),
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                logicalIOConfiguration,
                physicalIO,
                mock(ParquetColumnPrefetchStore.class)));

    when(parquetReadTailTask.readFileTail())
        .thenReturn(new FileTail(ByteBuffer.wrap(new byte[5]), 5));
    when(parquetMetadataParsingTask.storeColumnMappers(any(FileTail.class)))
        .thenThrow(new CompletionException("Error", new IOException()));
    when(physicalIO.execute(any(IOPlan.class), any(ReadMode.class)))
        .thenReturn(skippedIoPlanExecution);

    assertEquals(parquetPrefetcher.prefetchFooterAndBuildMetadata().join(), skippedIoPlanExecution);
  }

  private ParquetReadTailTask getTestParquetReadTailTask() {
    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
    when(parquetReadTailTask.readFileTail()).thenReturn(new FileTail(ByteBuffer.allocate(10), 10));
    return parquetReadTailTask;
  }

  private ParquetMetadataParsingTask getTestParquetMetadataTask() {
    ParquetMetadataParsingTask parquetMetadataParsingTask = mock(ParquetMetadataParsingTask.class);
    when(parquetMetadataParsingTask.storeColumnMappers(any()))
        .thenReturn(mock(ColumnMappers.class));
    return parquetMetadataParsingTask;
  }

  private ParquetPrefetcher getTestPrefetcher(
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetColumnPrefetchStore parquetColumnPrefetchStore,
      ParquetMetadataParsingTask parquetMetadataParsingTask,
      ParquetPrefetchTailTask parquetPrefetchTailTask,
      ParquetReadTailTask parquetReadTailTask,
      ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask,
      ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask) {

    return new ParquetPrefetcher(
        TEST_URI,
        logicalIOConfiguration,
        parquetColumnPrefetchStore,
        Telemetry.NOOP,
        parquetMetadataParsingTask,
        parquetPrefetchTailTask,
        parquetReadTailTask,
        parquetPrefetchRemainingColumnTask,
        parquetPredictivePrefetchingTask);
  }
}
