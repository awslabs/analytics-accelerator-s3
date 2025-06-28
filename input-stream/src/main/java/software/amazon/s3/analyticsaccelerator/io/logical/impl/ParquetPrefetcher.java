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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.*;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.ColumnPrefetchingServerClient;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/**
 * A Parquet prefetcher is a common place for all Parquet-related async prefetching activity
 * (prefetching and caching footers, parsing and interpreting footers, collecting Parquet usage
 * information and doing prefetching based on them).
 *
 * <p>The Parquet prefetcher swallows all exceptions arising from the tasks it schedules because
 * exceptions do not escape CompletableFutures.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class ParquetPrefetcher {
  @NonNull private final S3URI s3URI;
  @NonNull private final LogicalIOConfiguration logicalIOConfiguration;
  @NonNull private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;
  @NonNull private final Telemetry telemetry;

  // Tasks
  @NonNull private final ParquetMetadataParsingTask parquetMetadataParsingTask;
  @NonNull private final ParquetPrefetchTailTask parquetPrefetchTailTask;
  @NonNull private final ParquetReadTailTask parquetReadTailTask;
  @NonNull private final ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask;
  @NonNull private final ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPrefetcher.class);

  private static final String OPERATION_PARQUET_PREFETCH_COLUMN_CHUNK =
      "parquet.prefetcher.prefetch.column.chunk.async";
  private static final String OPERATION_PARQUET_PREFETCH_FOOTER_AND_METADATA =
      "parquet.prefetcher.prefetch.footer.and.metadata.async";

  /**
   * Constructs a ParquetPrefetcher.
   *
   * @param s3Uri the S3Uri of the underlying object
   * @param physicalIO the PhysicalIO capable of actually fetching the physical bytes from the
   *     object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration the LogicalIO's configuration
   * @param parquetColumnPrefetchStore a common place for Parquet usage information
   */
  public ParquetPrefetcher(
      S3URI s3Uri,
      PhysicalIO physicalIO,
      Telemetry telemetry,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetColumnPrefetchStore parquetColumnPrefetchStore,
      ColumnPrefetchingServerClient columnPrefetchingServerClient) {
    this(
        s3Uri,
        logicalIOConfiguration,
        parquetColumnPrefetchStore,
        telemetry,
        new ParquetMetadataParsingTask(s3Uri, parquetColumnPrefetchStore),
        new ParquetPrefetchTailTask(s3Uri, telemetry, logicalIOConfiguration, physicalIO),
        new ParquetReadTailTask(s3Uri, telemetry, logicalIOConfiguration, physicalIO),
        new ParquetPrefetchRemainingColumnTask(
            s3Uri, telemetry, physicalIO, parquetColumnPrefetchStore),
        new ParquetPredictivePrefetchingTask(
            s3Uri,
            telemetry,
            logicalIOConfiguration,
            physicalIO,
            parquetColumnPrefetchStore,
            columnPrefetchingServerClient));
  }

  /**
   * Given a position and length, prefetches the remaining part of the Parquet column.
   *
   * @param position a position of a read
   * @param len the length of a read
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  public CompletableFuture<IOPlanExecution> prefetchRemainingColumnChunk(long position, int len) {
    return telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_COLUMN_CHUNK)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(position, position + len - 1))
                .build(),
        prefetchRemainingColumnChunkImpl(position, len));
  }

  /**
   * Given a position and length, prefetches the remaining part of the Parquet column.
   *
   * @param position a position of a read
   * @param len the length of a read
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  private CompletableFuture<IOPlanExecution> prefetchRemainingColumnChunkImpl(
      long position, int len) {
    if (logicalIOConfiguration.getPrefetchingMode() == PrefetchMode.COLUMN_BOUND) {
      // TODO: https://github.com/awslabs/analytics-accelerator-s3/issues/88
      return CompletableFuture.supplyAsync(
          () -> parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(position, len));
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  /**
   * Prefetch the footer and Parquet metadata for the object that s3Uri points to
   *
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  public CompletableFuture<IOPlanExecution> prefetchFooterAndBuildMetadata() {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_FOOTER_AND_METADATA)
                .attribute(StreamAttributes.uri(this.s3URI))
                .build(),
        prefetchFooterAndBuildMetadataImpl());
  }

  /**
   * Prefetch the footer and Parquet metadata for the object that s3Uri points to
   *
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  private CompletableFuture<IOPlanExecution> prefetchFooterAndBuildMetadataImpl() {
    if (logicalIOConfiguration.isPrefetchFooterEnabled()) {
      parquetPrefetchTailTask.prefetchTail();
    }

    if (shouldPrefetch()) {
      // TODO: https://github.com/awslabs/analytics-accelerator-s3/issues/88
      CompletableFuture<ColumnMappers> columnMappersCompletableFuture =
          CompletableFuture.supplyAsync(parquetReadTailTask::readFileTail)
              .thenApply(parquetMetadataParsingTask::storeColumnMappers)
              .exceptionally(
                  (e) -> new ColumnMappers(Collections.emptyMap(), Collections.emptyMap()));

      return prefetchPredictedColumns(columnMappersCompletableFuture);
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  private CompletableFuture<IOPlanExecution> prefetchPredictedColumns(
      CompletableFuture<ColumnMappers> columnMappersCompletableFuture) {

    if (logicalIOConfiguration.getPrefetchingMode() == PrefetchMode.ALL) {
      return columnMappersCompletableFuture.thenApply(
          (ColumnMappers columnMappers) ->
              parquetPredictivePrefetchingTask.prefetchRecentColumns(
                  columnMappers, ParquetUtils.constructRowGroupsToPrefetch(), false));
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  /**
   * Record this position in the recent column list
   *
   * @param position the position to record
   * @param len The length of the current read
   */
  public void addToRecentColumnList(long position, int len) {
    try {
      if (logicalIOConfiguration.getPrefetchingMode() != PrefetchMode.OFF) {
        this.parquetPredictivePrefetchingTask.addToRecentColumnList(position, len);
      }
    } catch (Exception e) {
      LOG.debug(
          "Unable to add column to recently read columns tracked list for {}.", s3URI.getKey(), e);
    }
  }

  private boolean shouldPrefetch() {
    return logicalIOConfiguration.getPrefetchingMode() != PrefetchMode.OFF
        && parquetColumnPrefetchStore.getColumnMappers(s3URI) == null;
  }
}
