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
package software.amazon.s3.dataaccelerator.io.logical.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import software.amazon.s3.dataaccelerator.common.telemetry.Operation;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.logical.parquet.*;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.dataaccelerator.util.PrefetchMode;
import software.amazon.s3.dataaccelerator.util.S3URI;
import software.amazon.s3.dataaccelerator.util.StreamAttributes;

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
      ParquetColumnPrefetchStore parquetColumnPrefetchStore) {
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
            s3Uri, telemetry, logicalIOConfiguration, physicalIO, parquetColumnPrefetchStore));
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
      // TODO: https://github.com/awslabs/s3-connector-framework/issues/88
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
    if (logicalIOConfiguration.isFooterCachingEnabled()) {
      parquetPrefetchTailTask.prefetchTail();
    }

    if (shouldPrefetch()) {
      // TODO: https://github.com/awslabs/s3-connector-framework/issues/88
      CompletableFuture<ColumnMappers> columnMappersCompletableFuture =
          CompletableFuture.supplyAsync(parquetReadTailTask::readFileTail)
              .thenApply(parquetMetadataParsingTask::storeColumnMappers);

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
                  columnMappers, ParquetUtils.constructRowGroupsToPrefetch()));
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  /**
   * Record this position in the recent column list
   *
   * @param position the position to record
   */
  public Optional<String> addToRecentColumnList(long position, int len) {
    if (logicalIOConfiguration.getPrefetchingMode() != PrefetchMode.OFF) {
      return this.parquetPredictivePrefetchingTask.addToRecentColumnList(position, len);
    }

    return Optional.empty();
  }

  private boolean shouldPrefetch() {
    return logicalIOConfiguration.getPrefetchingMode() != PrefetchMode.OFF
        && parquetColumnPrefetchStore.getColumnMappers(s3URI) == null;
  }
}
