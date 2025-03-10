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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.Range;

/**
 * A prefetcher optimizing read performance for sequential file formats, currently focused on CSV.
 * It performs a small initial read followed by an asynchronous larger prefetch.
 *
 * <p>Configurable read sizes allow optimization for various use cases. While designed for CSV, this
 * approach is applicable to other sequentially read formats, potentially improving performance by
 * reducing individual read requests and leveraging asynchronous prefetching.
 */
public class CSVPrefetcher implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CSVPrefetcher.class);
  private final long initialReadSize;
  private final long prefetchSize;

  private final PhysicalIO physicalIO;
  private final Telemetry telemetry;
  private volatile boolean prefetchStarted = false;
  private CompletableFuture<IOPlanExecution> prefetchFuture;
  private long initialPosition = -1;
  /**
   * Constructs a CSVPrefetcher.
   *
   * @param physicalIO the PhysicalIO capable of fetching the physical bytes from the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration the LogicalIO's configuration
   */
  public CSVPrefetcher(
      @NonNull PhysicalIO physicalIO,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration) {
    this.physicalIO = physicalIO;
    this.telemetry = telemetry;
    this.initialReadSize = logicalIOConfiguration.getCsvInitialReadSize();
    this.prefetchSize = logicalIOConfiguration.getCsvPrefetchSize();
  }
  /**
   * Ensures that prefetching has started from the given position.
   *
   * @param position the position to start prefetching from
   * @throws IOException if an I/O error occurs
   */
  public synchronized void ensurePrefetchStarted(long position) throws IOException {
    if (initialPosition == -1) {
      initialPosition = position;
      startPrefetch(initialPosition);
    }
  }
  /**
   * Starts the prefetch process. This includes an initial synchronous read followed by an
   * asynchronous prefetch of a larger chunk.
   *
   * @param initialPosition the position to start prefetching from
   * @throws IOException if an I/O error occurs
   */
  private void startPrefetch(long initialPosition) throws IOException {
    if (prefetchStarted) {
      return;
    }
    prefetchStarted = true;

    long contentLength = physicalIO.metadata().getContentLength();
    long remainingLength = contentLength - initialPosition;

    long initialReadSize = Math.min(this.initialReadSize, remainingLength);
    IOPlan initialPlan =
        new IOPlan(new Range(initialPosition, initialPosition + initialReadSize - 1));
    IOPlanExecution initialExecution = physicalIO.execute(initialPlan);

    if (initialExecution.getState() != IOPlanState.SUBMITTED) {
      LOG.debug("Initial read plan execution failed or was skipped");
    }

    if (remainingLength > initialReadSize) {
      final long prefetchStart = initialPosition + initialReadSize;
      final long prefetchSize = Math.min(this.prefetchSize, remainingLength - initialReadSize);

      prefetchFuture =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return executePrefetchPlan(prefetchStart, prefetchSize);
                } catch (IOException e) {
                  LOG.debug("Failed to prefetch additional data", e);
                  return IOPlanExecution.builder().state(IOPlanState.FAILED).build();
                }
              });
    }
  }
  /**
   * Executes the prefetch plan for a given range.
   *
   * @param start the start position of the range to prefetch
   * @param size the size of the range to prefetch
   * @return the IOPlanExecution object of the prefetch operation
   * @throws IOException if an I/O error occurs
   */
  private IOPlanExecution executePrefetchPlan(long start, long size) throws IOException {
    IOPlan prefetchPlan = new IOPlan(new Range(start, start + size - 1));
    return physicalIO.execute(prefetchPlan);
  }
  /**
   * Closes the prefetcher and cancels any ongoing prefetch operations.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (prefetchFuture != null) {
      prefetchFuture.cancel(true);
    }
  }
}
