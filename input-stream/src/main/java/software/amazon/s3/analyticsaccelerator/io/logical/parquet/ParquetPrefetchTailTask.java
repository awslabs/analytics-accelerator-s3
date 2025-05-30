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

import java.util.List;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** Task for prefetching the tail of a parquet file. */
public class ParquetPrefetchTailTask {
  private final S3URI s3URI;
  private final Telemetry telemetry;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;
  private static final String OPERATION_PARQUET_PREFETCH_TAIL = "parquet.task.prefetch.tail";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetPrefetchTailTask.class);

  /**
   * Creates a new instance of {@link ParquetPrefetchTailTask}
   *
   * @param s3URI the S3URI of the object to prefetch
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration LogicalIO configuration
   * @param physicalIO PhysicalIO instance
   */
  public ParquetPrefetchTailTask(
      @NonNull S3URI s3URI,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO) {
    this.s3URI = s3URI;
    this.telemetry = telemetry;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.physicalIO = physicalIO;
  }

  /**
   * Prefetch tail of the parquet file
   *
   * @return range of file prefetched
   */
  public List<Range> prefetchTail() {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_TAIL)
                .attribute(StreamAttributes.uri(this.s3URI))
                .build(),
        () -> {
          try {
            long contentLength = physicalIO.metadata().getContentLength();
            List<Range> ranges =
                ParquetUtils.getFileTailPrefetchRanges(logicalIOConfiguration, 0, contentLength);

            IOPlan ioPlan = new IOPlan(ranges);
            // Create a non-empty IOPlan only if we have a valid range to work with
            physicalIO.execute(ioPlan);
            return ioPlan.getPrefetchRanges();
          } catch (Exception e) {
            LOG.debug(
                "Unable to prefetch file tail for {}, parquet prefetch optimisations will be disabled for this key.",
                this.s3URI.getKey(),
                e);
            throw new CompletionException("Error in executing tail prefetch plan", e);
          }
        });
  }
}
