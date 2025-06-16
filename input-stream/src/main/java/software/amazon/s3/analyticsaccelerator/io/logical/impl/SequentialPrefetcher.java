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

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/**
 * Handles prefetching of data for sequential read operations. Uses 'sparkPartitionSize' from
 * LogicalIOConfiguration to determine prefetch size. Designed to work with SequentialLogicalIOImpl
 * for optimizing large, sequential file reads.
 */
public class SequentialPrefetcher {
  private final PhysicalIO physicalIO;
  private final Telemetry telemetry;
  private final S3URI s3URI;
  private boolean prefetchStarted = false;
  private final long prefetchSize;

  private static final Logger LOG = LoggerFactory.getLogger(SequentialPrefetcher.class);
  private static final String OPERATION_SEQUENTIAL_PREFETCH = "sequential.prefetcher.prefetch";
  /**
   * Constructs an instance of SequentialLogicalIOImpl.
   *
   * @param s3URI the S3 URI of the object fetched
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration configuration for this logical IO implementation, particularly
   *     sparkPartitionSize
   */
  public SequentialPrefetcher(
      @NonNull S3URI s3URI,
      @NonNull PhysicalIO physicalIO,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration) {
    this.s3URI = s3URI;
    this.physicalIO = physicalIO;
    this.telemetry = telemetry;
    this.prefetchSize = logicalIOConfiguration.getPartitionSize();
  }
  /**
   * Attempts to initiate a one-time prefetch operation from the specified position. Subsequent
   * calls or any errors are silently ignored. Prefetches up to prefetch size or file end.
   *
   * @param position start position for prefetching
   */
  public void prefetch(long position) {
    try {
      if (prefetchStarted) {
        return;
      }
      prefetchStarted = true;

      long contentLength = physicalIO.metadata().getContentLength();
      long endPosition = Math.min(position + prefetchSize, contentLength);

      telemetry.measureVerbose(
          () ->
              Operation.builder()
                  .name(OPERATION_SEQUENTIAL_PREFETCH)
                  .attribute(StreamAttributes.uri(this.s3URI))
                  .attribute(StreamAttributes.range(position, endPosition - 1))
                  .build(),
          () -> {
            IOPlan prefetchPlan = new IOPlan(new Range(position, endPosition - 1));
            return physicalIO.execute(prefetchPlan, ReadMode.SEQUENTIAL_FILE_PREFETCH);
          });
    } catch (Exception e) {
      // Log the exception at debug level and swallow it
      LOG.debug("Error during prefetch operation for {}", this.s3URI.getKey(), e);
    }
  }
}
