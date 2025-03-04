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
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class OptimizedLogicalIOImpl extends DefaultLogicalIOImpl {
  // This class is extending DefaultLogicalIOImpl, inheriting its basic functionality
  private static final Logger LOG = LoggerFactory.getLogger(OptimizedLogicalIOImpl.class);
  private static final long INITIAL_READ_SIZE = 64 * 1024; // 64KB
  // Defines the size of the initial read, matching the default read-ahead size
  private static final long REMAINING_INITIAL_READ =
      8 * 1024 * 1024 - INITIAL_READ_SIZE; // Remaining of 8MB
  private static final long PREFETCH_SIZE = 120 * 1024 * 1024; // 128MB

  private volatile boolean prefetchStarted = false;
  private CompletableFuture<Void> prefetchFuture;

  public OptimizedLogicalIOImpl(
      @NonNull S3URI s3URI, @NonNull PhysicalIO physicalIO, @NonNull Telemetry telemetry) {
    super(s3URI, physicalIO, telemetry);
  }

  @Override
  // Overrides the read method from the superclass i.e. DefaultLogicalIOImpl
  public int read(byte[] buf, int off, int len, long position) throws IOException {

    if (!prefetchStarted) { // If prefetching hasn't started, initiate it
      startPrefetch(position);
    }
    // Call the superclass read method to perform the actual read operation
    return super.read(buf, off, len, position);
  }

  private synchronized void startPrefetch(long initialPosition) throws IOException {
    // If prefetching has already started, return immediately
    if (prefetchStarted) {
      return;
    }
    prefetchStarted = true;

    long contentLength = metadata().getContentLength(); // Get the total length of the file
    long remainingLength =
        contentLength - initialPosition; // Calculate how much of the file is left to read

    // First, read the initial chunk,Determine how much to read initially (64KB or less if file is
    // smaller)
    long initialReadSize = Math.min(INITIAL_READ_SIZE, remainingLength);
    if (initialReadSize > 0) {
      try {
        getPhysicalIO()
            .read(new byte[(int) initialReadSize], 0, (int) initialReadSize, initialPosition);
      } catch (IOException e) {
        LOG.warn("Failed to read initial chunk", e);
      }
    }

    remainingLength -= initialReadSize; // Update the remaining length after the initial read

    // Then read the remaining of the initial 8MB
    if (remainingLength > 0) {
      long remainingReadSize = Math.min(REMAINING_INITIAL_READ, remainingLength);
      try {
        getPhysicalIO()
            .read(
                new byte[(int) remainingReadSize],
                0,
                (int) remainingReadSize,
                initialPosition + initialReadSize);
      } catch (IOException e) {
        LOG.warn("Failed to read remaining initial chunk", e);
      }
      remainingLength -= remainingReadSize;
    }

    // Start the prefetch asynchronously
    if (remainingLength > 0) {
      final long prefetchStart = initialPosition + INITIAL_READ_SIZE + REMAINING_INITIAL_READ;
      final long prefetchSize = Math.min(PREFETCH_SIZE, remainingLength);
      prefetchFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  getPhysicalIO()
                      .read(new byte[(int) prefetchSize], 0, (int) prefetchSize, prefetchStart);
                } catch (IOException e) {
                  LOG.warn("Failed to prefetch additional data", e);
                }
              });
    }
  }

  @Override
  // Overrides the close method from the superclass
  public void close() throws IOException {
    if (prefetchFuture != null) {
      prefetchFuture.cancel(true);
    }
    super.close();
  }
}
/*
APPROACH:
 * Example: Reading a 500 MB file
 *
 * 1. Initial synchronous reads:
 *    - First 64 KB: positions 0 to 65,535
 *    - Next 7.75 MB: positions 65,536 to 8,388,607
 *    Total: 8 MB read synchronously
 *
 * 2. Asynchronous prefetch:
 *    - Next 120 MB: positions 8,388,608 to 134,217,727
 *
 * 3. Subsequent reads:
 *    - 0 to 8,388,607 (8 MB): Immediately available
 *    - 8,388,608 to 134,217,727 (120 MB): Available as we prefetch it
 *    - Beyond 134,217,727: Not prefetched, uses default read mechanism
 *
 * This approach provides fast access to the beginning of the file,
 * improves performance for the next large chunk through prefetching,
 *
 * Note: The prefetch size is set to 120 MB because Spark reads files in chunks of 128 MB.
 * By prefetching 120 MB after the initial 8 MB read, we ensure that the first full Spark
 * chunk (128 MB) is already available, optimizing performance for Spark's reading pattern.
 */
