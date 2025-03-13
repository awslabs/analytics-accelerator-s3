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
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * A sequential read-aware implementation of the LogicalIO layer. This implementation is capable of
 * configurable prefetching based on the provided LogicalIOConfiguration. It specifically utilizes
 * the 'sparkPartitionSize' configuration to determine the size of data to prefetch, optimizing for
 * Spark's data processing patterns.
 */
public class SequentialLogicalIOImpl extends DefaultLogicalIOImpl {
  private final SequentialPrefetcher prefetcher;
  private final S3URI s3URI;
  private final BlobStore blobStore;

  /**
   * Constructs an instance of SequentialLogicalIOImpl.
   *
   * @param s3URI the S3 URI of the object fetched
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration configuration for this logical IO implementation
   * @param blobStore a data cache
   */
  public SequentialLogicalIOImpl(
      @NonNull S3URI s3URI,
      @NonNull PhysicalIO physicalIO,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull BlobStore blobStore) {
    super(s3URI, physicalIO, telemetry);
    this.s3URI = s3URI;
    this.blobStore = blobStore;
    this.prefetcher =
        new SequentialPrefetcher(s3URI, physicalIO, telemetry, logicalIOConfiguration);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param position the position to begin reading from
   * @return an unsigned int representing the byte that was read
   * @throws IOException IO error, if incurred.
   */
  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    prefetcher.prefetch(position);
    return super.read(buf, off, len, position);
  }

  @Override
  public void close() throws IOException {
    ObjectKey objectKey = ObjectKey.builder().s3URI(s3URI).etag(metadata().getEtag()).build();
    blobStore.evictKey(objectKey);
  }
}
