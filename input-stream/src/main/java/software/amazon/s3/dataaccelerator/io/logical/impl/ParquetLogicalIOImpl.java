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

import java.io.IOException;
import java.util.Optional;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.logical.parquet.ParquetMetadataParsingTask;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.util.S3URI;

/**
 * A Parquet-aware implementation of a LogicalIO layer. It is capable of prefetching file tails,
 * parsing Parquet metadata and prefetching columns based on recent access patterns.
 */
public class ParquetLogicalIOImpl extends DefaultLogicalIOImpl {
  // Dependencies
  private final ParquetPrefetcher parquetPrefetcher;
  private final PhysicalIO physicalIO;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetLogicalIOImpl.class);

  boolean columnsRead = false;

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param s3Uri s3Uri pointing to object to fetch
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration configuration for this logical IO implementation
   * @param parquetColumnPrefetchStore object where Parquet usage information is aggregated
   */
  public ParquetLogicalIOImpl(
      @NonNull S3URI s3Uri,
      @NonNull PhysicalIO physicalIO,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull ParquetColumnPrefetchStore parquetColumnPrefetchStore) {
    super(physicalIO);

    // Initialise prefetcher and start prefetching
    this.parquetPrefetcher =
        new ParquetPrefetcher(
            s3Uri, physicalIO, telemetry, logicalIOConfiguration, parquetColumnPrefetchStore);

    this.physicalIO = physicalIO;
    this.parquetPrefetcher.prefetchFooterAndBuildMetadata();
  }

  /**
   * Reads Reads data into the provided buffer
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
    // Perform async prefetching before doing the blocking read
    this.parquetPrefetcher.prefetchRemainingColumnChunk(position, len);
    Optional<String> columnName = this.parquetPrefetcher.addToRecentColumnList(position, len);

    if (columnName.isPresent()) {
      columnsRead = true;
    }

    return super.read(buf, off, len, position);
  }

  @Override
  public void close() throws IOException {
    if (columnsRead) {
      LOG.info("Columns read!, removing from cache");
      physicalIO.close();
    }

  }

}
