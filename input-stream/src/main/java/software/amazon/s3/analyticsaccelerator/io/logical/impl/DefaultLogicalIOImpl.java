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
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.TelemetryLevel;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** The default implementation of a LogicalIO layer. Will be used for all non-parquet files. */
public class DefaultLogicalIOImpl implements LogicalIO {

  private static final String OPERATION_LOGICAL_READ = "logical.read";

  // Dependencies
  private final S3URI s3URI;
  private final PhysicalIO physicalIO;
  private final Telemetry telemetry;

  // When is the LogicalIO instance created?
  private final long birthTimestamp = System.nanoTime();

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param s3URI the S3 URI of the object fetched
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param telemetry an instance of telemetry
   */
  public DefaultLogicalIOImpl(
      @NonNull S3URI s3URI, @NonNull PhysicalIO physicalIO, @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.physicalIO = physicalIO;
    this.telemetry = telemetry;
  }

  /**
   * Reads a byte from the given position.
   *
   * @param position the position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException IO error, if incurred.
   */
  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
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
    // Perform read
    return telemetry.measureConditionally(
        TelemetryLevel.VERBOSE,
        () ->
            Operation.builder()
                .name(OPERATION_LOGICAL_READ)
                .attribute(StreamAttributes.logicalReadPosition(position))
                .attribute(StreamAttributes.logicalReadLength(len))
                .attribute(StreamAttributes.uri(s3URI))
                .attribute(
                    StreamAttributes.logicalIORelativeTimestamp(System.nanoTime() - birthTimestamp))
                .build(),
        () -> physicalIO.read(buf, off, len, position),
        bytesRead -> bytesRead > 1);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    long contentLength = metadata().getContentLength();
    long startOfRead = Math.max(0, contentLength - len);

    return telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_LOGICAL_READ)
                .attribute(StreamAttributes.logicalReadPosition(startOfRead))
                .attribute(StreamAttributes.uri(s3URI))
                .attribute(
                    StreamAttributes.logicalIORelativeTimestamp(System.nanoTime() - birthTimestamp))
                .build(),
        () -> physicalIO.readTail(buf, off, len));
  }

  /**
   * Returns object metadata.
   *
   * @return object metadata
   * @throws IOException if an I/O error occurs
   */
  @Override
  public ObjectMetadata metadata() throws IOException {
    return this.physicalIO.metadata();
  }

  protected PhysicalIO getPhysicalIO() {
    return this.physicalIO;
  }
  /**
   * Closes associate resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    physicalIO.close();
  }
}
