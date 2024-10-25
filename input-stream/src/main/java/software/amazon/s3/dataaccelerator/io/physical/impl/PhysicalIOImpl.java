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
package software.amazon.s3.dataaccelerator.io.physical.impl;

import java.io.IOException;
import lombok.NonNull;
import software.amazon.s3.dataaccelerator.common.Preconditions;
import software.amazon.s3.dataaccelerator.common.telemetry.Operation;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.dataaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.dataaccelerator.request.ObjectMetadata;
import software.amazon.s3.dataaccelerator.util.S3URI;
import software.amazon.s3.dataaccelerator.util.StreamAttributes;

/** A PhysicalIO frontend */
public class PhysicalIOImpl implements PhysicalIO {
  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final BlobStore blobStore;
  private final Telemetry telemetry;

  private static final String OPERATION_READ = "physical.io.read";
  private static final String OPERATION_EXECUTE = "physical.io.execute";
  private static final String FLAVOR_TAIL = "tail";
  private static final String FLAVOR_BYTE = "byte";
  /**
   * Construct a new instance of PhysicalIOV2.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore a metadata cache
   * @param blobStore a data cache
   * @param telemetry The {@link Telemetry} to use to report measurements.
   */
  public PhysicalIOImpl(
      @NonNull S3URI s3URI,
      @NonNull MetadataStore metadataStore,
      @NonNull BlobStore blobStore,
      @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
    this.blobStore = blobStore;
    this.telemetry = telemetry;
  }

  /**
   * Returns object metadata.
   *
   * @return the metadata of the object.
   */
  @Override
  public ObjectMetadata metadata() {
    return metadataStore.get(s3URI);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos the position to read
   * @return an unsigned int representing the byte that was read
   */
  @Override
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    return this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.variant(FLAVOR_BYTE))
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(pos, pos))
                .build(),
        () -> blobStore.get(s3URI).read(pos));
  }

  /**
   * Reads request data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  @Override
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    return this.telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(pos, pos + len - 1))
                .build(),
        () -> blobStore.get(s3URI).read(buf, off, len, pos));
  }

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   */
  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    long contentLength = contentLength();
    return telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_READ)
                .attribute(StreamAttributes.variant(FLAVOR_TAIL))
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(contentLength - len, contentLength - 1))
                .build(),
        () -> blobStore.get(s3URI).read(buf, off, len, contentLength - len));
  }

  /**
   * Async method capable of executing a logical IO plan.
   *
   * @param ioPlan the plan to execute asynchronously
   * @return an IOPlanExecution object tracking the execution of the submitted plan
   */
  @Override
  public IOPlanExecution execute(IOPlan ioPlan) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.ioPlan(ioPlan))
                .build(),
        () -> blobStore.get(s3URI).execute(ioPlan));
  }

  private long contentLength() {
    return metadata().getContentLength();
  }

  @Override
  public void close() throws IOException {
    this.blobStore.removeFromBlob(s3URI);
  }
}
