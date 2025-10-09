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
package software.amazon.s3.analyticsaccelerator.io.physical.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Blob;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** A PhysicalIO frontend */
public class PhysicalIOImpl implements PhysicalIO {
  private MetadataStore metadataStore;
  private BlobStore blobStore;
  private final Telemetry telemetry;
  private final OpenStreamInformation openStreamInformation;
  private ObjectKey objectKey;
  private final ObjectMetadata metadata;
  private final ExecutorService threadPool;
  private final PhysicalIOConfiguration configuration;

  private final long physicalIOBirth = System.nanoTime();

  private static final String OPERATION_READ = "physical.io.read";
  private static final String OPERATION_EXECUTE = "physical.io.execute";
  private static final String FLAVOR_TAIL = "tail";
  private static final String FLAVOR_BYTE = "byte";
  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(PhysicalIOImpl.class);

  /**
   * Construct a new instance of PhysicalIOV2.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore a metadata cache
   * @param blobStore a data cache
   * @param telemetry The {@link Telemetry} to use to report measurements.
   * @param openStreamInformation contains stream information
   * @param threadPool Thread pool for async operations
   * @param configuration PhysicalIO Configuration
   */
  public PhysicalIOImpl(
      @NonNull S3URI s3URI,
      @NonNull MetadataStore metadataStore,
      @NonNull BlobStore blobStore,
      @NonNull Telemetry telemetry,
      @NonNull OpenStreamInformation openStreamInformation,
      @NonNull ExecutorService threadPool,
      @NonNull PhysicalIOConfiguration configuration)
      throws IOException {
    this.metadataStore = metadataStore;
    this.blobStore = blobStore;
    this.telemetry = telemetry;
    this.openStreamInformation = openStreamInformation;
    this.metadata = this.metadataStore.get(s3URI, openStreamInformation);
    this.objectKey = ObjectKey.builder().s3URI(s3URI).etag(metadata.getEtag()).build();
    this.threadPool = threadPool;
    this.configuration = configuration;
  }

  /**
   * Returns object metadata.
   *
   * @return the metadata of the object.
   */
  @Override
  public ObjectMetadata metadata() {
    return metadata;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos the position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    try {
      return this.telemetry.measureVerbose(
          () ->
              Operation.builder()
                  .name(OPERATION_READ)
                  .attribute(StreamAttributes.variant(FLAVOR_BYTE))
                  .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                  .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                  .attribute(StreamAttributes.range(pos, pos))
                  .attribute(
                      StreamAttributes.physicalIORelativeTimestamp(
                          System.nanoTime() - physicalIOBirth))
                  .build(),
          () -> blobStore.get(this.objectKey, this.metadata, openStreamInformation).read(pos));
    } catch (Exception e) {
      handleOperationExceptions(e);
      throw e;
    }
  }

  /**
   * Reads request data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    try {
      return this.telemetry.measureVerbose(
          () ->
              Operation.builder()
                  .name(OPERATION_READ)
                  .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                  .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                  .attribute(StreamAttributes.range(pos, pos + len - 1))
                  .attribute(
                      StreamAttributes.physicalIORelativeTimestamp(
                          System.nanoTime() - physicalIOBirth))
                  .build(),
          () ->
              blobStore
                  .get(objectKey, this.metadata, openStreamInformation)
                  .read(buf, off, len, pos));
    } catch (Exception e) {
      handleOperationExceptions(e);
      throw e;
    }
  }

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    long contentLength = contentLength();
    try {
      return telemetry.measureVerbose(
          () ->
              Operation.builder()
                  .name(OPERATION_READ)
                  .attribute(StreamAttributes.variant(FLAVOR_TAIL))
                  .attribute(StreamAttributes.uri(objectKey.getS3URI()))
                  .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                  .attribute(StreamAttributes.range(contentLength - len, contentLength - 1))
                  .attribute(
                      StreamAttributes.physicalIORelativeTimestamp(
                          System.nanoTime() - physicalIOBirth))
                  .build(),
          () ->
              blobStore
                  .get(objectKey, this.metadata, openStreamInformation)
                  .read(buf, off, len, contentLength - len));
    } catch (Exception e) {
      handleOperationExceptions(e);
      throw e;
    }
  }

  /**
   * Async method capable of executing a logical IO plan.
   *
   * @param ioPlan the plan to execute asynchronously
   * @param readMode the read mode for which this IoPlan is being executed
   * @return an IOPlanExecution object tracking the execution of the submitted plan
   */
  @Override
  public IOPlanExecution execute(IOPlan ioPlan, ReadMode readMode) {
    if (readMode.coalesceRequests() && configuration.isRequestCoalesce()) {
      int rangeSizeBeforeCoalescing = ioPlan.getPrefetchRanges().size();
      ioPlan.coalesce(configuration.getRequestCoalesceTolerance());
      int coalescedRangeSize = ioPlan.getPrefetchRanges().size();

      if (readMode == ReadMode.READ_VECTORED) {
        openStreamInformation
            .getRequestCallback()
            .onReadVectored(rangeSizeBeforeCoalescing, coalescedRangeSize);
      }
    }
    return telemetry.measureVerbose(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(objectKey.getS3URI()))
                .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                .attribute(StreamAttributes.ioPlan(ioPlan))
                .attribute(
                    StreamAttributes.physicalIORelativeTimestamp(
                        System.nanoTime() - physicalIOBirth))
                .build(),
        () ->
            blobStore
                .get(objectKey, this.metadata, openStreamInformation)
                .execute(ioPlan, readMode));
  }

  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification =
          "This is complaining about `executor.submit`. In this case we do not have any use for this Future")
  @Override
  public void readVectored(
      List<ObjectRange> objectRanges,
      IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release)
      throws IOException {
    Blob blob = blobStore.get(objectKey, this.metadata, openStreamInformation);

    makeReadVectoredRangesAvailable(objectRanges);

    for (ObjectRange objectRange : objectRanges) {
      ByteBuffer buffer = allocate.apply(objectRange.getLength());
      threadPool.submit(
          () -> {
            try {
              LOG.debug(
                  "Starting readVectored for key: {}, range: {} - {}",
                  objectKey.getS3URI(),
                  objectRange.getOffset(),
                  objectRange.getOffset() + objectRange.getLength() - 1);

              if (buffer.isDirect()) {
                // Direct buffers do not support the buffer.array() method, so we need to read into
                // them using a temp buffer.
                readIntoDirectBuffer(buffer, blob, objectRange);
                buffer.flip();
              } else {
                // there is no use of a temp byte buffer, or buffer.put() calls,
                // so flip() is not needed.
                blob.read(buffer.array(), 0, objectRange.getLength(), objectRange.getOffset());
              }
              objectRange.getByteBuffer().complete(buffer);
            } catch (Exception e) {
              objectRange.getByteBuffer().completeExceptionally(e);
              release.accept(buffer);
            }
          });
    }
  }

  private void readIntoDirectBuffer(ByteBuffer buffer, Blob blob, ObjectRange range)
      throws IOException {
    int length = range.getLength();
    if (length == 0) {
      // no-op
      return;
    }

    int readBytes = 0;
    long position = range.getOffset();
    int tmpBufferMaxSize = Math.min(TMP_BUFFER_MAX_SIZE, length);
    byte[] tmp = new byte[tmpBufferMaxSize];
    while (readBytes < length) {
      int currentLength =
          (readBytes + tmpBufferMaxSize) < length ? tmpBufferMaxSize : (length - readBytes);
      LOG.debug(
          "Reading {} bytes from position {} (bytes read={}", currentLength, position, readBytes);
      blob.read(tmp, 0, currentLength, position);
      buffer.put(tmp, 0, currentLength);
      position = position + currentLength;
      readBytes = readBytes + currentLength;
    }
  }

  /**
   * Does the block creation for the read vectored ranges.
   *
   * @param objectRanges Vectored ranges to fetch
   */
  private void makeReadVectoredRangesAvailable(List<ObjectRange> objectRanges) {
    List<Range> ranges = new ArrayList<>();

    for (ObjectRange objectRange : objectRanges) {
      ranges.add(
          new Range(
              objectRange.getOffset(), objectRange.getOffset() + objectRange.getLength() - 1));
    }

    execute(new IOPlan(ranges), ReadMode.READ_VECTORED);
  }

  /**
   * Handles exceptions from operations by evicting keys from caches when appropriate.
   *
   * @param e The exception to handle
   */
  private void handleOperationExceptions(Exception e) {
    boolean shouldEvict = false;

    // Check for precondition failed errors (412)
    if (e.getCause() != null
        && e.getCause().getMessage() != null
        && e.getCause().getMessage().contains("Status Code: 412")) {
      shouldEvict = true;
    }

    // Evict keys if needed
    if (shouldEvict) {
      try {
        metadataStore.evictKey(this.objectKey.getS3URI());
      } finally {
        blobStore.evictKey(this.objectKey);
      }
    }
  }

  private long contentLength() {
    return metadata().getContentLength();
  }

  @Override
  public void close(boolean shouldEvict) throws IOException {
    if (shouldEvict) {
      blobStore.evictKey(this.objectKey);
    }
  }

  @Override
  public void close() throws IOException {
    close(false);
  }
}
