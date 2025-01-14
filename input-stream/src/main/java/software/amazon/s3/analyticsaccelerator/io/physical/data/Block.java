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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;
import software.amazon.s3.analyticsaccelerator.util.StreamUtils;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {
  private CompletableFuture<ObjectContent> source;
  private CompletableFuture<byte[]> data;
  private final S3URI s3URI;
  private final Range range;
  private final Telemetry telemetry;
  private final String etag;

  @Getter private final long start;
  @Getter private final long end;
  @Getter private final long generation;

  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";

  /**
   * Constructs a Block data.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param etag the etag of the S3 object
   */
  public Block(
      @NonNull S3URI s3URI,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode,
      String etag) {

    this(s3URI, objectClient, telemetry, start, end, generation, readMode, null, etag);
  }

  /**
   * Constructs a Block data.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param streamContext contains audit headers to be attached in the request header
   * @param etag the etag of the S3 object
   */
  public Block(
      @NonNull S3URI s3URI,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode,
      StreamContext streamContext,
      String etag) {

    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);

    this.start = start;
    this.end = end;
    this.generation = generation;
    this.telemetry = telemetry;
    this.s3URI = s3URI;
    this.range = new Range(start, end);
    this.etag = etag;

    this.source =
        this.telemetry.measureCritical(
            () ->
                Operation.builder()
                    .name(OPERATION_BLOCK_GET_ASYNC)
                    .attribute(StreamAttributes.uri(this.s3URI))
                    .attribute(StreamAttributes.range(this.range))
                    .attribute(StreamAttributes.generation(generation))
                    .build(),
            objectClient.getObject(
                GetRequest.builder()
                    .s3Uri(this.s3URI)
                    .range(this.range)
                    .etag(this.etag)
                    .referrer(new Referrer(range.toHttpString(), readMode))
                    .build(),
                streamContext));
    this.data = this.source.thenApply(StreamUtils::toByteArray);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  public int read(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    byte[] content = this.getData();
    return Byte.toUnsignedInt(content[posToOffset(pos)]);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  public int read(byte @NonNull [] buf, int off, int len, long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    byte[] content = this.getData();
    int available = content.length - posToOffset(pos);
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = content[posToOffset(pos) + i];
    }

    return bytesToCopy;
  }

  /**
   * Does this block contain the position?
   *
   * @param pos the position
   * @return true if the byte at the position is contained by this block
   */
  public boolean contains(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return start <= pos && pos <= end;
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - start);
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. This method will block until the
   * data is fully available.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   */
  private byte[] getData() {
    return this.telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_BLOCK_GET_JOIN)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(this.range))
                .attribute(StreamAttributes.rangeLength(this.range.getLength()))
                .build(),
        this.data);
  }

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {
    // Only the source needs to be canceled, the continuation will cancel on its own
    this.source.cancel(false);
  }
}
