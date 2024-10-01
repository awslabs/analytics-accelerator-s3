package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.request.ObjectContent;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.request.Referrer;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import com.amazon.connector.s3.util.StreamUtils;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

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

  @Getter private final long start;
  @Getter private final long end;
  @Getter private final long generation;

  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";

  /**
   * Constructs a Block.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   */
  public Block(
      @NonNull S3URI s3URI,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode) {
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
                    .referrer(new Referrer(range.toHttpString(), readMode))
                    .build()));
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
   * Gets size of the block
   *
   * @return size of the block in bytes
   */
  public long getLength() {
    return end - start + 1;
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
