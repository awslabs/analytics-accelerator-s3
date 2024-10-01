package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.io.Closeable;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A Blob representing an object. */
public class Blob implements Closeable {
  private static final Logger LOG = LogManager.getLogger(Blob.class);
  private static final String OPERATION_EXECUTE = "blob.execute";

  private final S3URI s3URI;
  private final BlockManager blockManager;
  private final MetadataStore metadataStore;
  private final Telemetry telemetry;

  /**
   * Construct a new Blob.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore the MetadataStore in the stream
   * @param blockManager the BlockManager for this object
   * @param telemetry an instance of {@link Telemetry} to use
   */
  public Blob(
      @NonNull S3URI s3URI,
      @NonNull MetadataStore metadataStore,
      @NonNull BlockManager blockManager,
      @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
    this.blockManager = blockManager;
    this.telemetry = telemetry;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  public int read(long pos) {
    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");
    blockManager.makePositionAvailable(pos, ReadMode.SYNC);
    return blockManager.getBlock(pos).get().read(pos);
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
  public int read(byte[] buf, int off, int len, long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    blockManager.makeRangeAvailable(pos, len, ReadMode.SYNC);

    long nextPosition = pos;
    int numBytesRead = 0;

    while (numBytesRead < len && nextPosition < contentLength()) {
      Block nextBlock =
          blockManager
              .getBlock(nextPosition)
              .orElseThrow(
                  () -> new IllegalStateException("This block should have been available."));

      int bytesRead = nextBlock.read(buf, off + numBytesRead, len - numBytesRead, nextPosition);

      if (bytesRead == -1) {
        return numBytesRead;
      }

      numBytesRead = numBytesRead + bytesRead;
      nextPosition += bytesRead;
    }

    return numBytesRead;
  }

  /**
   * Execute an IOPlan.
   *
   * @param plan the IOPlan to execute
   * @return the status of execution
   */
  public IOPlanExecution execute(IOPlan plan) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.ioPlan(plan))
                .build(),
        () -> {
          try {
            plan.getPrefetchRanges()
                .forEach(
                    range -> {
                      this.blockManager.makeRangeAvailable(
                          range.getStart(), range.getLength(), ReadMode.ASYNC);
                    });

            return IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build();
          } catch (Exception e) {
            LOG.error("Failed to submit IOPlan to PhysicalIO", e);
            return IOPlanExecution.builder().state(IOPlanState.FAILED).build();
          }
        });
  }

  private long contentLength() {
    return metadataStore.get(s3URI).getContentLength();
  }

  @Override
  public void close() {
    this.blockManager.close();
  }
}
