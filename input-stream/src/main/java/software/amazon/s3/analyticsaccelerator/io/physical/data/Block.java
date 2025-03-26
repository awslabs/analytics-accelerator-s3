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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
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
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;
import software.amazon.s3.analyticsaccelerator.util.StreamUtils;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {
  private ObjectContent source;
  private byte[] data;
  private final ObjectKey objectKey;
  private final Range range;
  private final Telemetry telemetry;
  private final ObjectClient objectClient;
  private final StreamContext streamContext;
  private final ReadMode readMode;
  private final Referrer referrer;
  private final long readTimeout;
  private final int readRetryCount;

  @Getter private final long start;
  @Getter private final long end;
  @Getter private final long generation;

  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";

  private static final Logger LOG = LoggerFactory.getLogger(Block.class);

  /**
   * Constructs a Block data.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param readTimeout Timeout duration (in milliseconds) for reading a block object from S3
   * @param readRetryCount Number of retries for block read failure
   */
  public Block(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode,
      long readTimeout,
      int readRetryCount)
      throws IOException {

    this(
        objectKey,
        objectClient,
        telemetry,
        start,
        end,
        generation,
        readMode,
        readTimeout,
        readRetryCount,
        null);
  }

  /**
   * Constructs a Block data.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param readTimeout Timeout duration (in milliseconds) for reading a block object from S3
   * @param readRetryCount Number of retries for block read failure
   * @param streamContext contains audit headers to be attached in the request header
   */
  public Block(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode,
      long readTimeout,
      int readRetryCount,
      StreamContext streamContext)
      throws IOException {

    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);
    Preconditions.checkArgument(
        0 < readTimeout, "`readTimeout` must be greater than 0; was %s", readTimeout);
    Preconditions.checkArgument(
        0 < readRetryCount, "`readRetryCount` must be greater than 0; was %s", readRetryCount);

    this.start = start;
    this.end = end;
    this.generation = generation;
    this.telemetry = telemetry;
    this.objectKey = objectKey;
    this.range = new Range(start, end);
    this.objectClient = objectClient;
    this.streamContext = streamContext;
    this.readMode = readMode;
    this.referrer = new Referrer(range.toHttpString(), readMode);
    this.readTimeout = readTimeout;
    this.readRetryCount = readRetryCount;

    generateSourceAndData();
  }

  /** Method to help construct source and data */
  private void generateSourceAndData() throws IOException {
    int retries = 0;
    while (retries < this.readRetryCount) {
      try {
        GetRequest getRequest =
            GetRequest.builder()
                .s3Uri(this.objectKey.getS3URI())
                .range(this.range)
                .etag(this.objectKey.getEtag())
                .referrer(referrer)
                .build();

        this.source = objectClient.getObject(getRequest, streamContext).join();

        // Handle IOExceptions when converting stream to byte array
        try {
          this.data = toByteArray(this.source, this.objectKey, this.range, this.readTimeout);
        } catch (IOException | TimeoutException e) {
          throw new RuntimeException(
              "Error while converting InputStream to byte array", e);
        }

        return; // Successfully generated source and data, exit loop
      } catch (RuntimeException e) {
        retries++;
        LOG.debug(
            "Retry {}/{} - Failed to fetch block data due to: {}",
            retries,
            this.readRetryCount,
            e.getMessage());

        if (retries >= this.readRetryCount) {
          LOG.error("Max retries reached. Unable to fetch block data.");
          throw new IOException("Failed to fetch block data after retries", e);
        }
      }
    }
  }


  private static final int BUFFER_SIZE = 8 * ONE_KB;

  private synchronized byte[] toByteArray(
      ObjectContent objectContent, ObjectKey objectKey, Range range, long timeoutMs)
      throws IOException, TimeoutException {
    InputStream inStream = objectContent.getStream();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Void> future =
        executorService.submit(
            () -> {
              try {
                int numBytesRead;
                LOG.info(
                    "Starting to read from InputStream for Block s3URI={}, etag={}, start={}, end={}",
                    objectKey.getS3URI(),
                    objectKey.getEtag(),
                    range.getStart(),
                    range.getEnd());
                while ((numBytesRead = inStream.read(buffer, 0, buffer.length)) != -1) {
                  outStream.write(buffer, 0, numBytesRead);
                }
                LOG.info(
                    "Successfully read from InputStream for Block numBytesRead={}, s3URI={}, etag={}, start={}, end={}",
                    numBytesRead,
                    objectKey.getS3URI(),
                    objectKey.getEtag(),
                    range.getStart(),
                    range.getEnd());
                return null;
              } finally {
                inStream.close();
              }
            });

    try {
      future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      LOG.info(
          "Reading from InputStream has timed out for Block s3URI={}, etag={}, start={}, end={}",
          objectKey.getS3URI(),
          objectKey.getEtag(),
          range.getStart(),
          range.getEnd());
      throw new TimeoutException("Read operation timed out");
    } catch (Exception e) {
      throw new IOException("Error reading stream", e);
    } finally {
      executorService.shutdown();
    }

    return outStream.toByteArray();
  }



  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    byte[] content = this.data;
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
   * @throws IOException if an I/O error occurs
   */
  public int read(byte @NonNull [] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    byte[] content = this.data;
    int contentOffset = posToOffset(pos);
    int available = content.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = content[contentOffset + i];
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

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {
    // Only the source needs to be canceled, the continuation will cancel on its own
  }
}
