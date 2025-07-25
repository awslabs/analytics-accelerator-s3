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
package software.amazon.s3.analyticsaccelerator.io.physical.reader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Block;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;
import software.amazon.s3.analyticsaccelerator.util.retry.DefaultRetryStrategyImpl;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryPolicy;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryStrategy;

/**
 * {@code StreamReader} is responsible for asynchronously reading a range of bytes from an object in
 * S3 and populating the corresponding {@link Block}s with the downloaded data.
 *
 * <p>It submits the read task to a provided {@link ExecutorService}, allowing non-blocking
 * operation.
 */
public class StreamReader implements Closeable {
  private final ObjectClient objectClient;
  private final ObjectKey objectKey;
  private final ExecutorService threadPool;
  // Callback function to remove failed blocks from the data store
  private final Consumer<List<Block>> removeBlocksFunc;
  private final Metrics aggregatingMetrics;
  private final OpenStreamInformation openStreamInformation;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration physicalIOConfiguration;

  private final RetryStrategy retryStrategy;

  private static final String OPERATION_GET_OBJECT = "s3.stream.get";
  private static final String OPERATION_STREAM_READ = "s3.stream.read";

  private static final Logger LOG = LoggerFactory.getLogger(StreamReader.class);

  /**
   * Constructs a {@code StreamReader} instance for reading objects from S3.
   *
   * @param objectClient the client used to fetch S3 object content
   * @param objectKey the key identifying the S3 object and its ETag
   * @param threadPool an {@link ExecutorService} used for async I/O operations
   * @param removeBlocksFunc a function to remove blocks from
   * @param aggregatingMetrics the metrics aggregator for performance or usage monitoring
   * @param openStreamInformation contains stream information
   * @param telemetry an instance of {@link Telemetry} to use
   * @param physicalIOConfiguration an instance of {@link PhysicalIOConfiguration} to use
   */
  public StreamReader(
      @NonNull ObjectClient objectClient,
      @NonNull ObjectKey objectKey,
      @NonNull ExecutorService threadPool,
      @NonNull Consumer<List<Block>> removeBlocksFunc,
      @NonNull Metrics aggregatingMetrics,
      @NonNull OpenStreamInformation openStreamInformation,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration physicalIOConfiguration) {
    this.objectClient = objectClient;
    this.objectKey = objectKey;
    this.threadPool = threadPool;
    this.removeBlocksFunc = removeBlocksFunc;
    this.aggregatingMetrics = aggregatingMetrics;
    this.openStreamInformation = openStreamInformation;
    this.telemetry = telemetry;
    this.physicalIOConfiguration = physicalIOConfiguration;
    this.retryStrategy = createRetryStrategy();
  }

  /**
   * Helper to construct retryStrategy
   *
   * @return a {@link RetryStrategy} to retry when timeouts are set
   * @throws RuntimeException if all retries fails and an error occurs
   */
  @SuppressWarnings("unchecked")
  private RetryStrategy createRetryStrategy() {
    RetryStrategy base = new DefaultRetryStrategyImpl();
    RetryStrategy provided = this.openStreamInformation.getRetryStrategy();

    if (provided != null) {
      base = base.merge(provided);
    }

    if (this.physicalIOConfiguration.getBlockReadTimeout() > 0) {
      RetryPolicy timeoutPolicy =
          RetryPolicy.builder()
              .handle(
                  IOException.class,
                  InterruptedException.class,
                  TimeoutException.class,
                  ExecutionException.class)
              .withMaxRetries(this.physicalIOConfiguration.getBlockReadRetryCount())
              .build();
      base = base.amend(timeoutPolicy);
    }
    return base;
  }

  /**
   * Asynchronously reads a range of bytes from the S3 object and fills the corresponding {@link
   * Block}s with data. The byte range is determined by the start of the first block and the end of
   * the last block.
   *
   * @param blocks the list of {@link Block}s to be populated; must not be empty and must be sorted
   *     by offset
   * @param readMode the mode in which the read is being performed (used for tracking or metrics)
   * @throws IllegalArgumentException if the {@code blocks} list is empty
   * @implNote This method uses a fire-and-forget strategy and doesn't return a {@code Future};
   *     failures are logged or wrapped in a {@code IOException}.
   */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification = "Intentional fire-and-forget task")
  public void read(@NonNull final List<Block> blocks, ReadMode readMode) {
    Preconditions.checkArgument(!blocks.isEmpty(), "`blocks` list must not be empty");
    threadPool.submit(processReadTask(blocks, readMode));
  }

  /**
   * Creates a runnable task that handles the complete read operation for a list of data blocks.
   * This includes fetching the S3 object content and populating each block with data.
   *
   * @param blocks the list of data blocks to populate with data
   * @param readMode the mode in which the read is being performed
   * @return a Runnable that executes the read operation asynchronously
   */
  private Runnable processReadTask(final List<Block> blocks, ReadMode readMode) {
    return () ->
        this.telemetry.measureCritical(
            () ->
                Operation.builder()
                    .name(OPERATION_STREAM_READ)
                    .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                    .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                    .attribute(
                        StreamAttributes.effectiveRange(
                            blocks.get(0).getBlockKey().getRange().getStart(),
                            blocks.get(blocks.size() - 1).getBlockKey().getRange().getEnd()))
                    .build(),
            () -> {
              // Calculate the byte range needed to cover all blocks
              Range requestRange = computeRange(blocks);

              // Build S3 GET request with range, ETag validation, and referrer info
              GetRequest getRequest =
                  GetRequest.builder()
                      .s3Uri(objectKey.getS3URI())
                      .range(requestRange)
                      .etag(objectKey.getEtag())
                      .referrer(new Referrer(requestRange.toHttpString(), readMode))
                      .build();

              openStreamInformation.getRequestCallback().onGetRequest();

              // Fetch the object content from S3
              ObjectContent objectContent;
              try {
                objectContent = this.retryStrategy.get(() -> fetchObjectContent(getRequest));
              } catch (IOException e) {
                LOG.error("IOException while fetching object content", e);
                removeNonFilledBlocksFromStore(blocks);
                return;
              }

              if (objectContent == null) {
                // Couldn't successfully get the response from S3.
                // Remove blocks from store and complete async operation
                removeNonFilledBlocksFromStore(blocks);
                return;
              }

              // Process the input stream and populate data blocks
              try (InputStream inputStream = objectContent.getStream()) {
                boolean success =
                    readBlocksFromStream(inputStream, blocks, requestRange.getStart());
                if (!success) {
                  removeNonFilledBlocksFromStore(blocks);
                }
              } catch (EOFException e) {
                LOG.error("EOFException while reading blocks", e);
                removeNonFilledBlocksFromStore(blocks);
              } catch (IOException e) {
                LOG.error("IOException while reading blocks", e);
                removeNonFilledBlocksFromStore(blocks);
              } catch (Exception e) {
                LOG.error("Unexpected exception while reading blocks", e);
                removeNonFilledBlocksFromStore(blocks);
              }
            });
  }

  /**
   * Sequentially reads data from the input stream to populate all blocks. Maintains current offset
   * position to handle potential gaps between blocks.
   *
   * @param inputStream the input stream to read data from
   * @param blocks the list of data blocks to populate
   * @param initialOffset the starting offset position in the stream
   * @return true if all blocks were successfully read, false otherwise
   * @throws IOException if an I/O error occurs while reading from the stream
   */
  private boolean readBlocksFromStream(
      InputStream inputStream, List<Block> blocks, long initialOffset) throws IOException {
    long currentOffset = initialOffset;
    for (Block block : blocks) {
      boolean success = readBlock(inputStream, block, currentOffset);
      if (!success) {
        return false;
      }

      // Update current position after reading this block
      currentOffset += block.getLength();
    }
    return true;
  }

  /**
   * Computes the overall byte range needed to fetch all blocks in a single S3 request. Uses the
   * start of the first block and end of the last block.
   *
   * @param blocks the list of data blocks, must be non-empty and sorted by offset
   * @return the Range covering all blocks from first start to last end
   */
  private Range computeRange(List<Block> blocks) {
    long rangeStart = blocks.get(0).getBlockKey().getRange().getStart();
    long rangeEnd = blocks.get(blocks.size() - 1).getBlockKey().getRange().getEnd();
    return new Range(rangeStart, rangeEnd);
  }

  /**
   * Fetches object content from S3 using the provided request. Returns null if the request fails,
   * allowing caller to handle gracefully.
   *
   * @param getRequest the S3 GET request containing object URI, range, and ETag
   * @return the ObjectContent containing the S3 object data stream, or null if request fails
   */
  private ObjectContent fetchObjectContent(GetRequest getRequest) throws IOException {
    this.aggregatingMetrics.add(MetricKey.GET_REQUEST_COUNT, 1);
    return telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_GET_OBJECT)
                .attribute(StreamAttributes.uri(getRequest.getS3Uri()))
                .attribute(StreamAttributes.rangeLength(getRequest.getRange().getLength()))
                .attribute(StreamAttributes.range(getRequest.getRange()))
                .build(),
        this.objectClient.getObject(getRequest, this.openStreamInformation),
        this.physicalIOConfiguration.getBlockReadTimeout());
  }

  /**
   * Reads data for a single block from the input stream. Handles skipping to the correct position
   * and reading the exact number of bytes.
   *
   * @param inputStream the input stream to read from
   * @param block the data block to populate with read data
   * @param currentPos the current position in the stream
   * @return true if the block was successfully read and populated, false otherwise
   * @throws IOException if an I/O error occurs while reading or skipping bytes
   */
  private boolean readBlock(InputStream inputStream, Block block, long currentPos)
      throws IOException {
    long blockStart = block.getBlockKey().getRange().getStart();
    int blockSize = block.getLength();

    // Skip bytes if there's a gap between current position and block start
    if (!skipToBlockStart(inputStream, blockStart, currentPos)) {
      return false;
    }

    // Read the exact number of bytes for this block
    byte[] blockData = readExactBytes(inputStream, blockSize);

    // Populate the block with the read data
    block.setData(blockData);
    return true;
  }

  /**
   * Skips bytes in the input stream to reach the start position of a block. Handles cases where
   * blocks may not be contiguous in the stream.
   *
   * <p><b>Examples:</b>
   *
   * <ul>
   *   <li>If currentPos=100 and blockStart=150, skips 50 bytes to reach block start
   *   <li>If currentPos=200 and blockStart=200, no skipping needed (already at position)
   *   <li>If currentPos=300 and blockStart=250, no skipping needed (past target position)
   * </ul>
   *
   * @param inputStream the input stream to skip bytes from
   * @param blockStart the target start position of the block
   * @param currentPos the current position in the stream
   * @return true if successfully skipped to the target position, false if EOF reached
   * @throws IOException if an I/O error occurs while skipping bytes
   */
  private boolean skipToBlockStart(InputStream inputStream, long blockStart, long currentPos)
      throws IOException {
    long skipBytes = blockStart - currentPos;
    if (skipBytes <= 0) {
      return true; // Already at or past the target position
    }

    // Skip bytes in chunks until we reach the target position
    long totalSkipped = 0;
    while (totalSkipped < skipBytes) {
      long skipped = inputStream.skip(skipBytes - totalSkipped);
      if (skipped <= 0) {
        return false; // Unable to skip, likely EOF
      }
      totalSkipped += skipped;
    }

    return true;
  }

  /**
   * Attempts to read exactly {@code size} bytes from the input stream. Returns {@code null} if the
   * end of the stream is reached before reading all bytes.
   *
   * @param inputStream The input stream to read from.
   * @param size Number of bytes to read.
   * @return A byte array of exactly {@code size} bytes, or {@code null} on premature EOF.
   * @throws IOException if an I/O error occurs while reading from the stream
   * @throws EOFException if the end of stream is reached before reading all requested bytes
   */
  private byte[] readExactBytes(InputStream inputStream, int size) throws IOException {
    byte[] buffer = new byte[size];
    int totalRead = 0;
    while (totalRead < size) {
      int bytesRead = inputStream.read(buffer, totalRead, size - totalRead);
      if (bytesRead == -1) {
        throw new EOFException("Premature EOF: expected " + size + " bytes, but got " + totalRead);
      }
      totalRead += bytesRead;
    }
    return buffer;
  }

  /**
   * Removes blocks that failed to be populated with data from the data store. This cleanup ensures
   * failed blocks don't remain in an inconsistent state.
   *
   * @param blocks the list of blocks to check and potentially remove if not filled with data
   */
  private void removeNonFilledBlocksFromStore(List<Block> blocks) {
    // Filter out blocks that don't have data and remove them via callback
    this.removeBlocksFunc.accept(
        blocks.stream().filter(block -> !block.isDataReady()).collect(Collectors.toList()));
  }

  /** Shuts down the StreamReader object. */
  @Override
  public void close() {}
}
