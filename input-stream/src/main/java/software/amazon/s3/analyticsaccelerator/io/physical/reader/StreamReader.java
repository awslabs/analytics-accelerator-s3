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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.data.DataBlock;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * {@code StreamReader} is responsible for asynchronously reading a range of bytes from an object in
 * S3 and populating the corresponding {@link DataBlock}s with the downloaded data.
 *
 * <p>It submits the read task to a provided {@link ExecutorService}, allowing non-blocking
 * operation.
 */
public class StreamReader implements Closeable {
  private final ObjectClient objectClient;
  private final ObjectKey objectKey;
  private final ExecutorService threadPool;
  private final OpenStreamInformation openStreamInformation;

  /**
   * Constructs a {@code StreamReader} instance for reading objects from S3.
   *
   * @param objectClient the client used to fetch S3 object content
   * @param objectKey the key identifying the S3 object and its ETag
   * @param threadPool an {@link ExecutorService} used for async I/O operations
   * @param openStreamInformation contains stream information
   */
  public StreamReader(
      @NonNull ObjectClient objectClient,
      @NonNull ObjectKey objectKey,
      @NonNull ExecutorService threadPool,
      @NonNull OpenStreamInformation openStreamInformation) {
    this.objectClient = objectClient;
    this.objectKey = objectKey;
    this.threadPool = threadPool;
    this.openStreamInformation = openStreamInformation;
  }

  /**
   * Asynchronously reads a range of bytes from the S3 object and fills the corresponding {@link
   * DataBlock}s with data. The byte range is determined by the start of the first block and the end
   * of the last block.
   *
   * @param blocks the list of {@link DataBlock}s to be populated; must not be empty and must be
   *     sorted by offset
   * @param readMode the mode in which the read is being performed (used for tracking or metrics)
   * @throws IllegalArgumentException if the {@code blocks} list is empty
   * @implNote This method uses a fire-and-forget strategy and doesn't return a {@code Future};
   *     failures are logged or wrapped in a {@code RuntimeException}.
   */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification = "Intentional fire-and-forget task")
  public void read(@NonNull final List<DataBlock> blocks, ReadMode readMode) {
    Preconditions.checkArgument(!blocks.isEmpty(), "`blocks` list must not be empty");

    long rangeStart = blocks.get(0).getBlockKey().getRange().getStart();
    long rangeEnd = blocks.get(blocks.size() - 1).getBlockKey().getRange().getEnd();
    final Range requestRange = new Range(rangeStart, rangeEnd);

    threadPool.submit(
        () -> {
          GetRequest getRequest =
              GetRequest.builder()
                  .s3Uri(objectKey.getS3URI())
                  .range(requestRange)
                  .etag(objectKey.getEtag())
                  .referrer(new Referrer(requestRange.toHttpString(), readMode))
                  .build();

          ObjectContent objectContent =
              objectClient.getObject(getRequest, openStreamInformation).join();

          try (InputStream inputStream = objectContent.getStream()) {
            long currentOffset = rangeStart;
            for (DataBlock block : blocks) {
              long blockStart = block.getBlockKey().getRange().getStart();
              long blockEnd = block.getBlockKey().getRange().getEnd();
              int blockSize = (int) (blockEnd - blockStart + 1);

              // Skip if needed
              long skipBytes = blockStart - currentOffset;
              if (skipBytes > 0) {
                long skipped = inputStream.skip(skipBytes);
                if (skipped != skipBytes) {
                  throw new IOException("Failed to skip required number of bytes in stream");
                }
                currentOffset += skipped;
              }

              byte[] blockData = new byte[blockSize];
              int totalRead = 0;
              while (totalRead < blockSize) {
                int bytesRead = inputStream.read(blockData, totalRead, blockSize - totalRead);
                if (bytesRead == -1) {
                  throw new IOException("Unexpected end of stream while reading block data");
                }
                totalRead += bytesRead;
              }

              block.setData(blockData);

              currentOffset += blockSize;
            }
          } catch (IOException e) {
            // TODO handle failure cases gracefully
            throw new RuntimeException("Unexpected error while reading from stream", e);
          }
        });
  }

  /**
   * Closes the underlying {@link ObjectClient} and shuts down the thread pool used for asynchronous
   * execution.
   *
   * @throws IOException if the {@code objectClient} fails to close properly
   */
  @Override
  public void close() throws IOException {
    this.objectClient.close();
    this.threadPool.shutdown();
  }
}
