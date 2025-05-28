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
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Block;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** Object responsible to get object from S3 and fill the data in Blocks */
public class StreamReader implements Closeable {
  private final ObjectClient objectClient;
  private final ObjectKey objectKey;
  private final ExecutorService readThreadPool;
  private final StreamContext streamContext;

  private static final Logger LOG = LoggerFactory.getLogger(StreamReader.class);

  /**
   * Default constructor
   *
   * @param objectClient object client
   * @param objectKey object key
   * @param readThreadPool thread pool
   * @param streamContext stream context
   */
  public StreamReader(
      @NonNull ObjectClient objectClient,
      @NonNull ObjectKey objectKey,
      @NonNull ExecutorService readThreadPool,
      StreamContext streamContext) {
    this.objectClient = objectClient;
    this.readThreadPool = readThreadPool;
    this.objectKey = objectKey;
    this.streamContext = streamContext;
  }

  /**
   * This method get the object from S3 and fills the Block objects
   *
   * @param blocks list of blocks to fill
   */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification = "Intentional fire-and-forget task")
  public void read(List<Block> blocks) {
    Range range =
        new Range(
            blocks.get(0).getBlockKey().getRange().getStart(),
            blocks.get(blocks.size() - 1).getBlockKey().getRange().getEnd());

    readThreadPool.submit(
        () -> {
          GetRequest getRequest =
              GetRequest.builder()
                  .s3Uri(objectKey.getS3URI())
                  .range(range)
                  .etag(objectKey.getEtag())
                  .referrer(
                      new Referrer(
                          range.toHttpString(), ReadMode.SYNC)) // TODO ReadMode should be updated
                  .build();

          ObjectContent objectContent =
              objectClient.getObject(getRequest, this.streamContext).join();

          try (InputStream inputStream = objectContent.getStream()) {
            long streamOffset = range.getStart();

            for (Block block : blocks) {
              long blockStart = block.getBlockKey().getRange().getStart();
              long blockEnd = block.getBlockKey().getRange().getEnd();
              int blockLength = (int) (blockEnd - blockStart + 1);

              // Skip if needed
              long skipBytes = blockStart - streamOffset;
              if (skipBytes > 0) {
                long skipped = inputStream.skip(skipBytes);
                if (skipped != skipBytes) {
                  throw new IOException("Failed to skip required number of bytes in stream");
                }
                streamOffset += skipped;
              }

              byte[] buffer = new byte[blockLength];
              int totalRead = 0;
              while (totalRead < blockLength) {
                int read = inputStream.read(buffer, totalRead, blockLength - totalRead);
                if (read == -1) {
                  throw new EOFException("Unexpected end of stream while reading block data");
                }
                totalRead += read;
              }

              block.setData(buffer);
              block.onDataLoaded();

              streamOffset += blockLength;
            }
          } catch (IOException e) {
            // TODO Handle Latch
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void close() throws IOException {
    this.objectClient.close();
    this.readThreadPool.shutdown();
  }
}
