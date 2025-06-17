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
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.util.*;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {
  private CompletableFuture<ObjectContent> source;
  private CompletableFuture<byte[]> data;
  @Getter private final BlockKey blockKey;
  private final Telemetry telemetry;
  private final ObjectClient objectClient;
  private final OpenStreamInformation openStreamInformation;
  private final ReadMode readMode;
  private final Referrer referrer;
  @Getter private final long generation;
  @Getter private final PhysicalIOConfiguration configuration;
  private final int readRetryCount;
  private final long readTimeout;
  private final Metrics aggregatingMetrics;
  private final BlobStoreIndexCache indexCache;
  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";

  private static final Logger LOG = LoggerFactory.getLogger(Block.class);

  /**
   * Constructs a Block data.
   *
   * @param blockKey the objectkey and range of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param configuration PhysicalIO Configuration to learn timeout and retry count
   * @param aggregatingMetrics blobstore metrics
   * @param indexCache blobstore index cache
   * @param openStreamInformation contains stream information
   */
  private Block(
      @NonNull BlockKey blockKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long generation,
      @NonNull ReadMode readMode,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull Metrics aggregatingMetrics,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {

    long start = blockKey.getRange().getStart();
    long end = blockKey.getRange().getEnd();
    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);

    this.generation = generation;
    this.telemetry = telemetry;
    this.blockKey = blockKey;
    this.objectClient = objectClient;
    this.openStreamInformation = openStreamInformation;
    this.readMode = readMode;
    this.referrer = new Referrer(this.blockKey.getRange().toHttpString(), readMode);
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
    this.configuration = configuration;
    this.readRetryCount = configuration.getBlockReadRetryCount();
    this.readTimeout = configuration.getBlockReadTimeout();
    generateSourceAndData();
  }

  /** Method to help construct source and data */
  private void generateSourceAndData() throws IOException {
    int retries = 0;
    while (retries < this.readRetryCount) {
      try {
        GetRequest getRequest =
            GetRequest.builder()
                .s3Uri(this.blockKey.getObjectKey().getS3URI())
                .range(this.blockKey.getRange())
                .etag(this.blockKey.getObjectKey().getEtag())
                .referrer(referrer)
                .build();

        this.source =
            this.telemetry.measureCritical(
                () ->
                    Operation.builder()
                        .name(OPERATION_BLOCK_GET_ASYNC)
                        .attribute(StreamAttributes.uri(this.blockKey.getObjectKey().getS3URI()))
                        .attribute(StreamAttributes.etag(this.blockKey.getObjectKey().getEtag()))
                        .attribute(StreamAttributes.range(this.blockKey.getRange()))
                        .attribute(StreamAttributes.generation(generation))
                        .build(),
                objectClient.getObject(getRequest, openStreamInformation));

        // Handle IOExceptions when converting stream to byte array
        this.data =
            this.source.thenApply(
                objectContent -> {
                  try {
                    byte[] bytes =
                        StreamUtils.toByteArray(
                            objectContent,
                            this.blockKey.getObjectKey(),
                            this.blockKey.getRange(),
                            this.readTimeout);
                    int blockRange = blockKey.getRange().getLength();
                    this.aggregatingMetrics.add(MetricKey.MEMORY_USAGE, blockRange);
                    this.indexCache.put(blockKey, blockRange);
                    return bytes;
                  } catch (IOException | TimeoutException e) {
                    throw new RuntimeException(
                        "Error while converting InputStream to byte array", e);
                  }
                });

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

  /** @return if data is loaded */
  public boolean isDataLoaded() {
    return data.isDone();
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

    byte[] content = this.getDataWithRetries();
    indexCache.recordAccess(blockKey);
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

    byte[] content = this.getDataWithRetries();
    indexCache.recordAccess(blockKey);
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
    return this.blockKey.getRange().contains(pos);
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - this.blockKey.getRange().getStart());
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. If it receives an IOException from
   * {@link S3SdkObjectClient}, retries for MAX_RETRIES count.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   * @throws IOException if an I/O error occurs after maximum retry counts
   */
  private byte[] getDataWithRetries() throws IOException {
    for (int i = 0; i < this.readRetryCount; i++) {
      try {
        return this.getData();
      } catch (IOException ex) {
        if (ex.getClass() == IOException.class) {
          if (i < this.readRetryCount - 1) {
            LOG.debug("Get data failed. Retrying. Retry Count {}", i);
            generateSourceAndData();
          } else {
            LOG.error("Cannot read block file. Retry reached the limit");
            throw new IOException("Cannot read block file", ex.getCause());
          }
        } else {
          throw ex;
        }
      }
    }
    throw new IOException("Cannot read block file", new IOException("Error while getting block"));
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. This method will block until the
   * data is fully available.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   * @throws IOException if an I/O error occurs
   */
  private byte[] getData() throws IOException {
    return this.telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_BLOCK_GET_JOIN)
                .attribute(StreamAttributes.uri(this.blockKey.getObjectKey().getS3URI()))
                .attribute(StreamAttributes.etag(this.blockKey.getObjectKey().getEtag()))
                .attribute(StreamAttributes.range(this.blockKey.getRange()))
                .attribute(StreamAttributes.rangeLength(this.blockKey.getRange().getLength()))
                .build(),
        this.data,
        this.readTimeout);
  }

  /**
   * Creates a new BlockBuilder instance to build a Block.
   *
   * @return a new BlockBuilder instance
   */
  public static BlockBuilder builder() {
    return new BlockBuilder();
  }

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {
    // Only the source needs to be canceled, the continuation will cancel on its own
    this.source.cancel(false);
  }

  /** Custom builder for Block class. */
  public static class BlockBuilder {
    private BlockKey blockKey;
    private ObjectClient objectClient;
    private Telemetry telemetry = Telemetry.DEFAULT;
    private long generation = 0;
    private ReadMode readMode;
    private PhysicalIOConfiguration configuration = PhysicalIOConfiguration.DEFAULT;
    private Metrics aggregatingMetrics;
    private BlobStoreIndexCache indexCache;
    private OpenStreamInformation openStreamInformation = OpenStreamInformation.DEFAULT;

    /**
     * Sets the BlockKey for the Block.
     *
     * @param blockKey the objectkey and range of the object
     * @return this builder instance
     */
    public Block.BlockBuilder blockKey(@NonNull BlockKey blockKey) {
      this.blockKey = blockKey;
      return this;
    }

    /**
     * Sets the ObjectClient for the Block.
     *
     * @param objectClient the object client to use to interact with the object store
     * @return this builder instance
     */
    public Block.BlockBuilder objectClient(@NonNull ObjectClient objectClient) {
      this.objectClient = objectClient;
      return this;
    }

    /**
     * Sets the Telemetry for the Block.
     *
     * @param telemetry an instance of {@link Telemetry} to use
     * @return this builder instance
     */
    public Block.BlockBuilder telemetry(@NonNull Telemetry telemetry) {
      this.telemetry = telemetry;
      return this;
    }

    /**
     * Sets the generation for the Block.
     *
     * @param generation generation of the block in a sequential read pattern
     * @return this builder instance
     */
    public Block.BlockBuilder generation(long generation) {
      this.generation = generation;
      return this;
    }

    /**
     * Sets the ReadMode for the Block.
     *
     * @param readMode read mode describing whether this is a sync or async fetch
     * @return this builder instance
     */
    public Block.BlockBuilder readMode(@NonNull ReadMode readMode) {
      this.readMode = readMode;
      return this;
    }

    /**
     * Sets the PhysicalIOConfiguration for the Block.
     *
     * @param configuration PhysicalIO Configuration to learn timeout and retry count
     * @return this builder instance
     */
    public Block.BlockBuilder configuration(@NonNull PhysicalIOConfiguration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * Sets the Metrics for the Block.
     *
     * @param aggregatingMetrics blobstore metrics
     * @return this builder instance
     */
    public Block.BlockBuilder aggregatingMetrics(@NonNull Metrics aggregatingMetrics) {
      this.aggregatingMetrics = aggregatingMetrics;
      return this;
    }

    /**
     * Sets the BlobStoreIndexCache for the Block.
     *
     * @param indexCache blobstore index cache
     * @return this builder instance
     */
    public Block.BlockBuilder indexCache(@NonNull BlobStoreIndexCache indexCache) {
      this.indexCache = indexCache;
      return this;
    }

    /**
     * Sets the OpenStreamInformation for the Block.
     *
     * @param openStreamInformation contains stream information
     * @return this builder instance
     */
    public Block.BlockBuilder openStreamInformation(
        @NonNull OpenStreamInformation openStreamInformation) {
      this.openStreamInformation = openStreamInformation;
      return this;
    }

    /**
     * Builds a new Block instance with the configured parameters.
     *
     * @return a new Block instance
     * @throws IOException if an I/O error occurs during Block creation
     */
    public Block build() throws IOException {
      return new Block(
          blockKey,
          objectClient,
          telemetry,
          generation,
          readMode,
          configuration,
          aggregatingMetrics,
          indexCache,
          openStreamInformation);
    }
  }
}
