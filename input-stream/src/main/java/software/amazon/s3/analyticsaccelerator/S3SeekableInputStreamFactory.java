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
package software.amazon.s3.analyticsaccelerator;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.DefaultLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.SequentialLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.Cache;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.ValkeyCacheImpl;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.ObjectFormatSelector;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Initialises resources to prepare for reading from S3. Resources initialised in this class are
 * shared across instances of {@link S3SeekableInputStream}. For example, this allows for the same
 * S3 client to be used across multiple input streams for more efficient connection management etc.
 *
 * <p>This factory does NOT assume ownership of the passed {@link ObjectClient}. It is the
 * responsibility of the caller to close the client and to make sure that it remains active for
 * {@link S3SeekableInputStreamFactory#createStream(S3URI)} to vend correct {@link
 * SeekableInputStream}.
 */
@Getter(AccessLevel.PACKAGE)
public class S3SeekableInputStreamFactory implements AutoCloseable {
  private final S3SeekableInputStreamConfiguration configuration;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;
  private final MetadataStore objectMetadataStore;
  private final Cache cache;
  private final ExecutorService executorService;
  private final BlobStore objectBlobStore;
  private final Telemetry telemetry;
  private final ObjectFormatSelector objectFormatSelector;

  private static final Logger LOG = LoggerFactory.getLogger(S3SeekableInputStreamFactory.class);

  /**
   * Creates a new instance of {@link S3SeekableInputStreamFactory}. This factory should be used to
   * create instances of the input stream to allow for sharing resources such as the object client
   * between streams.
   *
   * @param objectClient Object client
   * @param configuration {@link S3SeekableInputStream} configuration
   */
  public S3SeekableInputStreamFactory(
      @NonNull ObjectClient objectClient,
      @NonNull S3SeekableInputStreamConfiguration configuration) {
    this.configuration = configuration;
    this.telemetry = Telemetry.createTelemetry(configuration.getTelemetryConfiguration());
    this.parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(configuration.getLogicalIOConfiguration());
    this.objectMetadataStore =
        new MetadataStore(objectClient, telemetry, configuration.getPhysicalIOConfiguration());
    this.objectFormatSelector = new ObjectFormatSelector(configuration.getLogicalIOConfiguration());

    if (configuration.getPhysicalIOConfiguration().isEnableTailMetadataCaching()) {
      this.cache =
          new ValkeyCacheImpl(configuration.getPhysicalIOConfiguration().getCacheEndpoint());

      LOG.info("Cache successfully instantiated");

      this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);

    } else {
      LOG.info("Cache disabled");

      this.cache = null;
      this.executorService = null;
    }
    this.objectBlobStore =
        new BlobStore(objectClient, telemetry, configuration.getPhysicalIOConfiguration(), cache, executorService);
  }

  /**
   * Create an instance of S3SeekableInputStream.
   *
   * @param s3URI the object's S3 URI
   * @return An instance of the input stream.
   */
  public S3SeekableInputStream createStream(@NonNull S3URI s3URI) throws IOException {
    return new S3SeekableInputStream(s3URI, createLogicalIO(s3URI), telemetry);
  }

  /**
   * This method is deprecated. Please use {@link #createStream(S3URI, OpenStreamInformation)}
   *
   * @param s3URI the object's S3 URI
   * @param metadata the metadata for the object
   * @return An instance of the input stream.
   */
  public S3SeekableInputStream createStream(@NonNull S3URI s3URI, ObjectMetadata metadata)
      throws IOException {
    storeObjectMetadata(s3URI, metadata);
    return new S3SeekableInputStream(s3URI, createLogicalIO(s3URI), telemetry);
  }

  /**
   * Creates an instance of SeekableStream with file information
   *
   * @param s3URI the object's S3 URI
   * @param openStreamInformation known information for this key
   * @return An instance of the input stream.
   * @throws IOException IoException
   */
  public S3SeekableInputStream createStream(
      @NonNull S3URI s3URI, @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {
    storeObjectMetadata(s3URI, openStreamInformation.getObjectMetadata());
    return new S3SeekableInputStream(
        s3URI, createLogicalIO(s3URI, openStreamInformation), telemetry);
  }

  LogicalIO createLogicalIO(S3URI s3URI) throws IOException {
    return createLogicalIO(s3URI, OpenStreamInformation.DEFAULT);
  }

  LogicalIO createLogicalIO(S3URI s3URI, OpenStreamInformation openStreamInformation)
      throws IOException {
    switch (objectFormatSelector.getObjectFormat(s3URI, openStreamInformation)) {
      case PARQUET:
        return new ParquetLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(
                s3URI,
                objectMetadataStore,
                objectBlobStore,
                telemetry,
                openStreamInformation.getStreamContext()),
            telemetry,
            configuration.getLogicalIOConfiguration(),
            parquetColumnPrefetchStore);

      case SEQUENTIAL:
        return new SequentialLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(
                s3URI,
                objectMetadataStore,
                objectBlobStore,
                telemetry,
                openStreamInformation.getStreamContext()),
            telemetry,
            configuration.getLogicalIOConfiguration());

      default:
        return new DefaultLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(
                s3URI,
                objectMetadataStore,
                objectBlobStore,
                telemetry,
                openStreamInformation.getStreamContext()),
            telemetry);
    }
  }

  void storeObjectMetadata(S3URI s3URI, ObjectMetadata metadata) {
    if (metadata != null) {
      objectMetadataStore.storeObjectMetadata(s3URI, metadata);
    }
  }

  /**
   * Closes the factory and underlying resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    this.objectMetadataStore.close();
    this.objectBlobStore.close();
    this.telemetry.close();

    handleCacheClosure(cache, configuration.getPhysicalIOConfiguration().isEnableCacheFlush());
  }

  private void handleCacheClosure(Cache cache, boolean shouldFlushCache) {
    if (cache != null) {

      if (shouldFlushCache) {
        LOG.info("Cache is being closed");
        LOG.info("Starting to clear cache");

        long cacheClearStartTime = System.nanoTime();

        cache.clearCache();

        long cacheClearDuration = System.nanoTime() - cacheClearStartTime;
        double cacheClearMsDuration = cacheClearDuration / 1_000_000.0;

        LOG.info("Cache has been cleared");
        LOG.info("Cache clear took: {}ms", String.format("%.2f", cacheClearMsDuration));
      }

      cache.close();
    }

  }
}
