package com.amazon.connector.s3;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.impl.DefaultLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetColumnPrefetchStore;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MemoryTracker;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.util.ObjectFormatSelector;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import lombok.Getter;
import lombok.NonNull;

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
@Getter
public class S3SeekableInputStreamFactory implements AutoCloseable {
  private final ObjectClient objectClient;
  private final S3SeekableInputStreamConfiguration configuration;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;

  private final MetadataStore objectMetadataStore;
  private final BlobStore objectBlobStore;
  private final Telemetry telemetry;
  private final ObjectFormatSelector objectFormatSelector;
  private final MemoryTracker memoryTracker;

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
    this.objectClient = objectClient;
    this.configuration = configuration;
    this.telemetry = Telemetry.createTelemetry(configuration.getTelemetryConfiguration());
    this.parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(configuration.getLogicalIOConfiguration());
    this.objectMetadataStore =
        new MetadataStore(objectClient, telemetry, configuration.getPhysicalIOConfiguration());

    this.objectFormatSelector = new ObjectFormatSelector(configuration.getLogicalIOConfiguration());
    this.memoryTracker = new MemoryTracker();

    this.objectBlobStore =
        new BlobStore(
            objectMetadataStore,
            objectClient,
            telemetry,
            configuration.getPhysicalIOConfiguration(),
            memoryTracker);
  }

  /**
   * Create an instance of S3SeekableInputStream.
   *
   * @param s3URI the object's S3 URI
   * @return An instance of the input stream.
   */
  public S3SeekableInputStream createStream(@NonNull S3URI s3URI) {
    return new S3SeekableInputStream(s3URI, createLogicalIO(s3URI), telemetry);
  }

  LogicalIO createLogicalIO(S3URI s3URI) {
    switch (objectFormatSelector.getObjectFormat(s3URI)) {
      case PARQUET:
        return new ParquetLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(s3URI, objectMetadataStore, objectBlobStore, telemetry),
            telemetry,
            configuration.getLogicalIOConfiguration(),
            parquetColumnPrefetchStore);

      default:
        return new DefaultLogicalIOImpl(
            new PhysicalIOImpl(s3URI, objectMetadataStore, objectBlobStore, telemetry));
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
  }
}
