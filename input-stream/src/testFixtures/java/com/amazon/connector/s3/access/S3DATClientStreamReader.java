package com.amazon.connector.s3.access;

import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Client stream reader based on DAT */
public class S3DATClientStreamReader extends S3StreamReaderBase {
  @NonNull @Getter private final S3SdkObjectClient sdkObjectClient;
  @NonNull @Getter private final S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

  /**
   * Creates an instance of {@link S3DATClientStreamReader}
   *
   * @param s3AsyncClient an instance of {@link S3AsyncClient}
   * @param configuration {@link S3SeekableInputStreamConfiguration}
   * @param baseUri base URI for all objects
   * @param bufferSize buffer size
   */
  public S3DATClientStreamReader(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull S3SeekableInputStreamConfiguration configuration,
      @NonNull S3URI baseUri,
      int bufferSize) {
    super(baseUri, bufferSize);
    // Create the SDK client, ensure it doesn't close the underlying client
    this.sdkObjectClient = new S3SdkObjectClient(s3AsyncClient, false);
    s3SeekableInputStreamFactory = new S3SeekableInputStreamFactory(sdkObjectClient, configuration);
  }

  /**
   * Creates the read stream for a given object
   *
   * @param s3Object {@link S3Object} to create the stream for
   * @return read stream
   */
  public S3SeekableInputStream createReadStream(@NonNull S3Object s3Object) {
    S3URI s3URI = s3Object.getObjectUri(this.getBaseUri());
    return this.getS3SeekableInputStreamFactory().createStream(s3URI);
  }

  /**
   * Reads the specified pattern
   *
   * @param s3Object S3 Object to read
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   */
  @Override
  public void readPattern(
      @NonNull S3Object s3Object,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum)
      throws IOException {
    try (S3SeekableInputStream inputStream = this.createReadStream(s3Object)) {
      readPattern(s3Object, inputStream, streamReadPattern, checksum);
    }
  }

  /**
   * Reads the specified pattern
   *
   * @param s3Object S3 Object to read
   * @param inputStream read stream
   * @param streamReadPattern Stream read pattern
   * @param checksum optional checksum, to update
   */
  public void readPattern(
      @NonNull S3Object s3Object,
      @NonNull S3SeekableInputStream inputStream,
      @NonNull StreamReadPattern streamReadPattern,
      @NonNull Optional<Crc32CChecksum> checksum)
      throws IOException {
    // Replay the pattern through a set of seeks and drains
    // Apply seeks
    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      // Seek to the start
      inputStream.seek(streamRead.getStart());
      // drain bytes of size length
      drainStream(inputStream, s3Object, checksum, streamRead.getLength());
    }
  }

  /**
   * Closes the reader
   *
   * @throws IOException if IO error occurs
   */
  @Override
  public void close() throws IOException {
    // close the factory and the client
    this.s3SeekableInputStreamFactory.close();
    this.sdkObjectClient.close();
  }
}
