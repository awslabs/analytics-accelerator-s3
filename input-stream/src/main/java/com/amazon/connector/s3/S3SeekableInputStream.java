package com.amazon.connector.s3;

import com.amazon.connector.s3.blockmanager.BlockManager;
import com.amazon.connector.s3.util.S3URI;
import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import lombok.NonNull;

/**
 * High throughput seekable stream used to read data from Amazon S3.
 *
 * <p>Don't share between threads. The current implementation is not thread safe in that calling
 * {@link #seek(long) seek} will modify the position of the stream and the behaviour of calling
 * {@link #seek(long) seek} and {@link #read() read} concurrently from two different threads is
 * undefined.
 */
public class S3SeekableInputStream extends SeekableInputStream {

  private final BlockManager blockManager;
  private long position;

  /**
   * Creates a new instance of {@link S3SeekableInputStream}. This version of the constructor
   * initialises the stream with sensible defaults.
   *
   * @param s3URI the object's S3 URI
   */
  public S3SeekableInputStream(@NonNull S3URI s3URI) {
    this(new BlockManager(new S3SdkObjectClient(null), s3URI));
  }

  /**
   * Given a Block Manager, creates a new instance of {@link S3SeekableInputStream}. This version of
   * the constructor is useful for testing as it allows dependency injection.
   *
   * @param blockManager already initialised Block Manager
   */
  public S3SeekableInputStream(@NonNull BlockManager blockManager) {
    this.blockManager = blockManager;
    this.position = 0;
  }

  @Override
  public int read() throws IOException {
    if (this.position >= contentLength()) {
      return -1;
    }

    int byteRead = this.blockManager.readByte(this.position);
    this.position++;
    return byteRead;
  }

  @Override
  public void seek(long pos) throws IOException {
    // TODO: https://app.asana.com/0/1206885953994785/1207207312934251/f
    // S3A throws an EOFException here, S3FileIO does IllegalArgumentException
    Preconditions.checkState(pos >= 0, "position must be non-negative");

    if (pos >= contentLength()) {
      throw new EOFException("zero-indexed seek position must be less than the object size");
    }

    this.position = pos;
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.blockManager.close();
  }

  private long contentLength() {
    return this.blockManager.getMetadata().join().getContentLength();
  }
}