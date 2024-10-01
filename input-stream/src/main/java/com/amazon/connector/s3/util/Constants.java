package com.amazon.connector.s3.util;

/** S3SeekableInputStream constants. */
public final class Constants {
  /** Prevent instantiation, this is meant to be a facade */
  private Constants() {}

  public static final int ONE_KB = 1024;
  public static final int ONE_MB = 1024 * 1024;
  public static final int ONE_GB = ONE_MB * ONE_MB;
  public static final int PARQUET_MAGIC_STR_LENGTH = 4;
  public static final int PARQUET_FOOTER_LENGTH_SIZE = 4;
}
