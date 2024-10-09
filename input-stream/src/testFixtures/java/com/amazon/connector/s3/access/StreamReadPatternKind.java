package com.amazon.connector.s3.access;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Enum that describes different supported stream seeks */
@AllArgsConstructor
@Getter
public enum StreamReadPatternKind {
  SEQUENTIAL("SEQUENTIAL"),
  SKIPPING_FORWARD("SKIPPING_FORWARD"),
  SKIPPING_BACKWARD("SKIPPING_BACKWARD"),
  QUASI_PARQUET_ROW_GROUP("QUASI_PARQUET_ROW_GROUP"),
  QUASI_PARQUET_COLUMN_CHUNK("QUASI_PARQUET_COLUMN_CHUNK");
  private final String value;

  /**
   * Gets the stream read pattern based on the value of this enum
   *
   * @param s3Object {@link S3Object} to read
   * @return the stream read pattern
   */
  public StreamReadPattern getStreamReadPattern(S3Object s3Object) {
    switch (this) {
      case SEQUENTIAL:
        return StreamReadPatternFactory.getSequentialReadPattern(s3Object);
      case SKIPPING_FORWARD:
        return StreamReadPatternFactory.getForwardSeekReadPattern(s3Object);
      case SKIPPING_BACKWARD:
        return StreamReadPatternFactory.getBackwardSeekReadPattern(s3Object);
      case QUASI_PARQUET_ROW_GROUP:
        return StreamReadPatternFactory.getQuasiParquetRowGroupPattern(s3Object);
      case QUASI_PARQUET_COLUMN_CHUNK:
        return StreamReadPatternFactory.getQuasiParquetColumnChunkPattern(s3Object);
      default:
        throw new IllegalArgumentException("Unknown stream read pattern: " + this);
    }
  }
}
