package com.amazon.connector.s3.access;

import lombok.Builder;
import lombok.Value;

/** Object representing a read. Has a start and a length. */
@Value
@Builder
public class StreamRead {
  long start;
  long length;
}
