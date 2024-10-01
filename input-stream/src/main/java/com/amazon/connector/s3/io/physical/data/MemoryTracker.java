package com.amazon.connector.s3.io.physical.data;

import lombok.Getter;

/**
 * Responsible for tracking total memory used by an instance of {@link
 * com.amazon.connector.s3.S3SeekableInputStreamFactory}.
 */
public class MemoryTracker {

  @Getter long memoryUsed;

  /**
   * Increment memory used.
   *
   * @param bytesAllocated bytes allocated
   * @return total memory in use
   */
  public long incrementMemoryUsed(long bytesAllocated) {
    this.memoryUsed += bytesAllocated;
    return memoryUsed;
  }

  /**
   * Updates memory in use when it is freed.
   *
   * @param bytesFreed bytes freed
   * @return total memory in ues
   */
  public long freeMemory(long bytesFreed) {
    this.memoryUsed -= bytesFreed;
    return memoryUsed;
  }
}
