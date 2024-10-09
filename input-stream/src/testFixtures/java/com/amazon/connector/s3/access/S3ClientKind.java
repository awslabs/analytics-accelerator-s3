package com.amazon.connector.s3.access;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Kind of S3 Client used */
@AllArgsConstructor
@Getter
public enum S3ClientKind {
  SDK_V2_JAVA_ASYNC("ASYNC_JAVA"),
  SDK_V2_CRT_ASYNC("ASYNC_CRT");

  private final String value;
  /**
   * Creates the S3 client based on the context and client kind. This is used by benchmarks, and
   * allows us easily run the same benchmarks against different clients and configurations
   *
   * @param s3ExecutionContext benchmark context
   * @return an instance of {@link S3AsyncClient}
   */
  public S3AsyncClient getS3Client(@NonNull S3ExecutionContext s3ExecutionContext) {
    switch (this) {
      case SDK_V2_JAVA_ASYNC:
        return s3ExecutionContext.getS3Client();
      case SDK_V2_CRT_ASYNC:
        return s3ExecutionContext.getS3CrtClient();
      default:
        throw new IllegalArgumentException("Unsupported client kind: " + this);
    }
  }
}
