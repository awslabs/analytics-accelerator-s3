package com.amazon.connector.s3;

import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient, AutoCloseable {

  private S3AsyncClient s3AsyncClient = null;

  /**
   * Create an instance of a S3 client for interaction with Amazon S3. This version of the
   * constructor uses will use CRT as the S3 client.
   */
  S3SdkObjectClient() {
    this(S3AsyncClient.crtBuilder().maxConcurrency(300).build());
  }

  /**
   * Create an instance of a S3 client for interaction with Amazon S3 compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(@NonNull S3AsyncClient s3AsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
  }

  @Override
  public void close() {
    s3AsyncClient.close();
  }

  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    return s3AsyncClient
        .headObject(
            HeadObjectRequest.builder()
                .bucket(headRequest.getBucket())
                .key(headRequest.getKey())
                .build())
        .thenApply(
            headObjectResponse ->
                ObjectMetadata.builder().contentLength(headObjectResponse.contentLength()).build());
  }

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder().bucket(getRequest.getBucket()).key(getRequest.getKey());

    if (Objects.nonNull(getRequest.getRange())) {
      String range =
          String.format(
              "bytes=%s-%s", getRequest.getRange().getStart(), getRequest.getRange().getEnd());

      builder.range(range);

      // Temporarily adding range of data requested as a Referrer header to allow for easy analysis
      // of access logs. This is similar to what the Auditing feature in S3A does.
      builder.overrideConfiguration(
          AwsRequestOverrideConfiguration.builder().putHeader("Referer", range).build());
    }

    return s3AsyncClient
        .getObject(builder.build(), AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseInputStream -> ObjectContent.builder().stream(responseInputStream).build());
  }
}
