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

import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.common.telemetry.ConfigurableTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.*;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient {
  private static final String HEADER_USER_AGENT = "User-Agent";
  private static final String HEADER_REFERER = "Referer";

  @Getter @NonNull private final AwsClient s3Client;
  @NonNull private final Telemetry telemetry;
  @NonNull private final UserAgent userAgent;
  private final boolean closeClient;

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3Client Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(AwsClient s3Client) {
    this(s3Client, ObjectClientConfiguration.DEFAULT);
  }

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3Client Underlying client to be used for making requests to S3.
   * @param closeClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(AwsClient s3Client, boolean closeClient) {
    this(s3Client, ObjectClientConfiguration.DEFAULT, closeClient);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   * This takes ownership of the passed client and will close it on its own close().
   *
   * @param s3Client Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   */
  public S3SdkObjectClient(
      AwsClient s3Client, ObjectClientConfiguration objectClientConfiguration) {
    this(s3Client, objectClientConfiguration, true);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   *
   * @param s3Client Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   * @param closeClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(
      @NonNull AwsClient s3Client,
      @NonNull ObjectClientConfiguration objectClientConfiguration,
      boolean closeClient) {
    this.s3Client = s3Client;
    this.closeClient = closeClient;
    this.telemetry =
        new ConfigurableTelemetry(objectClientConfiguration.getTelemetryConfiguration());
    this.userAgent = new UserAgent();
    this.userAgent.prepend(objectClientConfiguration.getUserAgentPrefix());
  }

  /** Closes the underlying client if instructed by the constructor. */
  @Override
  public void close() {
    if (this.closeClient) {
      s3Client.close();
    }
  }

  /**
   * Make a headObject request to the object store.
   *
   * @param headRequest The HEAD request to be sent
   * @return HeadObjectResponse
   */
  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    HeadObjectRequest.Builder builder =
        HeadObjectRequest.builder()
            .bucket(headRequest.getS3Uri().getBucket())
            .key(headRequest.getS3Uri().getKey());

    // Add User-Agent header to the request.
    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
            .build());

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(ObjectClientTelemetry.OPERATION_HEAD)
                .attribute(ObjectClientTelemetry.uri(headRequest.getS3Uri()))
                .build(),
        headObjectForClient(builder.build())
            .thenApply(
                headObjectResponse ->
                    ObjectMetadata.builder()
                        .contentLength(headObjectResponse.contentLength())
                        .build()));
  }

  private CompletableFuture<HeadObjectResponse> headObjectForClient(HeadObjectRequest request) {
    if (s3Client instanceof S3AsyncClient) {
      return ((S3AsyncClient) s3Client).headObject(request);
    } else if (s3Client instanceof S3Client) {
      return CompletableFuture.completedFuture(((S3Client) s3Client).headObject(request));
    } else {
      throw new UnsupportedOperationException("Unsupported client type for headObject operation");
    }
  }

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @return ResponseInputStream<GetObjectResponse>
   */
  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder()
            .bucket(getRequest.getS3Uri().getBucket())
            .key(getRequest.getS3Uri().getKey());

    String range = getRequest.getRange().toHttpString();
    builder.range(range);

    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_REFERER, getRequest.getReferrer().toString())
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
            .build());

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(ObjectClientTelemetry.OPERATION_GET)
                .attribute(ObjectClientTelemetry.uri(getRequest.getS3Uri()))
                .attribute(ObjectClientTelemetry.rangeLength(getRequest.getRange()))
                .attribute(ObjectClientTelemetry.range(getRequest.getRange()))
                .build(),
        getObjectForClient(builder.build())
            .thenApply(
                responseInputStream ->
                    ObjectContent.builder().stream(responseInputStream).build()));
  }

  private CompletableFuture<ResponseInputStream<GetObjectResponse>> getObjectForClient(
      GetObjectRequest request) {
    if (s3Client instanceof S3AsyncClient) {
      return ((S3AsyncClient) s3Client)
          .getObject(request, AsyncResponseTransformer.toBlockingInputStream());
    } else if (s3Client instanceof S3Client) {
      return CompletableFuture.completedFuture(((S3Client) s3Client).getObject(request));
    } else {
      throw new UnsupportedOperationException("Unsupported client type for getObject operation");
    }
  }
}
