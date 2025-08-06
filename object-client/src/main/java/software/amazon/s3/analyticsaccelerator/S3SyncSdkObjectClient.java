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

import static software.amazon.s3.analyticsaccelerator.ObjectClientTelemetry.OPERATION_SYNC_GET;
import static software.amazon.s3.analyticsaccelerator.ObjectClientTelemetry.OPERATION_SYNC_HEAD;
import static software.amazon.s3.analyticsaccelerator.util.ObjectClientUtil.handleException;

import java.io.IOException;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.common.telemetry.ConfigurableTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.RequestFactory;
import software.amazon.s3.analyticsaccelerator.request.UserAgent;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Implementation of the Object client for the AWS SDK's Java sync client. */
public class S3SyncSdkObjectClient implements ObjectClient {

  @Getter @NonNull private final S3Client s3Client;
  @NonNull private final Telemetry telemetry;
  @NonNull private final UserAgent userAgent;
  RequestFactory requestFactory;

  /**
   * Create an instance of a S3 client with SDK V2's sync client, with default configuration, for
   * interaction with Amazon S3 compatible object stores. This takes ownership of the passed client
   * and will close it on its own close().
   *
   * @param s3Client Underlying sync client to be used for making requests to S3.
   */
  public S3SyncSdkObjectClient(@NonNull S3Client s3Client) {
    this(s3Client, ObjectClientConfiguration.DEFAULT);
  }

  /**
   * Create an instance of a S3 Client with SDK V2's sync client and given configuration for
   * interaction with Amazon S3 compatible object stores. This takes ownership of the passed client
   * and will close it on its own close().
   *
   * @param s3Client Underlying sync client to be used for making requests to S3.
   * @param objectClientConfiguration Object client configuration.
   */
  public S3SyncSdkObjectClient(
      S3Client s3Client, ObjectClientConfiguration objectClientConfiguration) {
    this.s3Client = s3Client;
    this.telemetry =
        new ConfigurableTelemetry(objectClientConfiguration.getTelemetryConfiguration());
    this.userAgent = new UserAgent();
    this.userAgent.prepend(objectClientConfiguration.getUserAgentPrefix());
    String customUserAgent =
        Optional.ofNullable(s3Client.serviceClientConfiguration())
            .map(SdkServiceClientConfiguration::overrideConfiguration)
            .flatMap(override -> override.advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX))
            .orElse("");
    this.userAgent.prepend(customUserAgent);
    this.requestFactory = new RequestFactory(this.userAgent);
  }

  @Override
  public ObjectMetadata headObject(
      HeadRequest headRequest, OpenStreamInformation openStreamInformation) {
    HeadObjectRequest.Builder builder =
        this.requestFactory.buildHeadObjectRequest(headRequest, openStreamInformation);

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(OPERATION_SYNC_HEAD)
                .attribute(ObjectClientTelemetry.uri(headRequest.getS3Uri()))
                .build(),
        () -> {
          try {
            HeadObjectResponse headObjectResponse = s3Client.headObject(builder.build());
            return ObjectMetadata.builder()
                .contentLength(headObjectResponse.contentLength())
                .etag(headObjectResponse.eTag())
                .build();
          } catch (Throwable t) {
            throw handleException(headRequest.getS3Uri(), t);
          }
        });
  }

  @Override
  public ObjectContent getObject(
      GetRequest getRequest, OpenStreamInformation openStreamInformation) {
    GetObjectRequest.Builder builder =
        this.requestFactory.getObjectRequest(getRequest, openStreamInformation);

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(OPERATION_SYNC_GET)
                .attribute(ObjectClientTelemetry.uri(getRequest.getS3Uri()))
                .attribute(ObjectClientTelemetry.rangeLength(getRequest.getRange()))
                .attribute(ObjectClientTelemetry.range(getRequest.getRange()))
                .build(),
        () -> {
          try {
            ResponseInputStream<GetObjectResponse> inputStream =
                s3Client.getObject(builder.build());
            return ObjectContent.builder().stream(inputStream).build();
          } catch (Throwable t) {
            throw handleException(getRequest.getS3Uri(), t);
          }
        });
  }

  @Override
  public void close() throws IOException {
    this.s3Client.close();
  }
}
