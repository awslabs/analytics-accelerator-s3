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

import static software.amazon.s3.analyticsaccelerator.ObjectClientTelemetry.*;
import static software.amazon.s3.analyticsaccelerator.util.ObjectClientUtil.handleException;

import java.io.IOException;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.common.telemetry.ConfigurableTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient {

  @Getter @NonNull private final S3AsyncClient s3AsyncClient;
  @NonNull private final Telemetry telemetry;
  @NonNull private final UserAgent userAgent;
  private final boolean closeAsyncClient;
  RequestFactory requestFactory;

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(@NonNull S3AsyncClient s3AsyncClient) {
    this(s3AsyncClient, ObjectClientConfiguration.DEFAULT);
  }

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param closeAsyncClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(@NonNull S3AsyncClient s3AsyncClient, boolean closeAsyncClient) {
    this(s3AsyncClient, ObjectClientConfiguration.DEFAULT, closeAsyncClient);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   * This takes ownership of the passed client and will close it on its own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   */
  public S3SdkObjectClient(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull ObjectClientConfiguration objectClientConfiguration) {
    this(s3AsyncClient, objectClientConfiguration, true);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   * @param closeAsyncClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull ObjectClientConfiguration objectClientConfiguration,
      boolean closeAsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
    this.closeAsyncClient = closeAsyncClient;
    this.telemetry =
        new ConfigurableTelemetry(objectClientConfiguration.getTelemetryConfiguration());
    this.userAgent = new UserAgent();
    this.userAgent.prepend(objectClientConfiguration.getUserAgentPrefix());
    String customUserAgent =
        Optional.ofNullable(s3AsyncClient.serviceClientConfiguration())
            .map(SdkServiceClientConfiguration::overrideConfiguration)
            .flatMap(override -> override.advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX))
            .orElse("");
    this.userAgent.prepend(customUserAgent);
    this.requestFactory = new RequestFactory(userAgent);
  }

  /** Closes the underlying client if instructed by the constructor. */
  @Override
  public void close() {
    if (this.closeAsyncClient) {
      s3AsyncClient.close();
    }
  }

  @Override
  public ObjectMetadata headObject(
      HeadRequest headRequest, OpenStreamInformation openStreamInformation) throws IOException {

    HeadObjectRequest.Builder builder =
        requestFactory.buildHeadObjectRequest(headRequest, openStreamInformation);

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(OPERATION_HEAD)
                .attribute(ObjectClientTelemetry.uri(headRequest.getS3Uri()))
                .build(),
        () -> {
          try {
            HeadObjectResponse headObjectResponse =
                s3AsyncClient.headObject(builder.build()).join();
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
  public ObjectContent getObject(GetRequest getRequest, OpenStreamInformation openStreamInformation)
      throws IOException {

    GetObjectRequest.Builder builder =
        requestFactory.getObjectRequest(getRequest, openStreamInformation);

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(OPERATION_GET)
                .attribute(ObjectClientTelemetry.uri(getRequest.getS3Uri()))
                .attribute(ObjectClientTelemetry.rangeLength(getRequest.getRange()))
                .attribute(ObjectClientTelemetry.range(getRequest.getRange()))
                .build(),
        () -> {
          try {
            ResponseInputStream<GetObjectResponse> inputStream =
                s3AsyncClient
                    .getObject(builder.build(), AsyncResponseTransformer.toBlockingInputStream())
                    .join();
            return ObjectContent.builder().stream(inputStream).build();
          } catch (Throwable t) {
            // TODO: Exception handling needs to be moved here as this is where the join happens.
            throw handleException(getRequest.getS3Uri(), t);
          }
        });
  }
}
