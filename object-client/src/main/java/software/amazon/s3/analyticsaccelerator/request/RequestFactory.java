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
package software.amazon.s3.analyticsaccelerator.request;

import static software.amazon.s3.analyticsaccelerator.request.Constants.HEADER_REFERER;
import static software.amazon.s3.analyticsaccelerator.request.Constants.HEADER_USER_AGENT;
import static software.amazon.s3.analyticsaccelerator.util.ObjectClientUtil.attachStreamContextToExecutionAttributes;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Factory to create request builders. */
public class RequestFactory {

  private final UserAgent userAgent;

  /**
   * Creates a request factory to be used to build S3 SDK V2 requests.
   *
   * @param userAgent userAgent to use in requests
   */
  public RequestFactory(UserAgent userAgent) {
    this.userAgent = userAgent;
  }

  /**
   * Head object request builder
   *
   * @param headRequest Head request.
   * @param openStreamInformation Open stream information.
   * @return HeadObjectRequest.Builder
   */
  public HeadObjectRequest.Builder buildHeadObjectRequest(
      HeadRequest headRequest, OpenStreamInformation openStreamInformation) {
    HeadObjectRequest.Builder builder =
        HeadObjectRequest.builder()
            .bucket(headRequest.getS3Uri().getBucket())
            .key(headRequest.getS3Uri().getKey());

    AwsRequestOverrideConfiguration.Builder requestOverrideConfigurationBuilder =
        AwsRequestOverrideConfiguration.builder();

    requestOverrideConfigurationBuilder.putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent());

    if (openStreamInformation.getStreamAuditContext() != null) {
      attachStreamContextToExecutionAttributes(
          requestOverrideConfigurationBuilder, openStreamInformation.getStreamAuditContext());
    }

    builder.overrideConfiguration(requestOverrideConfigurationBuilder.build());

    if (openStreamInformation.getEncryptionSecrets() != null
        && openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().isPresent()) {
      String customerKey = openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().get();
      String customerKeyMd5 = openStreamInformation.getEncryptionSecrets().getSsecCustomerKeyMd5();
      builder
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(customerKey)
          .sseCustomerKeyMD5(customerKeyMd5);
    }

    return builder;
  }

  /**
   * Get object request builder.
   *
   * @param getRequest Get request.
   * @param openStreamInformation Open stream information.
   * @return GetObjectRequest.Builder
   */
  public GetObjectRequest.Builder getObjectRequest(
      GetRequest getRequest, OpenStreamInformation openStreamInformation) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder()
            .bucket(getRequest.getS3Uri().getBucket())
            .ifMatch(getRequest.getEtag())
            .key(getRequest.getS3Uri().getKey());

    final String range = getRequest.getRange().toHttpString();
    builder.range(range);

    AwsRequestOverrideConfiguration.Builder requestOverrideConfigurationBuilder =
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_REFERER, getRequest.getReferrer().toString())
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent());

    if (openStreamInformation.getStreamAuditContext() != null) {
      attachStreamContextToExecutionAttributes(
          requestOverrideConfigurationBuilder, openStreamInformation.getStreamAuditContext());
    }

    builder.overrideConfiguration(requestOverrideConfigurationBuilder.build());

    if (openStreamInformation.getEncryptionSecrets() != null
        && openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().isPresent()) {
      String customerKey = openStreamInformation.getEncryptionSecrets().getSsecCustomerKey().get();
      String customerKeyMd5 = openStreamInformation.getEncryptionSecrets().getSsecCustomerKeyMd5();
      builder
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(customerKey)
          .sseCustomerKeyMD5(customerKeyMd5);
    }

    return builder;
  }
}
