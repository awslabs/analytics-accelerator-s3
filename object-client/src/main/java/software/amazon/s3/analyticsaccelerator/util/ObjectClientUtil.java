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
package software.amazon.s3.analyticsaccelerator.util;

import static software.amazon.s3.analyticsaccelerator.request.Constants.OPERATION_NAME;
import static software.amazon.s3.analyticsaccelerator.request.Constants.SPAN_ID;

import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.request.StreamAuditContext;

/** Util class for common methods for client creation. */
public class ObjectClientUtil {

  /**
   * Method to translate SDK exceptions into IoExceptions.
   *
   * @param s3Uri S3 URI
   * @param throwable exception to translate
   * @return translated exception
   */
  public static Throwable handleException(S3URI s3Uri, Throwable throwable) {
    Throwable cause =
        Optional.ofNullable(throwable.getCause())
            .filter(
                t ->
                    throwable instanceof CompletionException
                        || throwable instanceof ExecutionException)
            .orElse(throwable);
    return ExceptionHandler.toIOException(cause, s3Uri);
  }

  /**
   * Attach additional information to the request. These parameters are used by execution
   * interceptors defined in S3A for request auditing.
   *
   * @param requestOverrideConfigurationBuilder Request config builder
   * @param streamAuditContext Audit context for the current request
   */
  public static void attachStreamContextToExecutionAttributes(
      AwsRequestOverrideConfiguration.Builder requestOverrideConfigurationBuilder,
      StreamAuditContext streamAuditContext) {
    requestOverrideConfigurationBuilder
        .putExecutionAttribute(SPAN_ID, streamAuditContext.getSpanId())
        .putExecutionAttribute(OPERATION_NAME, streamAuditContext.getOperationName());
  }
}
