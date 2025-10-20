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

import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.s3.analyticsaccelerator.request.Constants.OPERATION_NAME;
import static software.amazon.s3.analyticsaccelerator.request.Constants.SPAN_ID;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.s3.analyticsaccelerator.request.StreamAuditContext;

public class ObjectClientUtilTest {

  private final S3URI S3_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testExceptionHandlerWithCompletionException() {
    RuntimeException causeException = new RuntimeException("Root cause");
    CompletionException completionException =
        new CompletionException("Completion failed", causeException);

    Throwable result = ObjectClientUtil.handleException(S3_URI, completionException);

    assertInstanceOf(IOException.class, result);
    assertInstanceOf(RuntimeException.class, result.getCause());
    assertEquals("Error accessing s3://test-bucket/test-key", result.getMessage());
  }

  @Test
  void testExceptionHandlerWithExecutionException() {
    ExecutionException executionException =
        new ExecutionException(
            "Execution failed",
            InvalidObjectStateException.builder().message("InvalidObjectState").build());

    Throwable result = ObjectClientUtil.handleException(S3_URI, executionException);

    assertInstanceOf(IOException.class, result);
    assertInstanceOf(InvalidObjectStateException.class, result.getCause());
    assertEquals(
        "Object s3://test-bucket/test-key is in invalid state, failed with: InvalidObjectState",
        result.getMessage());
  }

  @Test
  void testExceptionHandlerWithSDKClientException() {
    SdkClientException sdkClientException =
        SdkClientException.builder().message("SdkClientException").build();

    Throwable result = ObjectClientUtil.handleException(S3_URI, sdkClientException);

    assertInstanceOf(IOException.class, result);
    assertInstanceOf(SdkClientException.class, result.getCause());
    assertEquals(
        "Client error accessing s3://test-bucket/test-key, failed with: SdkClientException",
        result.getMessage());
  }

  @Test
  void attachStreamContextToExecutionAttributesTest() {
    AwsRequestOverrideConfiguration.Builder requestOverrideConfigurationBuilder =
        AwsRequestOverrideConfiguration.builder();
    ObjectClientUtil.attachStreamContextToExecutionAttributes(
        requestOverrideConfigurationBuilder,
        StreamAuditContext.builder()
            .spanId("test-span-id")
            .operationName("test-operation")
            .build());

    assertEquals(
        requestOverrideConfigurationBuilder.executionAttributes().getAttribute(SPAN_ID),
        "test-span-id");
    assertEquals(
        requestOverrideConfigurationBuilder.executionAttributes().getAttribute(OPERATION_NAME),
        "test-operation");
  }
}
