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
package software.amazon.s3.analyticsaccelerator.exceptions;

import static org.junit.jupiter.api.Assertions.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class ExceptionHandlerTest {
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testHandleNoSuchKeyException() {
    NoSuchKeyException cause = NoSuchKeyException.builder().build();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(FileNotFoundException.class, exception);
    assertInstanceOf(IOException.class, exception);
    assertNull(exception.getCause());
  }

  @Test
  void testHandleInvalidObjectStateException() {
    InvalidObjectStateException cause = InvalidObjectStateException.builder().build();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(IOException.class, exception);
    assertSame(cause, exception.getCause());
  }

  @Test
  void testHandleSdkClientException() {
    SdkClientException cause = SdkClientException.builder().build();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(IOException.class, exception);
    assertSame(cause, exception.getCause());
  }

  @Test
  void testHandleS3Exception() {
    AwsServiceException cause = S3Exception.builder().build();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(IOException.class, exception);
    assertSame(cause, exception.getCause());
  }

  @Test
  void testHandleSdkException() {
    RuntimeException cause = SdkException.builder().build();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(IOException.class, exception);
    assertSame(cause, exception.getCause());
  }

  @Test
  void testHandleRuntimeException() {
    RuntimeException cause = new RuntimeException();
    IOException exception = ExceptionHandler.toIOException(cause, TEST_URI);
    assertInstanceOf(IOException.class, exception);
    assertSame(cause, exception.getCause());
  }
}
