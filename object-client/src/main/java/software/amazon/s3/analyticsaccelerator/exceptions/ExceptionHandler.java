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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Stream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Handles mapping of S3 exceptions to IO exceptions. */
public enum ExceptionHandler {
  NO_SUCH_KEY(
      NoSuchKeyException.class,
      (cause, uri) -> createFileNotFoundException("Object not found %s", uri)),

  INVALID_OBJECT_STATE(
      InvalidObjectStateException.class,
      (cause, uri) -> createIOException("Object %s is in invalid state", uri, cause)),

  SDK_CLIENT(
      SdkClientException.class,
      (cause, uri) -> createIOException("Client error accessing %s", uri, cause)),

  S3_SERVICE(
      S3Exception.class,
      (cause, uri) -> createIOException("Server error accessing %s", uri, cause)),

  SDK_GENERAL(
      SdkException.class, (cause, uri) -> createIOException("SDK error accessing %s", uri, cause));

  private final Class<? extends Exception> exceptionClass;
  private final ExceptionMapper mapper;

  @FunctionalInterface
  private interface ExceptionMapper {
    IOException toIOException(Throwable cause, S3URI uri);
  }

  ExceptionHandler(Class<? extends Exception> exceptionClass, ExceptionMapper mapper) {
    this.exceptionClass = exceptionClass;
    this.mapper = mapper;
  }

  /**
   * Maps the passed exception to an IOException, if possible. Otherwise, returns a generic
   * IOException wrapping the passed exception.
   *
   * @param cause The exception to be mapped
   * @param uri The S3URI that caused the exception
   * @return An IOException
   */
  public static IOException toIOException(Throwable cause, S3URI uri) {
    return Stream.of(values())
        .filter(handler -> handler.exceptionClass.isInstance(cause))
        .findFirst()
        .map(handler -> handler.mapper.toIOException(cause, uri))
        .orElseGet(() -> createIOException("Error accessing %s", uri, cause));
  }

  private static IOException createIOException(String message, S3URI uri, Throwable cause) {
    return new IOException(String.format(message, uri), cause);
  }

  private static FileNotFoundException createFileNotFoundException(String message, S3URI uri) {
    return new FileNotFoundException(String.format(message, uri));
  }
}
