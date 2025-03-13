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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.DefaultLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.SequentialLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.InputPolicy;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamFactoryTest {
  private static S3URI s3URI = S3URI.of("bucket", "key");
  private static int CONTENT_LENGTH = 500;
  private static final ObjectMetadata objectMetadata =
      ObjectMetadata.builder().contentLength(CONTENT_LENGTH).etag("ETAG").build();

  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testConstructor() {
    ObjectClient objectClient = mock(ObjectClient.class);
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(objectClient, S3SeekableInputStreamConfiguration.DEFAULT);
    assertEquals(
        S3SeekableInputStreamConfiguration.DEFAULT,
        s3SeekableInputStreamFactory.getConfiguration());
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(null, S3SeekableInputStreamConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(mock(ObjectClient.class), null);
        });
  }

  @Test
  void testCreateDefaultStream() throws IOException {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());

    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(s3URI, objectMetadata);
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);

    inputStream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of("bucket", "key"), mock(OpenStreamInformation.class));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamWithContentLengthAndEtag() throws IOException {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(s3URI, objectMetadata);
    assertNotNull(inputStream);
    assertEquals(
        CONTENT_LENGTH,
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getContentLength());
    assertEquals(
        objectMetadata.getEtag(),
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getEtag());
  }

  @Test
  void testPreconditions() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());

    assertThrows(
        Exception.class,
        () ->
            s3SeekableInputStreamFactory.createStream(
                s3URI, ObjectMetadata.builder().contentLength(-1).build()));
  }

  @Test
  void testCreateIndependentStream() throws IOException {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(s3URI, objectMetadata);

    S3SeekableInputStream inputStream = s3SeekableInputStreamFactory.createStream(s3URI);
    assertNotNull(inputStream);

    inputStream =
        s3SeekableInputStreamFactory.createStream(s3URI, mock(OpenStreamInformation.class));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamThrowsOnNullArgument() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null, mock(OpenStreamInformation.class));
        });
  }

  @Test
  void testCreateLogicalIO() throws IOException {
    S3URI testURIParquet = S3URI.of("bucket", "key.parquet");
    S3URI testURIKEYPAR = S3URI.of("bucket", "key.par");
    S3URI testURIJAVA = S3URI.of("bucket", "key.java");
    S3URI testURITXT = S3URI.of("bucket", "key.txt");
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIParquet, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIKEYPAR, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIJAVA, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURITXT, objectMetadata);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                testURIParquet, mock(OpenStreamInformation.class))
            instanceof ParquetLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                testURIKEYPAR, mock(OpenStreamInformation.class))
            instanceof ParquetLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURIJAVA, mock(OpenStreamInformation.class))
            instanceof DefaultLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURITXT, mock(OpenStreamInformation.class))
            instanceof DefaultLogicalIOImpl);
    OpenStreamInformation sequentialOpenStreamInfo = mock(OpenStreamInformation.class);
    when(sequentialOpenStreamInfo.getInputPolicy()).thenReturn(InputPolicy.Sequential);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURIParquet, sequentialOpenStreamInfo)
            instanceof SequentialLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURITXT, sequentialOpenStreamInfo)
            instanceof SequentialLogicalIOImpl);
  }

  @Test
  void testClose() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertDoesNotThrow(() -> s3SeekableInputStreamFactory.close());
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testHeadObjectExceptions(Exception exception) throws IOException {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(failedFuture);

    assertInputStreamHeadException(exception, mockS3AsyncClient);
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testGetObjectExceptions(Exception exception) throws IOException {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    // As headObject call happens first, we make a successful headObject mocking so that failure
    // gets triggered only at the getObject
    CompletableFuture<HeadObjectResponse> successfulFuture = new CompletableFuture<>();
    successfulFuture.complete(HeadObjectResponse.builder().contentLength(1L).eTag("fish").build());
    when(mockS3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(successfulFuture);

    CompletableFuture<ResponseInputStream<GetObjectResponse>> failedFuture =
        new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.getObject(
            any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);

    assertInputStreamReadExceptions(exception, mockS3AsyncClient);
  }

  private static void assertInputStreamReadExceptions(
      final Exception expectedException, final S3AsyncClient mockS3AsyncClient) throws IOException {
    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(mockS3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
    S3SeekableInputStream inputStream =
        factory.createStream(TEST_URI, mock(OpenStreamInformation.class));
    Exception thrownException = assertThrows(Exception.class, inputStream::read);
    assertInstanceOf(IOException.class, thrownException);
    Optional.ofNullable(thrownException.getCause())
        .ifPresent(
            underlyingException ->
                assertInstanceOf(expectedException.getClass(), underlyingException));
  }

  private static void assertInputStreamHeadException(
      final Exception expectedException, final S3AsyncClient mockS3AsyncClient) throws IOException {
    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(mockS3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
    Exception thrownException =
        assertThrows(
            Exception.class,
            () -> factory.createStream(TEST_URI, mock(OpenStreamInformation.class)));
    assertInstanceOf(IOException.class, thrownException);
    Optional.ofNullable(thrownException.getCause())
        .ifPresent(
            underlyingException ->
                assertInstanceOf(expectedException.getClass(), underlyingException));
  }

  private static Exception[] exceptions() {
    return ExceptionHandler.getSampleExceptions();
  }
}
