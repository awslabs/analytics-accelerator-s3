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
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static software.amazon.s3.analyticsaccelerator.request.Constants.OPERATION_NAME;
import static software.amazon.s3.analyticsaccelerator.request.Constants.SPAN_ID;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = {"NP_NONNULL_PARAM_VIOLATION", "SIC_INNER_SHOULD_BE_STATIC_ANON"},
    justification =
        "We mean to pass nulls to checks. Also, closures cannot be made static in this case")
public class S3SdkObjectClientTest {

  private static final String HEADER_REFERER = "Referer";
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  private static final String ETAG = "RandomString";

  @Test
  void testForNullsInConstructor() {
    try (S3AsyncClient client = mock(S3AsyncClient.class)) {
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new S3SdkObjectClient(null, ObjectClientConfiguration.DEFAULT, true));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class, () -> new S3SdkObjectClient(client, null, true));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new S3SdkObjectClient(null, ObjectClientConfiguration.DEFAULT));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class, () -> new S3SdkObjectClient(null, true));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class, () -> new S3SdkObjectClient(null));
    }
  }

  @Test
  void testCloseCallsInnerCloseWhenInstructed() {
    S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient, true);

    AtomicBoolean closed = new AtomicBoolean(false);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                closed.set(true);
                return null;
              }
            })
        .when(s3AsyncClient)
        .close();
    client.close();
    assertTrue(closed.get());
  }

  @Test
  void testCloseDoesNotCallInnerCloseWhenInstructed() {
    S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient, false);

    AtomicBoolean closed = new AtomicBoolean(false);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                closed.set(true);
                return null;
              }
            })
        .when(s3AsyncClient)
        .close();
    client.close();
    assertFalse(closed.get());
  }

  @Test
  void testConstructorWithWrappedClient() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertNotNull(client);
    }
  }

  @Test
  void testConstructorWithConfiguration() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      ObjectClientConfiguration configuration = ObjectClientConfiguration.DEFAULT;
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient, configuration);
      assertNotNull(client);
    }
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      assertThrows(
          NullPointerException.class,
          () -> {
            new S3SdkObjectClient(null, ObjectClientConfiguration.DEFAULT);
          });

      assertThrows(
          NullPointerException.class,
          () -> {
            new S3SdkObjectClient(s3AsyncClient, null);
          });
    }
  }

  @Test
  void testHeadObject() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      ObjectMetadata metadata =
          client
              .headObject(
                  HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build(),
                  OpenStreamInformation.DEFAULT)
              .join();
      assertEquals(metadata, ObjectMetadata.builder().contentLength(42).etag(ETAG).build());
    }
  }

  @Test
  void testGetObjectWithDifferentEtagsThrowsError() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertInstanceOf(
          CompletableFuture.class,
          client.getObject(
              GetRequest.builder()
                  .s3Uri(S3URI.of("bucket", "key"))
                  .range(new Range(0, 20))
                  .etag(ETAG)
                  .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                  .build(),
              OpenStreamInformation.DEFAULT));
      assertThrows(
          S3Exception.class,
          () ->
              client
                  .getObject(
                      GetRequest.builder()
                          .s3Uri(S3URI.of("bucket", "key"))
                          .range(new Range(0, 20))
                          .etag("ANOTHER ONE")
                          .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                          .build(),
                      OpenStreamInformation.DEFAULT)
                  .get());
    }
  }

  @Test
  void testGetObjectWithRange() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertInstanceOf(
          CompletableFuture.class,
          client.getObject(
              GetRequest.builder()
                  .s3Uri(S3URI.of("bucket", "key"))
                  .range(new Range(0, 20))
                  .etag(ETAG)
                  .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                  .build(),
              OpenStreamInformation.DEFAULT));
    }
  }

  @Test
  void testObjectClientClose() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      try (S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient)) {
        client.headObject(
            HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build(),
            OpenStreamInformation.DEFAULT);
      }
      verify(s3AsyncClient, times(1)).close();
    }
  }

  @Test
  void testGetObjectAttachesExecutionAttributes() {
    S3AsyncClient mockS3AsyncClient = createMockClient();

    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder()
            .streamAuditContext(
                StreamAuditContext.builder().spanId("12345").operationName("op_open").build())
            .build();

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(S3URI.of("bucket", "key"))
            .range(new Range(0, 20))
            .etag(ETAG)
            .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
            .build();

    client.getObject(getRequest, openStreamInformation);

    ArgumentCaptor<GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(mockS3AsyncClient)
        .getObject(
            requestCaptor.capture(),
            ArgumentMatchers
                .<AsyncResponseTransformer<
                        GetObjectResponse, ResponseInputStream<GetObjectResponse>>>
                    any());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(
        "12345",
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(SPAN_ID));
    assertEquals(
        "op_open",
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(OPERATION_NAME));
  }

  @Test
  void testGetObjectDoesNotAttachExecutionAttributesWhenNotSet() {
    S3AsyncClient mockS3AsyncClient = createMockClient();

    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.DEFAULT;

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(S3URI.of("bucket", "key"))
            .range(new Range(0, 20))
            .etag(ETAG)
            .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
            .build();

    client.getObject(getRequest, openStreamInformation);

    ArgumentCaptor<GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(mockS3AsyncClient)
        .getObject(
            requestCaptor.capture(),
            ArgumentMatchers
                .<AsyncResponseTransformer<
                        GetObjectResponse, ResponseInputStream<GetObjectResponse>>>
                    any());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(
        null,
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(SPAN_ID));
    assertEquals(
        null,
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(OPERATION_NAME));
  }

  @Test
  void testHeadObjectAttachesExecutionAttributes() {
    S3AsyncClient mockS3AsyncClient = createMockClient();

    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder()
            .streamAuditContext(
                StreamAuditContext.builder().spanId("12345").operationName("op_open").build())
            .build();

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3AsyncClient).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(
        "12345",
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(SPAN_ID));
    assertEquals(
        "op_open",
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(OPERATION_NAME));
  }

  @Test
  void testHeadObjectDoesNotAttachExecutionAttributesWhenNotSet() {
    S3AsyncClient mockS3AsyncClient = createMockClient();

    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.DEFAULT;

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3AsyncClient).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(
        null,
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(SPAN_ID));
    assertEquals(
        null,
        capturedRequest
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttributes()
            .get(OPERATION_NAME));
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testHeadObjectExceptions(Exception exception) {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(failedFuture);
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    HeadRequest headRequest = HeadRequest.builder().s3Uri(TEST_URI).build();
    CompletableFuture<ObjectMetadata> future =
        client.headObject(headRequest, OpenStreamInformation.DEFAULT);
    assertObjectClientExceptions(exception, future);
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testGetObjectExceptions(Exception exception) {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<ResponseInputStream<GetObjectResponse>> failedFuture =
        new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.getObject(
            any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(TEST_URI)
            .etag("RANDOM")
            .range(new Range(0, 20))
            .referrer(new Referrer("original-referrer", ReadMode.SYNC))
            .build();
    CompletableFuture<ObjectContent> future =
        client.getObject(getRequest, OpenStreamInformation.DEFAULT);
    assertObjectClientExceptions(exception, future);
  }

  @SuppressWarnings("unchecked")
  private static S3AsyncClient createMockClient() {
    S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);

    when(s3AsyncClient.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                HeadObjectResponse.builder().contentLength(42L).eTag(ETAG).build()));

    /*
     The argument matcher is used to check if our arguments match the values we want to mock a return for
     (https://www.baeldung.com/mockito-argument-matchers)
     If the header doesn't exist we want return true and return a positive response
     Or if the header matches we want to return our positive response.
    */
    when(s3AsyncClient.getObject(
            argThat(
                (ArgumentMatcher<GetObjectRequest>)
                    request -> {
                      if (request == null) {
                        return false;
                      }
                      // Check if the If-Match header matches expected ETag
                      return request.ifMatch() == null || request.ifMatch().equals(ETAG);
                    }),
            (AsyncResponseTransformer<GetObjectResponse, Object>) any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    AbortableInputStreamSubscriber.builder().build())));

    /*
     Here we check if our header is present and the etags dont match then we expect an error to be thrown.
    */
    when(s3AsyncClient.getObject(
            argThat(
                (ArgumentMatcher<GetObjectRequest>)
                    request -> {
                      if (request == null) {
                        return false;
                      }
                      return (request).ifMatch() != null && !(request).ifMatch().equals(ETAG);
                    }),
            (AsyncResponseTransformer<GetObjectResponse, Object>) any()))
        .thenThrow(S3Exception.builder().message("PreconditionFailed").statusCode(412).build());

    doNothing().when(s3AsyncClient).close();

    return s3AsyncClient;
  }

  private static void assertObjectClientExceptions(
      final Exception expectedException, final CompletableFuture<?> future) {
    Throwable wrappedException = assertThrows(CompletionException.class, future::join).getCause();
    assertInstanceOf(UncheckedIOException.class, wrappedException);
    Throwable thrownException = wrappedException.getCause();
    assertInstanceOf(IOException.class, thrownException);
    Optional.ofNullable(thrownException.getCause())
        .ifPresent(
            underlyingException ->
                assertInstanceOf(expectedException.getClass(), underlyingException));
  }

  private static Exception[] exceptions() {
    return ExceptionHandler.getSampleExceptions();
  }

  @Test
  void testGetObjectWithEncryption() {
    S3AsyncClient mockS3AsyncClient = createMockClient();
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    // Create encryption secrets
    String base64Key =
        Base64.getEncoder()
            .encodeToString("32-bytes-long-key-for-testing-123".getBytes(StandardCharsets.UTF_8));
    EncryptionSecrets secrets =
        EncryptionSecrets.builder().sseCustomerKey(Optional.of(base64Key)).build();

    // Create OpenStreamInformation with encryption
    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder().encryptionSecrets(secrets).build();

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(S3URI.of("bucket", "key"))
            .range(new Range(0, 20))
            .etag(ETAG)
            .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
            .build();

    client.getObject(getRequest, openStreamInformation);

    // Verify the encryption parameters
    ArgumentCaptor<GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(mockS3AsyncClient)
        .getObject(
            requestCaptor.capture(),
            ArgumentMatchers
                .<AsyncResponseTransformer<
                        GetObjectResponse, ResponseInputStream<GetObjectResponse>>>
                    any());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(ServerSideEncryption.AES256.name(), capturedRequest.sseCustomerAlgorithm());
    assertEquals(base64Key, capturedRequest.sseCustomerKey());
    assertNotNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testHeadObjectWithEncryption() {
    S3AsyncClient mockS3AsyncClient = createMockClient();
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    // Create encryption secrets
    String base64Key =
        Base64.getEncoder()
            .encodeToString("32-bytes-long-key-for-testing-123".getBytes(StandardCharsets.UTF_8));
    EncryptionSecrets secrets =
        EncryptionSecrets.builder().sseCustomerKey(Optional.of(base64Key)).build();

    // Create OpenStreamInformation with encryption
    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder().encryptionSecrets(secrets).build();

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    // Verify the encryption parameters
    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3AsyncClient).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(ServerSideEncryption.AES256.name(), capturedRequest.sseCustomerAlgorithm());
    assertEquals(base64Key, capturedRequest.sseCustomerKey());
    assertNotNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testGetObjectWithoutEncryption() {
    S3AsyncClient mockS3AsyncClient = createMockClient();
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.builder().build();

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(S3URI.of("bucket", "key"))
            .range(new Range(0, 20))
            .etag(ETAG)
            .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
            .build();

    client.getObject(getRequest, openStreamInformation);

    ArgumentCaptor<GetObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(mockS3AsyncClient)
        .getObject(
            requestCaptor.capture(),
            ArgumentMatchers
                .<AsyncResponseTransformer<
                        GetObjectResponse, ResponseInputStream<GetObjectResponse>>>
                    any());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertNull(capturedRequest.sseCustomerAlgorithm());
    assertNull(capturedRequest.sseCustomerKey());
    assertNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testHeadObjectWithoutEncryption() {
    S3AsyncClient mockS3AsyncClient = createMockClient();
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.builder().build();

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3AsyncClient).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertNull(capturedRequest.sseCustomerAlgorithm());
    assertNull(capturedRequest.sseCustomerKey());
    assertNull(capturedRequest.sseCustomerKeyMD5());
  }
}
