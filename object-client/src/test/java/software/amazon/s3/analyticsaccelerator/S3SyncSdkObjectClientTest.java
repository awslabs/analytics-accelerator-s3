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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.request.Constants.OPERATION_NAME;
import static software.amazon.s3.analyticsaccelerator.request.Constants.SPAN_ID;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.request.EncryptionSecrets;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.request.StreamAuditContext;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SyncSdkObjectClientTest {

  private static final String HEADER_REFERER = "Referer";
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  private static final String ETAG = "RandomString";
  private static final String USER_AGENT_HEADER = "User-Agent";
  private static final String AAL_USER_AGENT = "s3analyticsaccelerator";

  @Test
  void testForNullsInConstructor() {
    assertThrows(NullPointerException.class, () -> new S3SyncSdkObjectClient(null));
  }

  @Test
  void testCloseCallsInnerCloseWhenInstructed() throws IOException {
    S3Client s3Client = mock(S3Client.class);
    S3SyncSdkObjectClient s3SyncSdkObjectClient = new S3SyncSdkObjectClient(s3Client);
    s3SyncSdkObjectClient.close();
    verify(s3Client).close();
  }

  @Test
  void testHeadObject() throws IOException {
    try (S3Client s3Client = createMockClient()) {
      S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(s3Client);
      ObjectMetadata metadata =
          client.headObject(
              HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build(),
              OpenStreamInformation.DEFAULT);
      assertEquals(metadata, ObjectMetadata.builder().contentLength(42).etag(ETAG).build());
    }
  }

  @Test
  void testGetObjectWithDifferentEtagsThrowsError() throws IOException {
    try (S3Client s3Client = createMockClient()) {
      S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(s3Client);
      assertInstanceOf(
          ObjectContent.class,
          client.getObject(
              GetRequest.builder()
                  .s3Uri(S3URI.of("bucket", "key"))
                  .range(new Range(0, 20))
                  .etag(ETAG)
                  .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                  .build(),
              OpenStreamInformation.DEFAULT));
      try {
        client.getObject(
            GetRequest.builder()
                .s3Uri(S3URI.of("bucket", "key"))
                .range(new Range(0, 20))
                .etag("ANOTHER ONE")
                .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                .build(),
            OpenStreamInformation.DEFAULT);
      } catch (Exception e) {
        assertInstanceOf(IOException.class, e);
        assertInstanceOf(S3Exception.class, e.getCause());
      }
    }
  }

  @Test
  void testGetObjectWithRange() throws IOException {
    try (S3Client s3Client = createMockClient()) {
      S3SyncSdkObjectClient s3SyncSdkObjectClient = new S3SyncSdkObjectClient(s3Client);
      assertInstanceOf(
          ObjectContent.class,
          s3SyncSdkObjectClient.getObject(
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
  void testGetObjectAttachesExecutionAttributes() throws IOException {
    S3Client mockS3Client = createMockClient();

    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

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
    verify(mockS3Client).getObject(requestCaptor.capture());

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
  void testGetObjectDoesNotAttachExecutionAttributesWhenNotSet() throws IOException {
    S3Client mockS3Client = createMockClient();

    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

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
    verify(mockS3Client).getObject(requestCaptor.capture());

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
  void testHeadObjectAttachesExecutionAttributes() throws IOException {
    S3Client mockS3Client = createMockClient();

    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

    OpenStreamInformation openStreamInformation =
        OpenStreamInformation.builder()
            .streamAuditContext(
                StreamAuditContext.builder().spanId("12345").operationName("op_open").build())
            .build();

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3Client).headObject(requestCaptor.capture());

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
  void testHeadObjectDoesNotAttachExecutionAttributesWhenNotSet() throws IOException {
    S3Client mockS3Client = createMockClient();

    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.DEFAULT;

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3Client).headObject(requestCaptor.capture());

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

  @Test
  void testGetObjectWithEncryption() throws IOException {
    S3Client mockS3Client = createMockClient();
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

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
    verify(mockS3Client).getObject(requestCaptor.capture());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(ServerSideEncryption.AES256.name(), capturedRequest.sseCustomerAlgorithm());
    assertEquals(base64Key, capturedRequest.sseCustomerKey());
    assertNotNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testHeadObjectWithEncryption() throws IOException {
    S3Client mockS3Client = createMockClient();
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

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
    verify(mockS3Client).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertEquals(ServerSideEncryption.AES256.name(), capturedRequest.sseCustomerAlgorithm());
    assertEquals(base64Key, capturedRequest.sseCustomerKey());
    assertNotNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testGetObjectWithoutEncryption() throws IOException {
    S3Client mockS3Client = createMockClient();
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

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
    verify(mockS3Client).getObject(requestCaptor.capture());

    GetObjectRequest capturedRequest = requestCaptor.getValue();
    assertNull(capturedRequest.sseCustomerAlgorithm());
    assertNull(capturedRequest.sseCustomerKey());
    assertNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testHeadObjectWithoutEncryption() throws IOException {
    S3Client mockS3Client = createMockClient();
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

    OpenStreamInformation openStreamInformation = OpenStreamInformation.builder().build();

    HeadRequest headRequest = HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();

    client.headObject(headRequest, openStreamInformation);

    ArgumentCaptor<HeadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3Client).headObject(requestCaptor.capture());

    HeadObjectRequest capturedRequest = requestCaptor.getValue();
    assertNull(capturedRequest.sseCustomerAlgorithm());
    assertNull(capturedRequest.sseCustomerKey());
    assertNull(capturedRequest.sseCustomerKeyMD5());
  }

  @Test
  void testCustomUserAgentWithNullServiceConfiguration() throws IOException {
    S3Client s3Client = createMockClientForUserAgent(null);
    S3SyncSdkObjectClient s3SyncSdkObjectClient = new S3SyncSdkObjectClient(s3Client);

    s3SyncSdkObjectClient.headObject(
        HeadRequest.builder().s3Uri(TEST_URI).build(), OpenStreamInformation.DEFAULT);

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(s3Client).headObject(captor.capture());
    assertTrue(captor.getValue().overrideConfiguration().isPresent());
    String userAgent =
        captor.getValue().overrideConfiguration().get().headers().get(USER_AGENT_HEADER).get(0);
    assertTrue(userAgent.contains(AAL_USER_AGENT));
  }

  @Test
  void testCustomUserAgentWithNullOverrideConfiguration() throws IOException {
    S3ServiceClientConfiguration serviceConfig = S3ServiceClientConfiguration.builder().build();

    S3Client s3Client = createMockClientForUserAgent(serviceConfig);
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(s3Client);

    client.headObject(HeadRequest.builder().s3Uri(TEST_URI).build(), OpenStreamInformation.DEFAULT);

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(s3Client).headObject(captor.capture());
    assertTrue(captor.getValue().overrideConfiguration().isPresent());
    String userAgent =
        captor.getValue().overrideConfiguration().get().headers().get(USER_AGENT_HEADER).get(0);
    assertTrue(userAgent.contains(AAL_USER_AGENT));
  }

  @Test
  void testCustomUserAgentWithEmptyUserAgent() throws IOException {
    ClientOverrideConfiguration clientOverrideConfig =
        ClientOverrideConfiguration.builder()
            .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, "")
            .build();

    S3ServiceClientConfiguration serviceConfig =
        S3ServiceClientConfiguration.builder().overrideConfiguration(clientOverrideConfig).build();

    S3Client s3Client = createMockClientForUserAgent(serviceConfig);
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(s3Client);

    client.headObject(HeadRequest.builder().s3Uri(TEST_URI).build(), OpenStreamInformation.DEFAULT);

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(s3Client).headObject(captor.capture());
    assertTrue(captor.getValue().overrideConfiguration().isPresent());
    String userAgent =
        captor.getValue().overrideConfiguration().get().headers().get(USER_AGENT_HEADER).get(0);
    assertTrue(userAgent.contains(AAL_USER_AGENT));
  }

  @Test
  void testCustomUserAgentWithCustomValue() throws IOException {

    ClientOverrideConfiguration clientOverrideConfig =
        ClientOverrideConfiguration.builder()
            .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, "custom-agent")
            .build();

    S3ServiceClientConfiguration serviceConfig =
        S3ServiceClientConfiguration.builder().overrideConfiguration(clientOverrideConfig).build();

    S3Client s3Client = createMockClientForUserAgent(serviceConfig);
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(s3Client);

    client.headObject(HeadRequest.builder().s3Uri(TEST_URI).build(), OpenStreamInformation.DEFAULT);

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(s3Client).headObject(captor.capture());
    assertTrue(captor.getValue().overrideConfiguration().isPresent());
    String userAgent =
        captor.getValue().overrideConfiguration().get().headers().get(USER_AGENT_HEADER).get(0);
    assertTrue(userAgent.contains("custom-agent"));
    assertTrue(userAgent.contains(AAL_USER_AGENT));
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testHeadObjectExceptions(Exception exception) {
    S3Client mockS3Client = mock(S3Client.class);
    when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(exception);
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

    HeadRequest headRequest = HeadRequest.builder().s3Uri(TEST_URI).build();

    Throwable actualException =
        assertThrows(
            Throwable.class,
            () -> {
              client.headObject(headRequest, OpenStreamInformation.DEFAULT);
            });

    assertObjectClientExceptions(exception, actualException);
  }

  @SuppressWarnings({"unchecked"})
  @ParameterizedTest
  @MethodSource("exceptions")
  void testGetObjectExceptions(Exception exception) {
    S3Client mockS3Client = mock(S3Client.class);
    when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(exception);
    S3SyncSdkObjectClient client = new S3SyncSdkObjectClient(mockS3Client);

    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(TEST_URI)
            .etag("RANDOM")
            .range(new Range(0, 20))
            .referrer(new Referrer("original-referrer", ReadMode.SYNC))
            .build();

    Throwable actualException =
        assertThrows(
            Throwable.class,
            () -> {
              client.getObject(getRequest, OpenStreamInformation.DEFAULT);
            });

    assertObjectClientExceptions(exception, actualException);
  }

  private static Exception[] exceptions() {
    return ExceptionHandler.getSampleExceptions();
  }

  private static void assertObjectClientExceptions(
      final Exception expectedException, final Throwable t) {
    // assertInstanceOf(UncheckedIOException.class, t);
    assertInstanceOf(IOException.class, t);
    Optional.ofNullable(t.getCause())
        .ifPresent(
            underlyingException ->
                assertInstanceOf(expectedException.getClass(), underlyingException));
  }

  private S3Client createMockClientForUserAgent(S3ServiceClientConfiguration serviceConfig) {
    S3Client s3AsyncClient = mock(S3Client.class);
    when(s3AsyncClient.serviceClientConfiguration()).thenReturn(serviceConfig);

    HeadObjectResponse response =
        HeadObjectResponse.builder().contentLength(42L).eTag(ETAG).build();
    when(s3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(response);

    return s3AsyncClient;
  }

  private static S3Client createMockClient() {
    S3Client s3Client = mock(S3Client.class);

    when(s3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().contentLength(42L).eTag(ETAG).build());

    /*
     The argument matcher is used to check if our arguments match the values we want to mock a return for
     (https://www.baeldung.com/mockito-argument-matchers)
     If the header doesn't exist we want return true and return a positive response
     Or if the header matches we want to return our positive response.
    */
    when(s3Client.getObject(
            argThat(
                (ArgumentMatcher<GetObjectRequest>)
                    request -> {
                      if (request == null) {
                        return false;
                      }
                      // Check if the If-Match header matches expected ETag
                      return request.ifMatch() == null || request.ifMatch().equals(ETAG);
                    })))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStreamSubscriber.builder().build()));

    /*
     Here we check if our header is present and the etags dont match then we expect an error to be thrown.
    */
    when(s3Client.getObject(
            argThat(
                (ArgumentMatcher<GetObjectRequest>)
                    request -> {
                      if (request == null) {
                        return false;
                      }
                      return (request).ifMatch() != null && !(request).ifMatch().equals(ETAG);
                    })))
        .thenThrow(S3Exception.builder().message("PreconditionFailed").statusCode(412).build());

    doNothing().when(s3Client).close();

    return s3Client;
  }
}
