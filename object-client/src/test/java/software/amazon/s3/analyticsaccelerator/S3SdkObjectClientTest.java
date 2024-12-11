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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = {"NP_NONNULL_PARAM_VIOLATION", "SIC_INNER_SHOULD_BE_STATIC_ANON"},
    justification =
        "We mean to pass nulls to checks. Also, closures cannot be made static in this case")
public class S3SdkObjectClientTest {
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
    try (S3AsyncClient s3AsyncClient = createMockS3AsyncClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertNotNull(client);
    }
  }

  @Test
  void testConstructorWithConfiguration() {
    try (S3AsyncClient s3AsyncClient = createMockS3AsyncClient()) {
      ObjectClientConfiguration configuration = ObjectClientConfiguration.DEFAULT;
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient, configuration);
      assertNotNull(client);
    }
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    try (S3AsyncClient s3AsyncClient = createMockS3AsyncClient()) {
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
  void testHeadObjectWithS3AsyncClient() {
    try (S3AsyncClient s3AsyncClient = createMockS3AsyncClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertEquals(
          client.headObject(HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build()).join(),
          ObjectMetadata.builder().contentLength(42).build());
    }
  }

  @Test
  void testGetObjectWithRangeWithS3AsyncClient() {
    try (S3AsyncClient s3AsyncClient = createMockS3AsyncClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertInstanceOf(
          CompletableFuture.class,
          client.getObject(
              GetRequest.builder()
                  .s3Uri(S3URI.of("bucket", "key"))
                  .range(new Range(0, 20))
                  .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                  .build()));
    }
  }

  @Test
  void testObjectClientClose() {
    try (AwsClient s3AsyncClient = createMockS3AsyncClient()) {
      try (S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient)) {
        client.headObject(HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build());
      }
      verify(s3AsyncClient, times(1)).close();
    }
  }

  @Test
  void testDoNotCloseObjectClient() {
    AwsClient awsClient = createMockS3AsyncClient();
    S3SdkObjectClient client = new S3SdkObjectClient(awsClient, false);
    client.close();
    verify(awsClient, never()).close();
  }

  @Test
  void testConstructorWithAwsClient() {
    AwsClient awsClient = createMockS3AsyncClient();
    S3SdkObjectClient client = new S3SdkObjectClient(awsClient);
    assertNotNull(client);
    assertEquals(awsClient, client.getS3Client());
  }

  @Test
  void testHeadObjectWithS3Client() {
    S3Client syncClient = createMockS3Client();
    S3SdkObjectClient client = new S3SdkObjectClient(syncClient);
    assertHeadObjectResult(client);
  }

  @Test
  void testGetObjectWithS3Client() {
    S3Client syncClient = createMockS3Client();
    S3SdkObjectClient client = new S3SdkObjectClient(syncClient);
    assertGetObjectResult(client);
  }

  @Test
  void testUnsupportedClientType() {
    AwsClient unsupportedClient = mock(AwsClient.class);
    S3SdkObjectClient client = new S3SdkObjectClient(unsupportedClient);

    assertThrows(
        UnsupportedOperationException.class, () -> client.headObject(createHeadRequest()).join());

    assertThrows(
        UnsupportedOperationException.class, () -> client.getObject(createGetRequest()).join());
  }

  @SuppressWarnings("unchecked")
  private static S3AsyncClient createMockS3AsyncClient() {
    S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);

    when(s3AsyncClient.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                HeadObjectResponse.builder().contentLength(42L).build()));

    when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    AbortableInputStreamSubscriber.builder().build())));

    doNothing().when(s3AsyncClient).close();

    return s3AsyncClient;
  }

  private static S3Client createMockS3Client() {
    S3Client s3Client = mock(S3Client.class);

    when(s3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().contentLength(42L).build());

    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStreamSubscriber.builder().build()));

    doNothing().when(s3Client).close();

    return s3Client;
  }

  private void assertHeadObjectResult(S3SdkObjectClient client) {
    CompletableFuture<ObjectMetadata> result = client.headObject(createHeadRequest());
    assertNotNull(result);
    assertEquals(42L, result.join().getContentLength());
  }

  private void assertGetObjectResult(S3SdkObjectClient client) {
    CompletableFuture<ObjectContent> result = client.getObject(createGetRequest());
    assertNotNull(result);
    assertDoesNotThrow(() -> result.join());
  }

  private HeadRequest createHeadRequest() {
    return HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build();
  }

  private GetRequest createGetRequest() {
    return GetRequest.builder()
        .s3Uri(S3URI.of("bucket", "key"))
        .range(new Range(0, 20))
        .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
        .build();
  }
}
