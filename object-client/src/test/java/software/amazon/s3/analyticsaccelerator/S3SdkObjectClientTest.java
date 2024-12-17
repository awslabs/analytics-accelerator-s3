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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
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
          () -> new S3SdkObjectClient(null, ObjectClientConfiguration.DEFAULT, true, null));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class, () -> new S3SdkObjectClient(client, null, true, null));
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

  /*@Test
  void testConstructorThrowsOnNullArgument() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      assertThrows(
          NullPointerException.class,
          () -> {
            new S3SdkObjectClient(null, ObjectClientConfiguration.DEFAULT);
          });

      /*assertThrows(
      NullPointerException.class,
      () -> {
        ObjectClientConfiguration configuration = null;
        new S3SdkObjectClient(s3AsyncClient, null);
      });
    }
  }*/

  @Test
  void testHeadObject() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
      assertEquals(
          client.headObject(HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build()).join(),
          ObjectMetadata.builder().contentLength(42).build());
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
                  .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                  .build()));
    }
  }

  @Test
  void testObjectClientClose() {
    try (S3AsyncClient s3AsyncClient = createMockClient()) {
      try (S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient)) {
        client.headObject(HeadRequest.builder().s3Uri(S3URI.of("bucket", "key")).build());
      }
      verify(s3AsyncClient, times(1)).close();
    }
  }

  @SuppressWarnings("unchecked")
  private static S3AsyncClient createMockClient() {
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
}
