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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class MetadataStoreTest {

  @Test
  public void test__get__cacheWorks() throws IOException {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(objectMetadata));
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");

    // When: get(..) is called multiple times
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Then: object store was accessed only once
    verify(objectClient, times(1)).headObject(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void test__close__closesAllElements()
      throws IOException, ExecutionException, InterruptedException {
    // Given:
    // - a MetadataStore with caching turned on
    // - an Object Client returning a hanging future that throws when closed
    ObjectClient objectClient = mock(ObjectClient.class);

    HeadRequest h1 = HeadRequest.builder().s3Uri(S3URI.of("b", "key1")).build();
    HeadRequest h2 = HeadRequest.builder().s3Uri(S3URI.of("b", "key2")).build();
    OpenStreamInformation openStreamInformation = OpenStreamInformation.DEFAULT;

    CompletableFuture<ObjectMetadata> future = mock(CompletableFuture.class);
    when(future.isDone()).thenReturn(false);
    when(future.cancel(anyBoolean())).thenThrow(new RuntimeException("something horrible"));

    when(objectClient.headObject(h1, openStreamInformation)).thenReturn(future);

    CompletableFuture<ObjectMetadata> objectMetadataCompletableFuture =
        mock(CompletableFuture.class);

    when(objectClient.headObject(h2, openStreamInformation))
        .thenReturn(objectMetadataCompletableFuture);

    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));

    // When: MetadataStore is closed
    metadataStore.get(S3URI.of("b", "key1"), openStreamInformation);
    metadataStore.get(S3URI.of("b", "key2"), openStreamInformation);
    metadataStore.close();

    // Then: nothing has thrown, all futures were cancelled
    verify(objectMetadataCompletableFuture, times(1)).cancel(false);
  }

  @Test
  void testEvictKey_ExistingKey() {
    // Setup
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(mock(ObjectMetadata.class)));
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");
    metadataStore.storeObjectMetadata(key, ObjectMetadata.builder().etag("random").build());

    // Test
    boolean result = metadataStore.evictKey(key);

    // Verify
    assertTrue(result, "Evicting existing key should return true");
    result = metadataStore.evictKey(key);
    assertFalse(result, "Evicting existing key should return false");
  }

  @Test
  public void testHeadRequestCallbackCalled() throws IOException {
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(objectMetadata));
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));

    RequestCallback mockCallback = mock(RequestCallback.class);
    OpenStreamInformation openStreamInfo =
        OpenStreamInformation.builder().requestCallback(mockCallback).build();

    S3URI s3URI = S3URI.of("bucket", "key");

    metadataStore.get(s3URI, openStreamInfo);
    verify(mockCallback, times(1)).onHeadRequest();
  }

  @Test
  public void testTtlBasedEviction() throws IOException, InterruptedException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(1).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(objectMetadata));

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");

    // Get entry, wait for TTL expiry, then get again
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    Thread.sleep(3); // Wait for TTL expiry
    metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Object store was accessed twice due to TTL expiry
    verify(objectClient, times(2)).headObject(any(), any());
  }

  @Test
  public void testEtagConsistencyWithinStream() throws IOException, InterruptedException {
    // Test that same stream always gets same etag even if metadata TTL expires
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(5).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata1 = ObjectMetadata.builder().etag("etag-v1").contentLength(100).build();

    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(metadata1));

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "key");

    ObjectMetadata firstStreamMetadata = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("etag-v1", firstStreamMetadata.getEtag());

    Thread.sleep(10); // Wait for TTL expiry

    assertEquals("etag-v1", firstStreamMetadata.getEtag());

    verify(objectClient, times(1)).headObject(any(), any());
  }

  @Test
  public void testMetadataTtlProvidesNewVersionAfterExpiry()
      throws IOException, InterruptedException {
    // Test that TTL expiry allows getting updated object versions
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(20).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata oldVersion =
        ObjectMetadata.builder().etag("old-etag").contentLength(100).build();
    ObjectMetadata newVersion =
        ObjectMetadata.builder().etag("new-etag").contentLength(200).build();

    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(oldVersion))
        .thenReturn(CompletableFuture.completedFuture(newVersion));

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "updated-object");

    // Get old version of object
    ObjectMetadata first = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("old-etag", first.getEtag());
    assertEquals(100, first.getContentLength());

    Thread.sleep(30); // Wait for TTL expiry

    // Get new version of object
    ObjectMetadata second = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("new-etag", second.getEtag());
    assertEquals(200, second.getContentLength());

    verify(objectClient, times(2)).headObject(any(), any());
  }

  @Test
  public void testZeroTtlDisablesMetadataCache() throws IOException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(0).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata originalMetadata = ObjectMetadata.builder().etag("original-etag").build();
    ObjectMetadata updatedMetadata = ObjectMetadata.builder().etag("updated-etag").build();

    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(originalMetadata))
        .thenReturn(CompletableFuture.completedFuture(originalMetadata))
        .thenReturn(CompletableFuture.completedFuture(updatedMetadata));

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "key");

    // Create first 2 streams - each should get original etag
    ObjectMetadata stream1 = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    ObjectMetadata stream2 = metadataStore.get(key, OpenStreamInformation.DEFAULT);

    assertEquals("original-etag", stream1.getEtag());
    assertEquals("original-etag", stream2.getEtag());

    // 3rd stream should get updated etag
    ObjectMetadata stream3 = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("updated-etag", stream3.getEtag());

    verify(objectClient, times(3)).headObject(any(), any());
  }

  @Test
  public void testStoreObjectMetadataRefreshesTtlForExistingKey()
      throws IOException, InterruptedException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(100).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata = ObjectMetadata.builder().etag("test-etag").build();
    when(objectClient.headObject(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(metadata));

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "key");

    // First get to populate cache
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    verify(objectClient, times(1)).headObject(any(), any());

    // Wait partial TTL, then store same key(should refresh TTL)
    Thread.sleep(60);
    metadataStore.storeObjectMetadata(key, metadata);

    // Wait remaining time, should still be cached due to TTL refresh
    Thread.sleep(70); // Total 130ms , but TTL was expired after 100ms but got refreshed at 60ms
    ObjectMetadata retrieved = metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Should not trigger new HEAD request, still cached due to TTL refresh
    assertEquals("test-etag", retrieved.getEtag());
    verify(objectClient, times(1)).headObject(any(), any());
  }
}
