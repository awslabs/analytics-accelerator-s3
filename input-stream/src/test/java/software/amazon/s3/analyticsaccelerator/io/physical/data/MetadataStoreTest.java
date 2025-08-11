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
    when(objectClient.headObject(any(), any())).thenReturn(objectMetadata);
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
  void testEvictKey_ExistingKey() throws IOException {
    // Setup
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any(), any())).thenReturn(mock(ObjectMetadata.class));
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
    when(objectClient.headObject(any(), any())).thenReturn(objectMetadata);
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
}
