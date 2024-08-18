package com.amazon.connector.s3.io.physical.data;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class MetadataStoreTest {

  @Test
  public void test__get__cacheWorks() {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(CompletableFuture.completedFuture(mock(ObjectMetadata.class)));
    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    S3URI key = S3URI.of("foo", "bar");

    // When: get(..) is called multiple times
    metadataStore.get(key);
    metadataStore.get(key);
    metadataStore.get(key);

    // Then: object store was accessed only once
    verify(objectClient, times(1)).headObject(any());
  }

  @Test
  public void test__close__closesAllElements() {
    // Given:
    // - a MetadataStore with caching turned on
    // - an Object Client returning a hanging future that throws when closed
    ObjectClient objectClient = mock(ObjectClient.class);

    HeadRequest h1 = HeadRequest.builder().bucket("b").key("key1").build();
    HeadRequest h2 = HeadRequest.builder().bucket("b").key("key2").build();

    CompletableFuture<ObjectMetadata> future = mock(CompletableFuture.class);
    when(future.isDone()).thenReturn(false);
    when(future.cancel(anyBoolean())).thenThrow(new RuntimeException("something horrible"));

    when(objectClient.headObject(h1)).thenReturn(future);
    CompletableFuture<ObjectMetadata> objectMetadataCompletableFuture =
        mock(CompletableFuture.class);
    when(objectClient.headObject(h2)).thenReturn(objectMetadataCompletableFuture);

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);

    // When: MetadataStore is closed
    metadataStore.get(S3URI.of("b", "key1"));
    metadataStore.get(S3URI.of("b", "key2"));
    metadataStore.close();

    // Then: nothing has thrown, all futures were cancelled
    verify(objectMetadataCompletableFuture, times(1)).cancel(false);
  }
}
