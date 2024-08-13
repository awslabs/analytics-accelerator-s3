package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;

public class BlobStoreTest {
  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                null,
                mock(ObjectClient.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(MetadataStore.class),
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(MetadataStore.class),
                mock(ObjectClient.class),
                null,
                mock(PhysicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlobStore(
                mock(MetadataStore.class), mock(ObjectClient.class), mock(Telemetry.class), null));
  }

  @Test
  public void testGetReturnsReadableBlob() {
    // Given: a BlobStore with an underlying metadata store and object client
    final String TEST_DATA = "test-data";
    ObjectClient objectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(TEST_DATA.length()).build());
    BlobStore blobStore =
        new BlobStore(metadataStore, objectClient, Telemetry.NOOP, PhysicalIOConfiguration.DEFAULT);

    // When: a Blob is asked for
    Blob blob = blobStore.get(S3URI.of("test", "test"));

    // Then:
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);
    assertEquals(TEST_DATA, new String(b));
  }
}
