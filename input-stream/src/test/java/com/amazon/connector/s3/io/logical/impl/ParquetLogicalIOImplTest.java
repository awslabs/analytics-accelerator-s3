package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MemoryTracker;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.request.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            S3URI.of("foo", "bar"),
            mock(PhysicalIO.class),
            TestTelemetry.DEFAULT,
            mock(LogicalIOConfiguration.class),
            mock(ParquetColumnPrefetchStore.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                null,
                TestTelemetry.DEFAULT,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                null,
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                mock(LogicalIOConfiguration.class),
                null));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .footerCachingEnabled(false)
            .metadataAwarePrefetchingEnabled(false)
            .build();

    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            TEST_URI,
            physicalIO,
            TestTelemetry.DEFAULT,
            configuration,
            new ParquetColumnPrefetchStore(configuration));

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }

  @Test
  void testMetadaWithZeroContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(0).build()));
    S3URI s3URI = S3URI.of("test", "test");
    MetadataStore metadataStore =
        new MetadataStore(mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            mockClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(MemoryTracker.class));
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                TestTelemetry.DEFAULT,
                LogicalIOConfiguration.DEFAULT,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testMetadataWithNegativeContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(-1).build()));
    S3URI s3URI = S3URI.of("test", "test");
    MetadataStore metadataStore =
        new MetadataStore(mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            mockClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(MemoryTracker.class));
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                TestTelemetry.DEFAULT,
                LogicalIOConfiguration.DEFAULT,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
  }
}
