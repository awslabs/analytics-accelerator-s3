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
package software.amazon.s3.dataaccelerator.io.logical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.TestTelemetry;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.dataaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.dataaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.dataaccelerator.request.HeadRequest;
import software.amazon.s3.dataaccelerator.request.ObjectClient;
import software.amazon.s3.dataaccelerator.request.ObjectMetadata;
import software.amazon.s3.dataaccelerator.util.PrefetchMode;
import software.amazon.s3.dataaccelerator.util.S3URI;

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
            .prefetchingMode(PrefetchMode.OFF)
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
            metadataStore, mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
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
            metadataStore, mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
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
