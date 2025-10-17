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
package software.amazon.s3.analyticsaccelerator.io.logical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.DefaultRequestCallbackImpl;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private final ExecutorService threadPool = Executors.newFixedThreadPool(30);

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            S3URI.of("foo", "bar"),
            mock(PhysicalIO.class),
            TestTelemetry.DEFAULT,
            mock(LogicalIOConfiguration.class),
            mock(ParquetColumnPrefetchStore.class),
            new DefaultRequestCallbackImpl()));
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
                mock(ParquetColumnPrefetchStore.class),
                new DefaultRequestCallbackImpl()));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                null,
                mock(ParquetColumnPrefetchStore.class),
                new DefaultRequestCallbackImpl()));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                new DefaultRequestCallbackImpl()));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                mock(LogicalIOConfiguration.class),
                null,
                new DefaultRequestCallbackImpl()));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                null,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                new DefaultRequestCallbackImpl()));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .prefetchFooterEnabled(false)
            .prefetchingMode(PrefetchMode.OFF)
            .build();

    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            TEST_URI,
            physicalIO,
            TestTelemetry.DEFAULT,
            configuration,
            new ParquetColumnPrefetchStore(configuration),
            new DefaultRequestCallbackImpl());

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close(false);
  }

  @Test
  void testMetadaWithZeroContentLength() throws IOException {
    ObjectClient mockClient = mock(ObjectClient.class);

    when(mockClient.headObject(any(HeadRequest.class), any(OpenStreamInformation.class)))
        .thenReturn(ObjectMetadata.builder().contentLength(0).etag("random").build());
    S3URI s3URI = S3URI.of("test", "test");
    MetadataStore metadataStore =
        new MetadataStore(
            mockClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            mockClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            threadPool);
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            mock(ExecutorService.class),
            PhysicalIOConfiguration.DEFAULT);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                TestTelemetry.DEFAULT,
                LogicalIOConfiguration.DEFAULT,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
                new DefaultRequestCallbackImpl()));
  }
}
