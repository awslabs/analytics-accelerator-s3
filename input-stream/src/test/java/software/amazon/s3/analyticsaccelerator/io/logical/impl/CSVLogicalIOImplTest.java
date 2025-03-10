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
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class CSVLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar.csv");

  @Test
  void testConstructor() {
    assertNotNull(
        new CSVLogicalIOImpl(
            S3URI.of("foo", "bar.csv"),
            mock(PhysicalIO.class),
            TestTelemetry.DEFAULT,
            mock(LogicalIOConfiguration.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () ->
            new CSVLogicalIOImpl(
                TEST_URI, null, TestTelemetry.DEFAULT, mock(LogicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () -> new CSVLogicalIOImpl(TEST_URI, mock(PhysicalIO.class), TestTelemetry.DEFAULT, null));
    assertThrows(
        NullPointerException.class,
        () ->
            new CSVLogicalIOImpl(
                TEST_URI, mock(PhysicalIO.class), null, mock(LogicalIOConfiguration.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new CSVLogicalIOImpl(
                null,
                mock(PhysicalIO.class),
                TestTelemetry.DEFAULT,
                mock(LogicalIOConfiguration.class)));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    CSVLogicalIOImpl logicalIO =
        new CSVLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration);

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }

  @Test
  void testMetadataWithZeroContentLength() throws IOException {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(0).etag("random").build()));
    S3URI s3URI = S3URI.of("test", "test.csv");
    MetadataStore metadataStore =
        new MetadataStore(mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(mockClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);
    assertDoesNotThrow(
        () ->
            new CSVLogicalIOImpl(
                TEST_URI, physicalIO, TestTelemetry.DEFAULT, LogicalIOConfiguration.DEFAULT));
  }

  @Test
  void testReadWithPrefetch() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    // Mock the behavior of physicalIO.metadata()
    ObjectMetadata mockMetadata = mock(ObjectMetadata.class);
    when(mockMetadata.getContentLength()).thenReturn(1000L);
    when(physicalIO.metadata()).thenReturn(mockMetadata);

    // Mock the behavior of physicalIO.execute()
    when(physicalIO.execute(any()))
        .thenReturn(IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build());

    CSVLogicalIOImpl logicalIO =
        new CSVLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration);

    byte[] buf = new byte[100];
    logicalIO.read(buf, 0, 100, 0);

    // Verify that read is called on physicalIO
    // We can't verify the exact parameters because the prefetcher might change them
    verify(physicalIO, atLeastOnce()).read(any(byte[].class), anyInt(), anyInt(), anyLong());
  }

  @Test
  public void testCloseWithException() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    Telemetry telemetry = mock(Telemetry.class);
    LogicalIOConfiguration config = mock(LogicalIOConfiguration.class);
    S3URI s3URI = mock(S3URI.class);

    CSVLogicalIOImpl csvLogicalIO = new CSVLogicalIOImpl(s3URI, physicalIO, telemetry, config);

    doThrow(new IOException("Test exception")).when(physicalIO).close();

    assertThrows(IOException.class, () -> csvLogicalIO.close());
  }
}
