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
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class SequentialLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar.data");

  @Test
  void testConstructor() {
    BlobStore blobStore = mock(BlobStore.class);
    assertNotNull(
        new SequentialLogicalIOImpl(
            TEST_URI,
            mock(PhysicalIO.class),
            TestTelemetry.DEFAULT,
            mock(LogicalIOConfiguration.class),
            blobStore));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    BlobStore blobStore = mock(BlobStore.class);
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = mock(LogicalIOConfiguration.class);

    assertThrows(
        NullPointerException.class,
        () ->
            new SequentialLogicalIOImpl(TEST_URI, null, TestTelemetry.DEFAULT, config, blobStore));

    assertThrows(
        NullPointerException.class,
        () ->
            new SequentialLogicalIOImpl(
                TEST_URI, physicalIO, TestTelemetry.DEFAULT, null, blobStore));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialLogicalIOImpl(TEST_URI, physicalIO, null, config, blobStore));

    assertThrows(
        NullPointerException.class,
        () ->
            new SequentialLogicalIOImpl(
                null, physicalIO, TestTelemetry.DEFAULT, config, blobStore));

    assertThrows(
        NullPointerException.class,
        () ->
            new SequentialLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, config, null));
  }

  @Test
  void testReadWithPrefetch() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    BlobStore blobStore = mock(BlobStore.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    ObjectMetadata mockMetadata = mock(ObjectMetadata.class);
    when(mockMetadata.getContentLength()).thenReturn(1000L);
    when(physicalIO.metadata()).thenReturn(mockMetadata);

    SequentialLogicalIOImpl logicalIO =
        new SequentialLogicalIOImpl(
            TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration, blobStore);

    byte[] buf = new byte[100];
    logicalIO.read(buf, 0, 100, 0);

    verify(physicalIO, atLeastOnce()).read(any(byte[].class), anyInt(), anyInt(), anyLong());
  }

  @Test
  void testClose() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    BlobStore blobStore = mock(BlobStore.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    ObjectMetadata mockMetadata = mock(ObjectMetadata.class);
    when(mockMetadata.getEtag()).thenReturn("testEtag");
    when(physicalIO.metadata()).thenReturn(mockMetadata);

    SequentialLogicalIOImpl logicalIO =
        new SequentialLogicalIOImpl(
            TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration, blobStore);

    logicalIO.close();

    verify(blobStore).evictKey(any(ObjectKey.class));
  }
}
