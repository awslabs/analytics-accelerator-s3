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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class DefaultLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new DefaultLogicalIOImpl(TEST_URI, mock(PhysicalIO.class), mock(Telemetry.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> new DefaultLogicalIOImpl(null, mock(PhysicalIO.class), mock(Telemetry.class)));
    assertThrows(
        NullPointerException.class,
        () -> new DefaultLogicalIOImpl(TEST_URI, null, mock(Telemetry.class)));
    assertThrows(
        NullPointerException.class,
        () -> new DefaultLogicalIOImpl(TEST_URI, mock(PhysicalIO.class), null));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    DefaultLogicalIOImpl logicalIO =
        new DefaultLogicalIOImpl(TEST_URI, physicalIO, mock(Telemetry.class));

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close(false);
  }

  @Test
  void testCloseWithEviction() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    DefaultLogicalIOImpl logicalIO =
        new DefaultLogicalIOImpl(TEST_URI, physicalIO, mock(Telemetry.class));

    // When: closeWithEviction called with true
    logicalIO.closeWithEviction(true);

    // Then: close will be called with true
    verify(physicalIO, times(1)).close(true);
  }

  @Test
  void testRead() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    DefaultLogicalIOImpl logicalIO =
        new DefaultLogicalIOImpl(TEST_URI, physicalIO, mock(Telemetry.class));
    logicalIO.read(5);
    verify(physicalIO).read(5);
  }

  @Test
  void testReadWithBuffer() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    DefaultLogicalIOImpl logicalIO = new DefaultLogicalIOImpl(TEST_URI, physicalIO, Telemetry.NOOP);

    byte[] buffer = new byte[5];
    logicalIO.read(buffer, 0, 5, 5);
    verify(physicalIO).read(buffer, 0, 5, 5L);
  }

  @Test
  void testReadTail() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    when(physicalIO.metadata())
        .thenReturn(ObjectMetadata.builder().contentLength(123).etag("random").build());
    DefaultLogicalIOImpl logicalIO = new DefaultLogicalIOImpl(TEST_URI, physicalIO, Telemetry.NOOP);
    byte[] buffer = new byte[5];
    logicalIO.readTail(buffer, 0, 5);
    verify(physicalIO).readTail(buffer, 0, 5);
  }

  @Test
  void testReadVectored() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    when(physicalIO.metadata())
        .thenReturn(ObjectMetadata.builder().contentLength(123).etag("random").build());
    DefaultLogicalIOImpl logicalIO =
        new DefaultLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT);
    List<ObjectRange> ranges = new ArrayList<>();
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    logicalIO.readVectored(ranges, allocate);
    verify(physicalIO).readVectored(ranges, allocate);
  }
}
