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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.EOFException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Tests for negative position handling in seek operations.
 */
public class SeekNegativePositionTest {
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testSeekToNegativePositionThrowsEOFException() throws IOException {
    // Given: A seekable input stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, mockLogicalIO, TestTelemetry.DEFAULT)) {

      // When: Attempting to seek to a negative position
      // Then: EOFException is thrown (matching S3A behavior)
      EOFException exception = assertThrows(EOFException.class, () -> stream.seek(-1));
      assertTrue(
          exception.getMessage().contains("negative"),
          "Exception message should mention negative position");
    }
  }

  @Test
  void testSeekToNegativePositionWithLargeNegativeValue() throws IOException {
    // Given: A seekable input stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, mockLogicalIO, TestTelemetry.DEFAULT)) {

      // When: Attempting to seek to a large negative position
      // Then: EOFException is thrown
      assertThrows(EOFException.class, () -> stream.seek(-1000000));
    }
  }

  @Test
  void testSeekToZeroIsValid() throws IOException {
    // Given: A seekable input stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, mockLogicalIO, TestTelemetry.DEFAULT)) {

      // When: Seeking to position 0
      stream.seek(0);

      // Then: No exception is thrown and position is 0
      assertEquals(0, stream.getPos());
    }
  }

  @Test
  void testSeekToPositivePositionIsValid() throws IOException {
    // Given: A seekable input stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    try (S3SeekableInputStream stream =
        new S3SeekableInputStream(TEST_URI, mockLogicalIO, TestTelemetry.DEFAULT)) {

      // When: Seeking to a positive position
      stream.seek(100);

      // Then: No exception is thrown and position is updated
      assertEquals(100, stream.getPos());
    }
  }
}