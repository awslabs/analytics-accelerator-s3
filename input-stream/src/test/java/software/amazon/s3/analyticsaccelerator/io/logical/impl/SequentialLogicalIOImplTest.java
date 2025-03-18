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
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class SequentialLogicalIOImplTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar.data");

  @Test
  void testConstructor() {
    assertNotNull(
        new SequentialLogicalIOImpl(
            TEST_URI,
            mock(PhysicalIO.class),
            TestTelemetry.DEFAULT,
            mock(LogicalIOConfiguration.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration config = mock(LogicalIOConfiguration.class);

    assertThrows(
        NullPointerException.class,
        () -> new SequentialLogicalIOImpl(TEST_URI, null, TestTelemetry.DEFAULT, config));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, null));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialLogicalIOImpl(TEST_URI, physicalIO, null, config));

    assertThrows(
        NullPointerException.class,
        () -> new SequentialLogicalIOImpl(null, physicalIO, TestTelemetry.DEFAULT, config));
  }

  @Test
  void testReadCallsPrefetcher() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    // Mock SequentialPrefetcher
    try (MockedConstruction<SequentialPrefetcher> mockedPrefetcher =
        mockConstruction(SequentialPrefetcher.class)) {
      SequentialLogicalIOImpl logicalIO =
          new SequentialLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration);

      byte[] buf = new byte[10];
      int off = 0;
      int len = 10;
      long position = 0;

      logicalIO.read(buf, off, len, position);

      // Verify that the prefetcher's prefetch method was called with the correct position
      SequentialPrefetcher constructedPrefetcher = mockedPrefetcher.constructed().get(0);
      verify(constructedPrefetcher).prefetch(position);
    }
  }

  @Test
  void testClose() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();

    SequentialLogicalIOImpl logicalIO =
        new SequentialLogicalIOImpl(TEST_URI, physicalIO, TestTelemetry.DEFAULT, configuration);

    logicalIO.close();

    verify(physicalIO).close(true);
  }
}
