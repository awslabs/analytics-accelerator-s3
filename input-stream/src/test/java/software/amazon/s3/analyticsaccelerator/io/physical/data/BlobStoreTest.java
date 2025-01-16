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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
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
  public void testGetReturnsReadableBlob() throws IOException {
    // Given: a BlobStore with an underlying metadata store and object client
    final String TEST_DATA = "test-data";
    ObjectClient objectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(TEST_DATA.length()).build());
    BlobStore blobStore =
        new BlobStore(
            metadataStore, objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);

    // When: a Blob is asked for
    Blob blob = blobStore.get(S3URI.of("test", "test"), mock(StreamContext.class));

    // Then:
    byte[] b = new byte[TEST_DATA.length()];
    blob.read(b, 0, b.length, 0);
    assertEquals(TEST_DATA, new String(b, StandardCharsets.UTF_8));
  }
}
