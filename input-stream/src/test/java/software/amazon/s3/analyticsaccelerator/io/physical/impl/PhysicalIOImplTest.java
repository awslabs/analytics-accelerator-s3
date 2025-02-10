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
package software.amazon.s3.analyticsaccelerator.io.physical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MemoryManager;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class PhysicalIOImplTest {

  private static final S3URI s3URI = S3URI.of("foo", "bar");
  private static final String etag = "random";

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              null,
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              mock(StreamContext.class),
              mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              null,
              TestTelemetry.DEFAULT,
              mock(StreamContext.class),
              mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              null,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              null,
              mock(MemoryManager.class));
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI, null, mock(BlobStore.class), TestTelemetry.DEFAULT, mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              null,
              TestTelemetry.DEFAULT,
              mock(MemoryManager.class));
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              null,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              null,
              mock(MemoryManager.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              null,
              mock(MemoryManager.class));
        });
  }

  @Test
  public void test__readSingleByte_isCorrect() throws IOException {
    // Given: physicalIOImplV2
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT, mock(MemoryManager.class));

    // When: we read
    // Then: returned data is correct
    assertEquals(97, physicalIOImplV2.read(0)); // a
    assertEquals(98, physicalIOImplV2.read(1)); // b
    assertEquals(99, physicalIOImplV2.read(2)); // c
  }

  @Test
  public void test__regression_singleByteStream() throws IOException {
    // Given: physicalIOImplV2 backed by a single byte object
    final String TEST_DATA = "x";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT, mock(MemoryManager.class));

    // When: we read
    // Then: returned data is correct
    assertEquals(120, physicalIOImplV2.read(0)); // a
  }

  @Test
  void testReadWithBuffer() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT, mock(MemoryManager.class));

    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.read(buffer, 0, 5, 5));
  }

  @Test
  void testReadTail() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT, mock(MemoryManager.class));
    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.readTail(buffer, 0, 5));
    assertEquals(1, blobStore.blobCount());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void test_FailureEvictsObjectsAsExpected() throws IOException {
    AwsServiceException s3Exception =
        S3Exception.builder()
            .message("At least one of the pre-conditions you specified did not hold")
            .statusCode(412)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("PreconditionFailed")
                    .errorMessage("At least one of the preconditions you specified did not hold")
                    .serviceName("S3")
                    .build())
            .build();
    IOException ioException = new IOException(s3Exception);

    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<ResponseInputStream<GetObjectResponse>> failedFuture =
        new CompletableFuture<>();
    failedFuture.completeExceptionally(ioException);
    when(mockS3AsyncClient.getObject(
            any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);

    MetadataStore metadataStore =
        new MetadataStore(client, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().contentLength(100).etag(etag).build();
    metadataStore.storeObjectMetadata(s3URI, objectMetadata);
    BlobStore blobStore =
        new BlobStore(client, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT, mock(MemoryManager.class));

    assertThrows(IOException.class, () -> physicalIOImplV2.read(0));
    assertEquals(0, blobStore.blobCount());
    assertThrows(Exception.class, () -> metadataStore.get(s3URI));
  }
}
