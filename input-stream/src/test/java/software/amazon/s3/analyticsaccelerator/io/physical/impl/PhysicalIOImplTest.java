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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
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
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class PhysicalIOImplTest {

  private static final S3URI s3URI = S3URI.of("foo", "bar");
  private static final String etag = "random";
  private final ExecutorService executorService = Executors.newFixedThreadPool(30);

  @Test
  void testConstructorThrowsOnNullArgument() {

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              null,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              OpenStreamInformation.DEFAULT,
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              null,
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              mock(OpenStreamInformation.class),
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              null,
              TestTelemetry.DEFAULT,
              mock(OpenStreamInformation.class),
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              null,
              mock(OpenStreamInformation.class),
              executorService,
              PhysicalIOConfiguration.DEFAULT);
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
              null,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              null,
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              OpenStreamInformation.DEFAULT,
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              null,
              TestTelemetry.DEFAULT,
              OpenStreamInformation.DEFAULT,
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              null,
              OpenStreamInformation.DEFAULT,
              executorService,
              PhysicalIOConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              mock(BlobStore.class),
              TestTelemetry.DEFAULT,
              OpenStreamInformation.DEFAULT,
              null,
              PhysicalIOConfiguration.DEFAULT);
        });
  }

  @Test
  public void test__readSingleByte_isCorrect() throws IOException {
    // Given: physicalIOImplV2
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    // When: we read
    // Then: returned data is correct
    assertEquals(97, physicalIOImplV2.read(0)); // a
    assertEquals(98, physicalIOImplV2.read(1)); // b
    assertEquals(99, physicalIOImplV2.read(2)); // c
    assertThrows(IllegalArgumentException.class, () -> physicalIOImplV2.read(-1));
    assertThrows(IllegalArgumentException.class, () -> physicalIOImplV2.read(10000));
  }

  @Test
  public void test__regression_singleByteStream() throws IOException {
    // Given: physicalIOImplV2 backed by a single byte object
    final String TEST_DATA = "x";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    // When: we read
    // Then: returned data is correct
    assertEquals(120, physicalIOImplV2.read(0)); // a
  }

  @Test
  void testReadWithBuffer() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.read(buffer, 0, 5, 5));
  }

  @Test
  void testReadTail() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class),
            executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);
    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.readTail(buffer, 0, 5));
    assertEquals(1, blobStore.blobCount());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void test_FailureEvictsObjectsAsExpected() throws IOException, InterruptedException {
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
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().blockReadTimeout(200).blockReadRetryCount(2).build();

    MetadataStore metadataStore =
        new MetadataStore(client, TestTelemetry.DEFAULT, configuration, mock(Metrics.class));
    ObjectMetadata objectMetadata = ObjectMetadata.builder().contentLength(100).etag(etag).build();
    metadataStore.storeObjectMetadata(s3URI, objectMetadata);
    BlobStore blobStore =
        new BlobStore(
            client, TestTelemetry.DEFAULT, configuration, mock(Metrics.class), executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    assertThrows(IOException.class, () -> physicalIOImplV2.read(0));
    assertEquals(0, blobStore.blobCount());
    assertThrows(Exception.class, () -> metadataStore.get(s3URI, OpenStreamInformation.DEFAULT));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void test_FailureEvictsObjectsAsExpected_WhenSDKClientGetsStuck() throws IOException {
    AwsServiceException sdkException =
        S3Exception.builder()
            .message(
                "software.amazon.awssdk.services.s3.model.S3Exception: At least one of the pre-conditions you specified did not hold (Service: S3, Status Code: 412, Request ID")
            .build();
    IOException ioException = new IOException(sdkException);

    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<ResponseInputStream<GetObjectResponse>> failedFuture =
        new CompletableFuture<>();
    failedFuture.completeExceptionally(ioException);
    when(mockS3AsyncClient.getObject(
            any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);
    S3SdkObjectClient client = new S3SdkObjectClient(mockS3AsyncClient);
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().blockReadTimeout(200).build();

    MetadataStore metadataStore =
        new MetadataStore(client, TestTelemetry.DEFAULT, configuration, mock(Metrics.class));
    ObjectMetadata objectMetadata = ObjectMetadata.builder().contentLength(100).etag(etag).build();
    metadataStore.storeObjectMetadata(s3URI, objectMetadata);
    BlobStore blobStore =
        new BlobStore(
            client, TestTelemetry.DEFAULT, configuration, mock(Metrics.class), executorService);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    assertThrows(IOException.class, () -> physicalIOImplV2.read(0));
    assertEquals(0, blobStore.blobCount());
    assertThrows(Exception.class, () -> metadataStore.get(s3URI, OpenStreamInformation.DEFAULT));
  }

  // @Test
  void testClose_WithoutEviction() throws IOException {
    // Given
    final String TEST_DATA = "test";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    Metrics metrics = new Metrics();
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            metrics,
            executorService);
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    // When: Read data to ensure blob is created
    byte[] buffer = new byte[4];
    physicalIO.read(buffer, 0, 4, 0);

    // Then: Memory usage should equal bytes read
    assertEquals(
        TEST_DATA.length(),
        metrics.get(MetricKey.MEMORY_USAGE),
        "Memory usage should equal bytes read");

    // When: Close without eviction
    physicalIO.close();

    // Then: Memory usage should remain unchanged
    assertEquals(
        TEST_DATA.length(),
        metrics.get(MetricKey.MEMORY_USAGE),
        "Memory usage should remain unchanged after close without eviction");
  }

  @Test
  void testCloseWithEviction() throws IOException {
    // Given
    final String TEST_DATA = "test data longer than buffer";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);

    BlobStore mockBlobStore = mock(BlobStore.class);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));

    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            mockBlobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);
    ObjectKey objectKey = ObjectKey.builder().s3URI(s3URI).etag(fakeObjectClient.getEtag()).build();
    // When
    physicalIO.close(true);
    // Then
    verify(mockBlobStore, times(1)).evictKey(objectKey);
  }

  @Test
  void testPartialRead() throws IOException {
    // Given
    final String TEST_DATA = "test data longer than buffer";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    Metrics metrics = new Metrics();
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            metrics,
            executorService);
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    // When: Read partial data
    byte[] buffer = new byte[4];
    int bytesRead = physicalIO.read(buffer, 0, 4, 0);

    // Then: Memory usage should equal total data length, not just bytes read
    assertEquals(
        TEST_DATA.length(),
        metrics.get(MetricKey.MEMORY_USAGE),
        "Memory usage should equal total data length");
    assertEquals(4, bytesRead, "Should have read requested number of bytes");
  }

  @Test
  void testReadVectored() throws IOException {
    // Run for both direct and non-direct buffers.
    readVectored(ByteBuffer::allocate);
    readVectored(ByteBuffer::allocateDirect);
  }

  private void readVectored(IntFunction<ByteBuffer> allocate) throws IOException {
    final String TEST_DATA = "test data for read vectored";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    Metrics metrics = new Metrics();
    BlobStore blobStore =
        new BlobStore(
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            metrics,
            executorService);
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            s3URI,
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            OpenStreamInformation.DEFAULT,
            executorService,
            PhysicalIOConfiguration.DEFAULT);

    List<ObjectRange> objectRanges = new ArrayList<>();
    objectRanges.add(new ObjectRange(new CompletableFuture<>(), 2, 3));
    objectRanges.add(new ObjectRange(new CompletableFuture<>(), 8, 1));
    objectRanges.add(new ObjectRange(new CompletableFuture<>(), 12, 6));

    // first buffer to contain "st "
    byte[] firstBufferExpected = new byte[] {115, 116, 32};
    // second buffer to contain "a"
    byte[] secondBufferExpected = new byte[] {97};
    // third buffer to contain "r read"
    byte[] thirdBufferExpected = new byte[] {114, 32, 114, 101, 97, 100};

    physicalIO.readVectored(objectRanges, allocate, (buffer) -> {});

    verifyBufferContentsEqual(objectRanges.get(0).getByteBuffer().join(), firstBufferExpected);
    verifyBufferContentsEqual(objectRanges.get(1).getByteBuffer().join(), secondBufferExpected);
    verifyBufferContentsEqual(objectRanges.get(2).getByteBuffer().join(), thirdBufferExpected);
  }

  /**
   * Verify the contents of two buffers are equal
   *
   * @param buffer ByteBuffer to verify contents for
   * @param expected expected contents in byte buffer
   */
  private void verifyBufferContentsEqual(ByteBuffer buffer, byte[] expected) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(buffer.get(i), expected[i]);
    }
  }
}
