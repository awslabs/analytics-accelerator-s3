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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.InputPolicy;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenFileInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockManagerTest {
  private static final String ETAG = "RandomString";
  private ObjectMetadata metadataStore;
  static S3URI testUri = S3URI.of("foo", "bar");
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(testUri).etag(ETAG).build();

  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                null,
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                OpenFileInformation.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                null,
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                OpenFileInformation.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                OpenFileInformation.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                null,
                mock(PhysicalIOConfiguration.class),
                OpenFileInformation.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                null,
                OpenFileInformation.DEFAULT));
  }

  @Test
  void testGetBlockIsEmpty() throws IOException {
    // Given
    BlockManager blockManager = getTestBlockManager(42);

    // When: nothing

    // Then
    assertFalse(blockManager.getBlock(0).isPresent());
  }

  @Test
  void testGetBlockReturnsAvailableBlock() throws IOException {
    // Given
    BlockManager blockManager = getTestBlockManager(65 * ONE_KB);

    // When: have a 64KB block available from 0
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertTrue(blockManager.getBlock(0).isPresent());
    assertFalse(blockManager.getBlock(64 * ONE_KB).isPresent());
  }

  @Test
  void testMakePositionAvailableRespectsReadAhead() throws IOException {
    // Given
    final int objectSize = (int) PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() + ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture(), any());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() - 1,
        requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  void testMakePositionAvailableRespectsLastObjectByte() throws IOException {
    // Given
    final int objectSize = 5 * ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture(), any());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(objectSize - 1, requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  void testMakeRangeAvailableDoesNotOverread() throws IOException {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 128 * ONE_KB);
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(64 * ONE_KB + 1, ReadMode.SYNC);

    // When: requesting the byte at 64KB
    blockManager.makeRangeAvailable(64 * ONE_KB, 100, ReadMode.SYNC);
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, times(3)).getObject(requestCaptor.capture(), any());

    // Then: request size is a single byte as more is not needed
    GetRequest firstRequest = requestCaptor.getAllValues().get(0);
    GetRequest secondRequest = requestCaptor.getAllValues().get(1);
    GetRequest lastRequest = requestCaptor.getAllValues().get(2);

    assertEquals(65_536, firstRequest.getRange().getLength());
    assertEquals(65_535, secondRequest.getRange().getLength());
    assertEquals(1, lastRequest.getRange().getLength());
  }

  @Test
  void testMakeRangeAvailableThrowsExceptionWhenEtagChanges() throws IOException {
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 128 * ONE_MB);
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    int readAheadBytes = (int) PhysicalIOConfiguration.DEFAULT.getReadAheadBytes();

    // Overwrite our client to now throw an error with our old etag. This simulates the scenario
    // where the etag changes during a read.
    when(objectClient.getObject(
            argThat(
                request -> {
                  if (request == null) {
                    return false;
                  }
                  // Check if the If-Match header matches expected ETag
                  return request.getEtag() != null && request.getEtag().equals(ETAG);
                }),
            any()))
        .thenThrow(S3Exception.builder().message("PreconditionFailed").statusCode(412).build());

    assertThrows(
        S3Exception.class,
        () -> blockManager.makePositionAvailable(readAheadBytes + 1, ReadMode.SYNC));
  }

  @Test
  void regressionTestSequentialPrefetchShouldNotShrinkRanges() throws IOException {
    // Given: BlockManager with some blocks loaded
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager =
        getTestBlockManager(
            objectClient,
            128 * ONE_MB,
            PhysicalIOConfiguration.builder().sequentialPrefetchBase(2.0).build());
    blockManager.makeRangeAvailable(20_837_974, 8_323_072, ReadMode.SYNC);
    blockManager.makeRangeAvailable(20_772_438, 65_536, ReadMode.SYNC);
    blockManager.makeRangeAvailable(29_161_046, 4_194_305, ReadMode.SYNC);
    blockManager.makeRangeAvailable(106_182_410, 1_048_576, ReadMode.SYNC);

    // When: [29161046 - 37549653] is requested
    blockManager.makeRangeAvailable(29_161_046, 8_388_608, ReadMode.SYNC);

    // Then: position 33_355_351 should be available
    // This was throwing before, and it shouldn't, given that 33_355_351 is contained in [29161046 -
    // 37549653].
    // The positions here are from a real-life workload scenario.
    assertDoesNotThrow(
        () ->
            blockManager
                .getBlock(33_355_351)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "block should have been available because it was requested before")));
  }

  private BlockManager getTestBlockManager(int size) throws IOException {
    return getTestBlockManager(mock(ObjectClient.class), size);
  }

  private BlockManager getTestBlockManager(ObjectClient objectClient, int size) throws IOException {
    return getTestBlockManager(objectClient, size, PhysicalIOConfiguration.DEFAULT);
  }

  private BlockManager getTestBlockManager(
      ObjectClient objectClient, int size, PhysicalIOConfiguration configuration) {
    /*
     The argument matcher is used to check if our arguments match the values we want to mock a return for
     (https://www.baeldung.com/mockito-argument-matchers)
     If the header doesn't exist or if the header matches we want to return our positive response.
    */
    when(objectClient.getObject(
            argThat(
                request -> {
                  if (request == null) {
                    return false;
                  }
                  // Check if the If-Match header matches expected ETag
                  return request.getEtag() == null || request.getEtag().equals(ETAG);
                }),
            any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[size])).build()));

    /*
     Here we check if our header is present and the etags don't match then we expect an error to be thrown.
    */
    when(objectClient.getObject(
            argThat(
                request -> {
                  if (request == null) {
                    return false;
                  }
                  // Check if the If-Match header matches expected ETag
                  return request.getEtag() != null && !request.getEtag().equals(ETAG);
                }),
            any()))
        .thenThrow(S3Exception.builder().message("PreconditionFailed").statusCode(412).build());

    metadataStore = ObjectMetadata.builder().contentLength(size).etag(ETAG).build();

    return new BlockManager(
        objectKey,
        objectClient,
        metadataStore,
        TestTelemetry.DEFAULT,
        configuration,
        OpenFileInformation.DEFAULT);
  }

  @Test
  void testNonParquetBehavior() throws IOException {
    // Given: Test setup for CSV file
    S3URI csvUri = S3URI.of("bucket", "test.csv"); // Non-parquet extension
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata =
        ObjectMetadata.builder().contentLength(16L * ONE_MB).etag(ETAG).build();

    PhysicalIOConfiguration testConfig =
        PhysicalIOConfiguration.builder()
            .readAheadBytes(ONE_KB)
            .sequentialPrefetchBase(2.0)
            .build();

    // For CSV file
    OpenFileInformation csvInfo = OpenFileInformation.builder().objectMetadata(metadata).build();

    // Create BlockManager for CSV
    BlockManager csvBlockManager =
        new BlockManager(
            ObjectKey.builder().s3URI(csvUri).etag(ETAG).build(),
            objectClient,
            metadata,
            TestTelemetry.DEFAULT,
            testConfig,
            csvInfo);

    // Setup mock response
    when(objectClient.getObject(any(GetRequest.class), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[1024])).build()));

    // Make sequential requests
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    csvBlockManager.makeRangeAvailable(0L, 100L, ReadMode.SYNC);
    csvBlockManager.makeRangeAvailable(512L, 100L, ReadMode.SYNC);

    verify(objectClient, atLeast(1)).getObject(requestCaptor.capture(), any());

    // Verify all requests are limited to readAheadBytes
    requestCaptor
        .getAllValues()
        .forEach(
            request ->
                assertTrue(
                    request.getRange().getLength() <= testConfig.getReadAheadBytes(),
                    "Non-Parquet requests should not exceed readAheadBytes"));
  }

  @Test
  void testParquetBehavior() throws IOException {
    // Given: Test setup for Parquet file
    S3URI parquetUri = S3URI.of("bucket", "test.parquet"); // Parquet extension
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata =
        ObjectMetadata.builder().contentLength(16L * ONE_MB).etag(ETAG).build();

    PhysicalIOConfiguration testConfig =
        PhysicalIOConfiguration.builder()
            .readAheadBytes(ONE_KB)
            .sequentialPrefetchBase(2.0)
            .build();

    // For Parquet file - set Sequential policy
    OpenFileInformation parquetInfo =
        OpenFileInformation.builder()
            .objectMetadata(metadata)
            .inputPolicy(InputPolicy.Sequential) // This is the key change
            .build();

    // Create BlockManager for Parquet
    BlockManager parquetBlockManager =
        new BlockManager(
            ObjectKey.builder().s3URI(parquetUri).etag(ETAG).build(),
            objectClient,
            metadata,
            TestTelemetry.DEFAULT,
            testConfig,
            parquetInfo);

    // Setup mock response
    when(objectClient.getObject(any(GetRequest.class), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[1024])).build()));

    // Make sequential requests
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    parquetBlockManager.makeRangeAvailable(0L, 100L, ReadMode.SYNC);
    parquetBlockManager.makeRangeAvailable(512L, 100L, ReadMode.SYNC);

    verify(objectClient, atLeast(1)).getObject(requestCaptor.capture(), any());

    // Verify requests use sequential prefetching
    requestCaptor
        .getAllValues()
        .forEach(
            request ->
                assertTrue(
                    request.getRange().getLength() <= testConfig.getReadAheadBytes(),
                    "Parquet requests should show sequential prefetching"));
  }
}
