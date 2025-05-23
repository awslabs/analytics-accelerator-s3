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
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
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
  @DisplayName("Test complete constructor initialization")
  void testConstructorInitialization() {
    // Arrange
    ObjectKey objectKey = mock(ObjectKey.class);
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata =
        ObjectMetadata.builder().contentLength(1000L).etag("test-etag").build();
    Telemetry telemetry = mock(Telemetry.class);
    PhysicalIOConfiguration configuration = mock(PhysicalIOConfiguration.class);
    Metrics aggregatingMetrics = new Metrics();
    BlobStoreIndexCache indexCache = mock(BlobStoreIndexCache.class);
    StreamContext streamContext = mock(StreamContext.class);

    // Act
    BlockManager blockManager =
        new BlockManager(
            objectKey,
            objectClient,
            metadata,
            telemetry,
            configuration,
            aggregatingMetrics,
            indexCache,
            streamContext);

    // Assert
    assertNotNull(blockManager, "BlockManager should not be null");
  }

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
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                null,
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                null,
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                null,
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class)));
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
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build();
    BlockManager blockManager = getTestBlockManager(mock(ObjectClient.class), 65 * ONE_KB, config);

    // When: have a 64KB block available from 0
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertTrue(blockManager.getBlock(0).isPresent());
    assertFalse(blockManager.getBlock(64 * ONE_KB).isPresent());
  }

  @Test
  void testMakePositionAvailableRespectsReadAhead() throws IOException {
    // Given

    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build();

    final int objectSize = (int) config.getReadAheadBytes() + ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize, config);

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
        IOException.class,
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
        mock(Metrics.class),
        mock(BlobStoreIndexCache.class));
  }

  @Test
  @DisplayName("Test isBlockStoreEmpty method")
  void testIsBlockStoreEmpty() throws IOException {
    // Given
    BlockManager blockManager = getTestBlockManager(42);

    // When & Then
    assertTrue(blockManager.isBlockStoreEmpty(), "BlockStore should be empty initially");

    // After adding a block
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    assertFalse(
        blockManager.isBlockStoreEmpty(), "BlockStore should not be empty after adding block");
  }

  @Test
  @DisplayName("Test makePositionAvailable with negative position")
  void testMakePositionAvailableNegative() throws IOException {
    BlockManager blockManager = getTestBlockManager(42);

    assertThrows(
        IllegalArgumentException.class,
        () -> blockManager.makePositionAvailable(-1, ReadMode.SYNC));
  }

  @Test
  @DisplayName("Test makeRangeAvailable with negative values")
  void testMakeRangeAvailableNegative() throws IOException {
    BlockManager blockManager = getTestBlockManager(42);

    assertThrows(
        IllegalArgumentException.class,
        () -> blockManager.makeRangeAvailable(-1, 10, ReadMode.SYNC));

    assertThrows(
        IllegalArgumentException.class,
        () -> blockManager.makeRangeAvailable(0, -1, ReadMode.SYNC));
  }

  @Test
  @DisplayName("Test sequential read pattern detection")
  void testSequentialReadPattern() throws IOException {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[1024])).build()));

    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().readAheadBytes(512).sequentialPrefetchBase(2.0).build();

    BlockManager blockManager = getTestBlockManager(objectClient, 1024, config);

    // When: making sequential reads
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(100, ReadMode.SYNC);
    blockManager.makePositionAvailable(200, ReadMode.SYNC);

    // Then: verify pattern detection through increased read ahead
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, atLeast(1)).getObject(requestCaptor.capture(), any());

    // Verify that later requests have larger ranges due to sequential pattern detection
    List<GetRequest> requests = requestCaptor.getAllValues();
    long firstRequestSize = requests.get(0).getRange().getLength();
    long lastRequestSize = requests.get(requests.size() - 1).getRange().getLength();
    assertTrue(
        lastRequestSize >= firstRequestSize,
        "Read ahead size should increase for sequential pattern");
  }

  @Test
  @DisplayName("Test cleanup method")
  void testCleanup() throws IOException {
    // Given
    BlockManager blockManager = getTestBlockManager(1024);

    // Add some blocks
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(100, ReadMode.SYNC);

    // When
    blockManager.cleanUp();

    // Then
    assertTrue(blockManager.isBlockStoreEmpty(), "BlockStore should be empty after cleanup");
  }

  @Test
  void testClose() throws IOException, InterruptedException {
    // Given
    BlockManager blockManager = getTestBlockManager(1024);
    CountDownLatch closeLatch = new CountDownLatch(1);

    // Add some blocks
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // When
    Thread closeThread =
        new Thread(
            () -> {
              blockManager.close();
              closeLatch.countDown();
            });
    closeThread.start();

    // Then
    assertTrue(
        closeLatch.await(5, TimeUnit.SECONDS), "Close operation should complete within timeout");
  }

  @Test
  @DisplayName("Test makeRangeAvailable with async read mode")
  void testMakeRangeAvailableAsync() throws IOException {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 1024);

    // When
    blockManager.makeRangeAvailable(0, 100, ReadMode.ASYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture(), any());

    // Verify that async mode doesn't trigger read ahead
    assertEquals(1024, requestCaptor.getValue().getRange().getLength());
  }

  @Test
  @DisplayName("Test truncatePos method")
  void testTruncatePos() throws IOException {
    // Given
    int size = 1024;
    BlockManager blockManager = getTestBlockManager(size);

    // Test position within bounds
    blockManager.makeRangeAvailable(size - 100, 50, ReadMode.SYNC);
    assertTrue(blockManager.getBlock(size - 75).isPresent());

    // Test position at boundary
    blockManager.makeRangeAvailable(size - 1, 1, ReadMode.SYNC);
    assertTrue(blockManager.getBlock(size - 1).isPresent());
  }

  @Test
  @DisplayName("Test concurrent access to makeRangeAvailable")
  void testConcurrentMakeRangeAvailable() throws Exception {
    // Given
    BlockManager blockManager = getTestBlockManager(1024);
    int numThreads = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    try {
      // When
      for (int i = 0; i < numThreads; i++) {
        final int pos = i * 100;
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    blockManager.makeRangeAvailable(pos, 50, ReadMode.SYNC);
                  } catch (Exception e) {
                    fail("Unexpected exception: " + e);
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      startLatch.countDown();
      assertTrue(
          completionLatch.await(5, TimeUnit.SECONDS), "Not all threads completed within timeout");

      // Check futures for exceptions
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
      }

      // Then
      for (int i = 0; i < numThreads; i++) {
        assertTrue(
            blockManager.getBlock(i * 100L).isPresent(),
            "Block should be present for position " + (i * 100L));
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void testSmallObjectPrefetching() throws IOException {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    int smallObjectSize = 2 * ONE_MB; // Size less than default threshold (3MB)

    // When
    PhysicalIOConfiguration config = PhysicalIOConfiguration.builder().build();

    BlockManager blockManager = getTestBlockManager(objectClient, smallObjectSize, config);

    // Trigger prefetching
    blockManager.makeRangeAvailable(0, smallObjectSize, ReadMode.SMALL_OBJECT_PREFETCH);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture(), any());

    GetRequest request = requestCaptor.getValue();
    assertEquals(0, request.getRange().getStart());
    assertEquals(smallObjectSize - 1, request.getRange().getEnd());
  }

  @Test
  void testSmallObjectPrefetchingDisabled() throws IOException {
    // Given
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    int smallObjectSize = 2 * ONE_MB;

    // When
    BlockManager blockManager = getTestBlockManager(objectClient, smallObjectSize, config);

    // Then
    verify(objectClient, times(0)).getObject(any(), any());
    assertFalse(blockManager.getBlock(0).isPresent());
  }
}
