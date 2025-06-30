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
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockManagerTest {
  private static final String ETAG = "RandomString";
  private ObjectMetadata metadataStore;
  static S3URI testUri = S3URI.of("foo", "bar");
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(testUri).etag(ETAG).build();
  private final ExecutorService threadPool = Executors.newFixedThreadPool(30);

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
    OpenStreamInformation openStreamInformation = mock(OpenStreamInformation.class);
    ExecutorService executorService = mock(ExecutorService.class);

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
            openStreamInformation,
            executorService);

    // Assert
    assertNotNull(blockManager, "BlockManager should not be null");
  }

  @Test
  void testCreateBoundaries() {
    // Test when objectKey is null
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
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                mock(ExecutorService.class)));

    // Test when objectClient is null
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
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when metadata is null
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
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when telemetry is null
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
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when configuration is null
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
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when metrics is null
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                null,
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when indexCache is null
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                null,
                OpenStreamInformation.DEFAULT,
                threadPool));

    // Test when openStreamInformation is null
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                null,
                threadPool));

    // Test when threadPool is null
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectKey.class),
                mock(ObjectClient.class),
                mock(ObjectMetadata.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(Metrics.class),
                mock(BlobStoreIndexCache.class),
                OpenStreamInformation.DEFAULT,
                null));
  }

  @Test
  void testGetBlockIsEmptyWhenNotSmallObject() {
    ObjectClient objectClient = mock(ObjectClient.class);
    int largeObjectSize = 9 * ONE_MB;
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder()
            .smallObjectSizeThreshold(8 * ONE_MB)
            .smallObjectsPrefetchingEnabled(true)
            .build();

    // Given
    BlockManager blockManager = getTestBlockManager(objectClient, largeObjectSize, configuration);

    // When: nothing

    // Then
    verifyNoInteractions(objectClient);
    assertFalse(blockManager.getBlock(0).isPresent());
  }

  @Test
  void testGetBlockIsNotEmptyWhenSmallObject() {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder()
            .smallObjectSizeThreshold(8 * ONE_MB)
            .smallObjectsPrefetchingEnabled(true)
            .build();
    BlockManager blockManager = getTestBlockManager(objectClient, 42, configuration);

    // When: nothing

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000)).getObject(requestCaptor.capture(), any());
    assertTrue(blockManager.getBlock(0).isPresent());
  }

  @Test
  void testSmallObjectPrefetchingDisabled() {
    // Given
    int smallObjectSize = 2 * ONE_MB;
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder()
            .smallObjectsPrefetchingEnabled(false)
            .smallObjectSizeThreshold(
                8 * ONE_MB) // Make sure that threshold is always higher than small object size
            .build();

    ObjectClient objectClient = mock(ObjectClient.class);

    // When
    BlockManager blockManager = getTestBlockManager(objectClient, smallObjectSize, config);

    // Then
    verifyNoInteractions(objectClient);
    assertFalse(blockManager.getBlock(0).isPresent());
  }

  @Test
  void testSmallObjectPrefetching() {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    int smallObjectSize = 2 * ONE_MB; // Size less than default threshold (3MB)

    // When
    BlockManager blockManager = getTestBlockManager(objectClient, smallObjectSize);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000)).getObject(requestCaptor.capture(), any());

    GetRequest request = requestCaptor.getValue();
    assertEquals(1, requestCaptor.getAllValues().size());
    assertEquals(0, request.getRange().getStart());
    assertEquals(smallObjectSize - 1, request.getRange().getEnd());
    assertRangeIsAvailable(blockManager, 0, smallObjectSize - 1);
  }

  @Test
  void testGetBlockReturnsAvailableBlock() {
    // Given
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build();
    BlockManager blockManager = getTestBlockManager(mock(ObjectClient.class), 65 * ONE_KB, config);

    // When: have a 64KB block available from 0
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertRangeIsAvailable(blockManager, 0, (64 * ONE_KB) - 1);
  }

  @Test
  void testMakePositionAvailableRespectsReadAhead() {
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
    verify(objectClient, timeout(1_000)).getObject(requestCaptor.capture(), any());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() - 1,
        requestCaptor.getValue().getRange().getEnd());
    assertRangeIsAvailable(
        blockManager, 0, PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() - 1);
  }

  @Test
  void testMakePositionAvailableRespectsLastObjectByte() {
    // Given
    final int objectSize = 5 * ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000)).getObject(requestCaptor.capture(), any());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(objectSize - 1, requestCaptor.getValue().getRange().getEnd());
    assertRangeIsAvailable(blockManager, 0, objectSize - 1);
  }

  @Test
  void testMakeRangeAvailableDoesNotOverreadWhenSmallObjectPrefetchingIsDisabled() {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager =
        getTestBlockManager(
            objectClient,
            136 * ONE_KB,
            PhysicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build());
    blockManager.makePositionAvailable(
        0, ReadMode.SYNC); // This code will create blocks [0,1,2,3,4,5,6,7]
    blockManager.makePositionAvailable(
        72 * ONE_KB + 1, ReadMode.SYNC); // This code will create blocks [9,10,11,12,13,14,15,16]

    // When: requesting the byte at 64KB
    blockManager.makeRangeAvailable(64 * ONE_KB, 100, ReadMode.SYNC); // This will create block [8]
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000).times(3)).getObject(requestCaptor.capture(), any());

    List<GetRequest> capturedRequests = requestCaptor.getAllValues();

    // Convert expected ranges to a Set
    Set<Range> expectedRanges = new HashSet<>();
    expectedRanges.add(new Range(0, 65535));
    expectedRanges.add(new Range(65536, 73727));
    expectedRanges.add(new Range(73728, 139263));

    // Convert actual requests to ranges
    Set<Range> actualRanges = new HashSet<>();
    for (GetRequest req : capturedRequests) {
      actualRanges.add(new Range(req.getRange().getStart(), req.getRange().getEnd()));
    }

    assertEquals(expectedRanges, actualRanges);
  }

  @Test
  void testMakeRangeAvailableDoesNotOverreadWhenSmallObjectPrefetchingIsEnabled() {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 136 * ONE_KB);
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(72 * ONE_KB + 1, ReadMode.SYNC);

    // When: requesting the byte at 64KB
    blockManager.makeRangeAvailable(64 * ONE_KB, 100, ReadMode.SYNC);
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000)).getObject(requestCaptor.capture(), any());

    // Then: request size is a single byte as more is not needed
    GetRequest firstRequest = requestCaptor.getAllValues().get(0);

    assertEquals(139264, firstRequest.getRange().getLength());
  }

  @Test
  void testMakeRangeAvailableNotFillBlockWhenEtagChanges() {
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
                  return request.getEtag().equals(ETAG);
                }),
            any()))
        .thenThrow(S3Exception.builder().message("PreconditionFailed").statusCode(412).build());

    Optional<Block> blockOpt = blockManager.getBlock(readAheadBytes + 1);
    assertFalse(blockOpt.isPresent());
  }

  @Test
  void regressionTestSequentialPrefetchShouldNotShrinkRanges() {
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

  @Test
  @DisplayName("Test isBlockStoreEmpty method")
  void testIsBlockStoreEmpty() {
    // Given
    BlockManager blockManager = getTestBlockManager(42);
    // After adding a block
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    assertFalse(
        blockManager.isBlockStoreEmpty(), "BlockStore should not be empty after adding block");
  }

  @Test
  @DisplayName("Test makePositionAvailable with negative position")
  void testMakePositionAvailableNegative() {
    BlockManager blockManager = getTestBlockManager(42);

    assertThrows(
        IllegalArgumentException.class,
        () -> blockManager.makePositionAvailable(-1, ReadMode.SYNC));
  }

  @Test
  @DisplayName("Test makeRangeAvailable with negative values")
  void testMakeRangeAvailableNegative() {
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
  void testSequentialReadPattern() {
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
    verify(objectClient, timeout(1_00).atLeast(1)).getObject(requestCaptor.capture(), any());

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
  void testCleanup() throws InterruptedException {
    // Given
    BlockManager blockManager = getTestBlockManager(1024);

    // Add some blocks
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(100, ReadMode.SYNC);

    // Wait for some time till data is ready
    Thread.sleep(500);
    // When
    blockManager.cleanUp();

    // Then
    assertTrue(blockManager.isBlockStoreEmpty(), "BlockStore should be empty after cleanup");
  }

  @Test
  void testClose() throws InterruptedException {
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
  @DisplayName("Test makeRangeAvailable with sync read mode")
  void testMakeRangeAvailableSync() {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 100 * ONE_MB);

    // When
    blockManager.makeRangeAvailable(0, 5 * ONE_MB, ReadMode.SYNC);
    blockManager.makeRangeAvailable(5 * ONE_MB, 3 * ONE_MB, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000).times(2)).getObject(requestCaptor.capture(), any());

    List<GetRequest> getRequestList = requestCaptor.getAllValues();

    // Verify that with the SYNC mode, sequential prefetching kicks in
    assertEquals(getRequestList.get(0).getRange().getLength(), 5 * ONE_MB);
    // Second request gets extended by 4MB to 9MB.
    assertEquals(getRequestList.get(1).getRange().getLength(), 4 * ONE_MB);
  }

  @Test
  @DisplayName("Test truncatePos method")
  void testTruncatePos() {
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

  @ParameterizedTest
  @MethodSource("readModes")
  @DisplayName("Test makeRangeAvailable with async read modes")
  void testMakeRangeAvailableAsync(ReadMode readMode) {
    // Given
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 100 * ONE_MB);

    // When
    blockManager.makeRangeAvailable(0, 5 * ONE_MB, readMode);
    blockManager.makeRangeAvailable(5 * ONE_MB, 3 * ONE_MB, readMode);
    blockManager.makeRangeAvailable(8 * ONE_MB, 5 * ONE_MB, readMode);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, timeout(1_000).times(3)).getObject(requestCaptor.capture(), any());

    List<GetRequest> getRequestList = requestCaptor.getAllValues();

    // Verify that prefetch modes don't trigger sequential prefetching
    assertEquals(getRequestList.get(0).getRange().getLength(), 5 * ONE_MB);
    assertEquals(getRequestList.get(1).getRange().getLength(), 3 * ONE_MB);
    assertEquals(getRequestList.get(2).getRange().getLength(), 5 * ONE_MB);
  }

  private static List<ReadMode> readModes() {
    List<ReadMode> readModes = new ArrayList<>();
    readModes.add(ReadMode.READ_VECTORED);
    readModes.add(ReadMode.COLUMN_PREFETCH);
    readModes.add(ReadMode.DICTIONARY_PREFETCH);
    readModes.add(ReadMode.PREFETCH_TAIL);
    readModes.add(ReadMode.REMAINING_COLUMN_PREFETCH);
    return readModes;
  }

  private BlockManager getTestBlockManager(int size) {
    return getTestBlockManager(mock(ObjectClient.class), size);
  }

  private BlockManager getTestBlockManager(ObjectClient objectClient, int size) {
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
        mock(BlobStoreIndexCache.class),
        OpenStreamInformation.DEFAULT,
        threadPool);
  }

  private void assertRangeIsAvailable(BlockManager blockManager, long start, long end) {
    for (long pos = start; pos <= end; ) {
      Optional<Block> blockOpt = blockManager.getBlock(pos);
      assertTrue(blockOpt.isPresent(), "Block should be available at position " + pos);

      Block block = blockOpt.get();
      pos = block.getBlockKey().getRange().getEnd() + 1;
    }
  }
}
