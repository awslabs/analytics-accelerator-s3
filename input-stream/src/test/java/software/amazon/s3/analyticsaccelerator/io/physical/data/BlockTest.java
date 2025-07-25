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
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RandomString";
  private static final String TEST_DATA = "test-data";
  private static final byte[] TEST_DATA_BYTES = TEST_DATA.getBytes(StandardCharsets.UTF_8);
  private static final long READ_TIMEOUT = 5_000;
  private static final int RETRY_COUNT = 2;

  private ObjectKey objectKey;
  private BlockKey blockKey;
  private Metrics mockMetrics;
  private BlobStoreIndexCache mockIndexCache;

  @BeforeEach
  void setUp() {
    objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
    blockKey = new BlockKey(objectKey, new Range(0, TEST_DATA.length()));
    mockMetrics = mock(Metrics.class);
    mockIndexCache = mock(BlobStoreIndexCache.class);
  }

  @Test
  void testConstructorWithValidParameters() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    assertNotNull(block);
    assertEquals(blockKey, block.getBlockKey());
    assertEquals(0, block.getGeneration());
    assertFalse(block.isDataReady());
  }

  @Test
  void testConstructorWithNullBlockKey() {
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                null,
                0,
                mockIndexCache,
                mockMetrics,
                READ_TIMEOUT,
                RETRY_COUNT,
                OpenStreamInformation.DEFAULT));
  }

  @Test
  void testConstructorWithNullIndexCache() {
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                blockKey,
                0,
                null,
                mockMetrics,
                READ_TIMEOUT,
                RETRY_COUNT,
                OpenStreamInformation.DEFAULT));
  }

  @Test
  void testConstructorWithNullMetrics() {
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                blockKey,
                0,
                mockIndexCache,
                null,
                READ_TIMEOUT,
                RETRY_COUNT,
                OpenStreamInformation.DEFAULT));
  }

  @Test
  void testConstructorWithNegativeGeneration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                blockKey,
                -1,
                mockIndexCache,
                mockMetrics,
                READ_TIMEOUT,
                RETRY_COUNT,
                OpenStreamInformation.DEFAULT));
  }

  @Test
  void testConstructorWithNegativeRangeStart() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          BlockKey invalidBlockKey = new BlockKey(objectKey, new Range(-1, TEST_DATA.length()));
          new Block(
              invalidBlockKey,
              0,
              mockIndexCache,
              mockMetrics,
              READ_TIMEOUT,
              RETRY_COUNT,
              OpenStreamInformation.DEFAULT);
        });
  }

  @Test
  void testConstructorWithNegativeRangeEnd() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          BlockKey blockKey = new BlockKey(objectKey, new Range(0, -1));
          new Block(
              blockKey,
              0,
              mockIndexCache,
              mockMetrics,
              READ_TIMEOUT,
              RETRY_COUNT,
              OpenStreamInformation.DEFAULT);
        });
  }

  @Test
  void testSetDataAndIsDataReady() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    assertFalse(block.isDataReady());

    block.setData(TEST_DATA_BYTES);

    assertTrue(block.isDataReady());
    verify(mockMetrics).add(any(), eq((long) TEST_DATA_BYTES.length));
    verify(mockIndexCache).put(blockKey, blockKey.getRange().getLength());
  }

  @Test
  void testReadSingleByteAfterDataSet() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    int result = block.read(0);

    assertEquals(Byte.toUnsignedInt(TEST_DATA_BYTES[0]), result);
    verify(mockIndexCache).recordAccess(blockKey);
  }

  @Test
  void testReadSingleByteAtDifferentPositions() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    assertEquals(116, block.read(0)); // 't'
    assertEquals(101, block.read(1)); // 'e'
    assertEquals(115, block.read(2)); // 's'
    assertEquals(116, block.read(3)); // 't'
    assertEquals(45, block.read(4)); // '-'
    assertEquals(100, block.read(5)); // 'd'
    assertEquals(97, block.read(6)); // 'a'
    assertEquals(116, block.read(7)); // 't'
    assertEquals(97, block.read(8)); // 'a'
  }

  @Test
  void testReadSingleByteWithNegativePosition() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    assertThrows(IllegalArgumentException.class, () -> block.read(-1));
  }

  @Test
  void testReadBufferAfterDataSet() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    byte[] buffer = new byte[4];
    int bytesRead = block.read(buffer, 0, 4, 0);

    assertEquals(4, bytesRead);
    assertEquals("test", new String(buffer, StandardCharsets.UTF_8));
    verify(mockIndexCache).recordAccess(blockKey);
  }

  @Test
  void testReadBufferAtDifferentPositions() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    byte[] buffer1 = new byte[4];
    int bytesRead1 = block.read(buffer1, 0, 4, 0);
    assertEquals(4, bytesRead1);
    assertEquals("test", new String(buffer1, StandardCharsets.UTF_8));

    byte[] buffer2 = new byte[4];
    int bytesRead2 = block.read(buffer2, 0, 4, 5);
    assertEquals(4, bytesRead2);
    assertEquals("data", new String(buffer2, StandardCharsets.UTF_8));
  }

  @Test
  void testReadBufferPartialRead() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    byte[] buffer = new byte[10];
    int bytesRead = block.read(buffer, 0, 10, 7);

    assertEquals(2, bytesRead); // Only 2 bytes available from position 7
    assertEquals("ta", new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
  }

  @Test
  void testReadBufferWithInvalidParameters() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    byte[] buffer = new byte[4];

    assertThrows(IllegalArgumentException.class, () -> block.read(buffer, -1, 4, 0));
    assertThrows(IllegalArgumentException.class, () -> block.read(buffer, 0, -1, 0));
    assertThrows(IllegalArgumentException.class, () -> block.read(buffer, 0, 4, -1));
    assertThrows(IllegalArgumentException.class, () -> block.read(buffer, 4, 1, 0));
  }

  @Test
  void testReadBeforeDataSet() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            100,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT); // Short timeout

    assertThrows(IOException.class, () -> block.read(0));
  }

  @Test
  void testReadBufferBeforeDataSet() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            100,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT); // Short timeout
    byte[] buffer = new byte[4];

    assertThrows(IOException.class, () -> block.read(buffer, 0, 4, 0));
  }

  @Test
  void testReadWithTimeout() throws InterruptedException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            100,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT); // Short timeout

    CountDownLatch latch = new CountDownLatch(1);
    CompletableFuture<Void> readTask =
        CompletableFuture.runAsync(
            () -> {
              try {
                latch.countDown();
                block.read(0);
                fail("Expected IOException due to timeout");
              } catch (IOException e) {
                // Expected
              }
            });

    latch.await(); // Wait for read to start
    Thread.sleep(200); // Wait longer than timeout

    assertDoesNotThrow(() -> readTask.get(1, TimeUnit.SECONDS));
  }

  @Test
  void testConcurrentReadsAfterDataSet() throws InterruptedException, ExecutionException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<CompletableFuture<Integer>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final int pos = i % TEST_DATA_BYTES.length;
      CompletableFuture<Integer> future =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return block.read(pos);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              executor);
      futures.add(future);
    }

    for (int i = 0; i < numThreads; i++) {
      int expectedByte = Byte.toUnsignedInt(TEST_DATA_BYTES[i % TEST_DATA_BYTES.length]);
      assertEquals(expectedByte, futures.get(i).get().intValue());
    }

    executor.shutdown();
  }

  @Test
  void testCloseReleasesData() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    assertTrue(block.isDataReady());

    block.close();

    // After close, reading should fail
    assertThrows(IOException.class, () -> block.read(0));
  }

  @Test
  void testMultipleSetDataCalls() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    block.setData(TEST_DATA_BYTES);
    assertTrue(block.isDataReady());

    // Second call should not affect the state
    byte[] newData = "new-data".getBytes(StandardCharsets.UTF_8);
    block.setData(newData);
    assertTrue(block.isDataReady());
  }

  @Test
  void testGenerationProperty() {
    Block block1 =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    Block block2 =
        new Block(
            blockKey,
            5,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    Block block3 =
        new Block(
            blockKey,
            100,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    assertEquals(0, block1.getGeneration());
    assertEquals(5, block2.getGeneration());
    assertEquals(100, block3.getGeneration());
  }

  @Test
  void testReadIntoBuffer() throws IOException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            READ_TIMEOUT,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);
    block.setData(TEST_DATA_BYTES);

    byte[] buffer = new byte[20];
    int bytesRead = block.read(buffer, 2, 5, 0);

    assertEquals(5, bytesRead);
    assertEquals(TEST_DATA.substring(0, 5), new String(buffer, 2, 5, StandardCharsets.UTF_8));
  }

  @Test
  void testReadTimeoutIfDataNeverSet() {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            100,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT); // 100 ms

    IOException ex = assertThrows(IOException.class, () -> block.read(0));
    assertTrue(ex.getMessage().contains("Error while reading data."));
  }

  @Test
  void testReadBlocksUntilDataIsReady() throws Exception {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            1000,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Integer> result = executor.submit(() -> block.read(0));

    // simulate delay
    Thread.sleep(100);
    block.setData(TEST_DATA_BYTES);

    assertEquals(Byte.toUnsignedInt(TEST_DATA_BYTES[0]), result.get(1, TimeUnit.SECONDS));
    executor.shutdown();
  }

  @Test
  void testReadHandlesInterruptedException() throws InterruptedException {
    Block block =
        new Block(
            blockKey,
            0,
            mockIndexCache,
            mockMetrics,
            500,
            RETRY_COUNT,
            OpenStreamInformation.DEFAULT);

    Thread testThread =
        new Thread(
            () -> {
              try {
                block.read(0); // this internally calls awaitData()
                fail("Expected IOException due to interruption");
              } catch (IOException e) {
                assertTrue(e.getMessage().contains("interrupted"));
                assertTrue(Thread.currentThread().isInterrupted());
              }
            });

    testThread.start();
    Thread.sleep(100); // Ensure thread is waiting inside awaitData()
    testThread.interrupt();
    testThread.join();
  }

  @Test
  void testRetryStrategyWithTimeout() throws Exception {
    // Create a counter to track retry attempts
    AtomicInteger attempts = new AtomicInteger(0);

    // Create a custom CountDownLatch to coordinate the test
    CountDownLatch attemptLatch = new CountDownLatch(1);
    CountDownLatch dataSetLatch = new CountDownLatch(1);

    // Create a block with a short timeout and 2 retries
    Block block =
        new Block(blockKey, 0, mockIndexCache, mockMetrics, 50, 2, OpenStreamInformation.DEFAULT);

    // Start a thread that will try to read from the block
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                // Record the attempt and notify the test thread
                attempts.incrementAndGet();
                attemptLatch.countDown();

                // Try to read - this will trigger retries internally
                return block.read(0);
              } catch (IOException e) {
                throw new CompletionException(e);
              }
            });

    // Wait for the read attempt to start
    assertTrue(attemptLatch.await(500, TimeUnit.MILLISECONDS));

    // Wait a bit to ensure at least one retry happens (50ms timeout * 1 retry)
    Thread.sleep(75);

    // Now set the data so the next retry will succeed
    block.setData(TEST_DATA_BYTES);
    dataSetLatch.countDown();

    // The read should eventually succeed
    assertEquals(Byte.toUnsignedInt(TEST_DATA_BYTES[0]), future.join());
  }

  @Test
  void testRetryStrategyExhaustsRetries() {
    // Create a block with a short timeout and only 1 retry
    Block block =
        new Block(blockKey, 0, mockIndexCache, mockMetrics, 100, 1, OpenStreamInformation.DEFAULT);

    // Don't set data, so all retries will fail with timeout

    // This should fail after max retries
    IOException exception = assertThrows(IOException.class, () -> block.read(0));
    assertTrue(exception.getMessage().contains("timed out"));
  }

  @Test
  void testRetryStrategyWithMultipleRetries() throws InterruptedException {
    // Create a block with a short timeout and multiple retries
    CountDownLatch readStarted = new CountDownLatch(1);

    Block block =
        new Block(blockKey, 0, mockIndexCache, mockMetrics, 100, 3, OpenStreamInformation.DEFAULT);

    // Start a thread that will try to read from the block
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                readStarted.countDown();
                return block.read(0);
              } catch (IOException e) {
                throw new CompletionException(e);
              }
            });

    // Wait for read to start
    assertTrue(readStarted.await(500, TimeUnit.MILLISECONDS));

    // Wait a bit to ensure at least one retry happens
    Thread.sleep(250);

    // Now set the data
    block.setData(TEST_DATA_BYTES);

    // The read should eventually succeed
    assertEquals(Byte.toUnsignedInt(TEST_DATA_BYTES[0]), future.join());
  }
}
