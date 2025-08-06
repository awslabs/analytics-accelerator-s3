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
package software.amazon.s3.analyticsaccelerator.io.physical.reader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.TelemetryConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Block;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class StreamReaderTest {

  private ObjectClient mockObjectClient;
  private ObjectKey mockObjectKey;
  private ExecutorService mockExecutorService;
  private Consumer<List<Block>> mockRemoveBlocksFunc;
  private OpenStreamInformation openStreamInfo;
  private RequestCallback mockRequestCallback;
  private Metrics mockMetrics;
  private Telemetry telemetry;
  private PhysicalIOConfiguration configuration;

  private StreamReader streamReader;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    mockObjectClient = mock(ObjectClient.class);
    mockObjectKey = mock(ObjectKey.class);
    mockExecutorService = mock(ExecutorService.class);
    mockRemoveBlocksFunc = mock(Consumer.class);
    mockMetrics = mock(Metrics.class);
    mockRequestCallback = mock(RequestCallback.class);
    telemetry = Telemetry.createTelemetry(TelemetryConfiguration.DEFAULT);
    configuration = PhysicalIOConfiguration.DEFAULT;

    openStreamInfo = OpenStreamInformation.builder().requestCallback(mockRequestCallback).build();

    streamReader =
        new StreamReader(
            mockObjectClient,
            mockObjectKey,
            mockExecutorService,
            mockRemoveBlocksFunc,
            mockMetrics,
            openStreamInfo,
            telemetry,
            configuration);
  }

  @Test
  void test_initializeExceptions() {
    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                null,
                mockObjectKey,
                mockExecutorService,
                mockRemoveBlocksFunc,
                mockMetrics,
                openStreamInfo,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                null,
                mockExecutorService,
                mockRemoveBlocksFunc,
                mockMetrics,
                openStreamInfo,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                null,
                mockRemoveBlocksFunc,
                mockMetrics,
                openStreamInfo,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                mockExecutorService,
                null,
                mockMetrics,
                openStreamInfo,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                mockExecutorService,
                mockRemoveBlocksFunc,
                null,
                openStreamInfo,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                mockExecutorService,
                mockRemoveBlocksFunc,
                mockMetrics,
                null,
                telemetry,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                mockExecutorService,
                mockRemoveBlocksFunc,
                mockMetrics,
                openStreamInfo,
                null,
                configuration));

    assertThrows(
        NullPointerException.class,
        () ->
            new StreamReader(
                mockObjectClient,
                mockObjectKey,
                mockExecutorService,
                mockRemoveBlocksFunc,
                mockMetrics,
                openStreamInfo,
                telemetry,
                null));
  }

  @Test
  void read_throwsException_ifBlocksNull() {
    assertThrows(NullPointerException.class, () -> streamReader.read(null, ReadMode.SYNC));
  }

  @Test
  void read_throwsException_ifBlocksEmpty() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> streamReader.read(Collections.emptyList(), ReadMode.SYNC));
    assertTrue(thrown.getMessage().contains("must not be empty"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void read_submitsTaskToExecutor() {
    Block block = createMockBlock(0, 9);
    List<Block> blocks = Collections.singletonList(block);

    when(mockExecutorService.submit(any(Runnable.class))).thenReturn(mock(Future.class));

    streamReader.read(blocks, ReadMode.SYNC);

    verify(mockExecutorService, times(1)).submit(any(Runnable.class));
  }

  @Test
  void processReadTask_successfulRead_populatesBlocks() throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc, never()).accept(any());
    verify(mockObjectClient).getObject(any(GetRequest.class), eq(openStreamInfo));
    verifyNoMoreInteractions(mockObjectClient);
    verify(mockMetrics).add(MetricKey.GET_REQUEST_COUNT, 1);
    verifyNoMoreInteractions(mockMetrics);
    verify(block).setData(testData);
    verify(mockRequestCallback, times(1)).onGetRequest();
  }

  @Test
  void processReadTask_fetchObjectContentFails_callsRemoveBlocks() throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenThrow(new RuntimeException("fail"));

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void processReadTask_fetchObjectContentIOException_setsErrorOnBlocks()
      throws InterruptedException, IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    IOException testException = new IOException("S3 fetch failed");
    CompletableFuture<ObjectContent> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(testException);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenThrow(testException);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(block).setError(any(IOException.class));
    verify(mockRemoveBlocksFunc).accept(Collections.singletonList(block));
  }

  @Test
  void processReadTask_readBlocksFromStreamThrowsEOFException_callsRemoveBlocks()
      throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    InputStream throwingStream = mock(InputStream.class);
    when(throwingStream.read(any(), anyInt(), anyInt())).thenThrow(new EOFException());

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(throwingStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void processReadTask_readBlocksFromStreamThrowsEOFException_setsErrorOnBlocks()
      throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    EOFException testException = new EOFException("Premature EOF");
    InputStream throwingStream = mock(InputStream.class);
    when(throwingStream.read(any(), anyInt(), anyInt())).thenThrow(testException);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(throwingStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(block).setError(testException);
    verify(mockRemoveBlocksFunc).accept(Collections.singletonList(block));
  }

  @Test
  void processReadTask_readBlocksFromStreamThrowsIOException_callsRemoveBlocks()
      throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    InputStream throwingStream = mock(InputStream.class);
    when(throwingStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("IO error"));

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(throwingStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void processReadTask_readBlocksFromStreamThrowsIOException_setsErrorOnBlocks()
      throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    IOException testException = new IOException("Stream read error");
    InputStream throwingStream = mock(InputStream.class);
    when(throwingStream.read(any(), anyInt(), anyInt())).thenThrow(testException);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(throwingStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(block).setError(testException);
    verify(mockRemoveBlocksFunc).accept(Collections.singletonList(block));
  }

  @Test
  void processReadTask_multipleBlocks_readsAllSuccessfully() throws Exception {
    Block block1 = createMockBlock(0, 4);
    Block block2 = createMockBlock(5, 9);
    List<Block> blocks = Arrays.asList(block1, block2);

    byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc, never()).accept(any());
    verify(block1).setData(new byte[] {1, 2, 3, 4, 5});
    verify(block2).setData(new byte[] {6, 7, 8, 9, 10});
  }

  @Test
  void processReadTask_blocksWithGaps_skipsCorrectly() throws Exception {
    Block block1 = createMockBlock(0, 2);
    Block block2 = createMockBlock(5, 7); // Gap between blocks
    List<Block> blocks = Arrays.asList(block1, block2);

    byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc, never()).accept(any());
    verify(block1).setData(new byte[] {1, 2, 3});
    verify(block2).setData(new byte[] {6, 7, 8});
  }

  @Test
  void processReadTask_streamTooShort_callsRemoveBlocks() throws Exception {
    Block block = createMockBlock(0, 9);
    List<Block> blocks = Collections.singletonList(block);

    byte[] shortData = new byte[] {1, 2, 3}; // Not enough data
    InputStream testStream = new ByteArrayInputStream(shortData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void processReadTask_skipFailsDueToEOF_callsRemoveBlocks() throws Exception {
    Block block = createMockBlock(10, 14); // Start beyond available data
    List<Block> blocks = Collections.singletonList(block);

    byte[] shortData = new byte[] {1, 2, 3};
    InputStream testStream = new ByteArrayInputStream(shortData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void processReadTask_tracksMetrics() throws Exception {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockMetrics).add(MetricKey.GET_REQUEST_COUNT, 1);
  }

  @Test
  void processReadTask_asyncReadMode_buildsCorrectRequest() throws Exception {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.ASYNC);
    readTask.run();

    verify(mockObjectClient)
        .getObject(
            argThat(
                request -> {
                  Referrer referrer = request.getReferrer();
                  return referrer.getReadMode() == ReadMode.ASYNC;
                }),
            eq(openStreamInfo));
  }

  @Test
  void processReadTask_removeNonFilledBlocksFromStore_filtersCorrectly() throws IOException {
    Block filledBlock = createMockBlock(0, 2);
    Block unfilledBlock = createMockBlock(3, 5);
    when(filledBlock.isDataReady()).thenReturn(true);
    when(unfilledBlock.isDataReady()).thenReturn(false);

    List<Block> blocks = Arrays.asList(filledBlock, unfilledBlock);

    // Simulate failure scenario
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo))).thenReturn(null);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc)
        .accept(
            argThat(
                blocksToRemove ->
                    blocksToRemove.size() == 1 && blocksToRemove.contains(unfilledBlock)));
  }

  @Test
  void processReadTask_unexpectedException_wrapsInIOExceptionAndSetsErrorOnBlocks()
      throws IOException {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    RuntimeException testException = new RuntimeException("Unexpected error");
    InputStream throwingStream = mock(InputStream.class);
    when(throwingStream.read(any(), anyInt(), anyInt())).thenThrow(testException);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(throwingStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenReturn(mockContent);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(block)
        .setError(
            argThat(
                error ->
                    error != null
                        && error.getMessage().equals("Unexpected error during block reading")
                        && error.getCause() == testException));
    verify(mockRemoveBlocksFunc).accept(Collections.singletonList(block));
  }

  @Test
  void processReadTask_multipleBlocksWithError_setsErrorOnlyOnNonReadyBlocks() throws IOException {
    Block readyBlock = createMockBlock(0, 2);
    Block nonReadyBlock1 = createMockBlock(3, 5);
    Block nonReadyBlock2 = createMockBlock(6, 8);

    when(readyBlock.isDataReady()).thenReturn(true);
    when(nonReadyBlock1.isDataReady()).thenReturn(false);
    when(nonReadyBlock2.isDataReady()).thenReturn(false);

    List<Block> blocks = Arrays.asList(readyBlock, nonReadyBlock1, nonReadyBlock2);

    IOException testException = new IOException("Test error");
    CompletableFuture<ObjectContent> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(testException);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(openStreamInfo)))
        .thenThrow(testException);

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(readyBlock, never()).setError(any());
    verify(nonReadyBlock1).setError(any(IOException.class));
    verify(nonReadyBlock2).setError(any(IOException.class));
    verify(mockRemoveBlocksFunc).accept(Arrays.asList(nonReadyBlock1, nonReadyBlock2));
  }

  // Helper to call private processReadTask using reflection for testing
  private Runnable invokeProcessReadTask(List<Block> blocks, ReadMode readMode) {
    try {
      java.lang.reflect.Method method =
          StreamReader.class.getDeclaredMethod("processReadTask", List.class, ReadMode.class);
      method.setAccessible(true);
      return (Runnable) method.invoke(streamReader, blocks, readMode);
    } catch (RuntimeException e) {
      throw e; // rethrow unchecked exceptions
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke processReadTask via reflection", e);
    }
  }

  // Helper method to create a mock Block with a given range
  private Block createMockBlock(long start, long end) {
    Block mockBlock = mock(Block.class);
    BlockKey mockBlockKey = mock(BlockKey.class);
    Range range = new Range(start, end);

    when(mockBlockKey.getObjectKey()).thenReturn(mockObjectKey);
    when(mockObjectKey.getS3URI()).thenReturn(S3URI.of("dummy-bucket", "/dummy-key"));
    when(mockObjectKey.getEtag()).thenReturn("dummy-etag");

    when(mockBlock.getBlockKey()).thenReturn(mockBlockKey);
    when(mockBlockKey.getRange()).thenReturn(range);
    when(mockBlock.isDataReady()).thenReturn(false);
    when(mockBlock.getLength()).thenReturn((int) (end - start + 1));

    doAnswer(
            invocation -> {
              byte[] data = invocation.getArgument(0);
              // simulate data set by returning true on isDataReady
              when(mockBlock.isDataReady()).thenReturn(true);
              return null;
            })
        .when(mockBlock)
        .setData(any(byte[].class));

    doAnswer(
            invocation -> {
              IOException error = invocation.getArgument(0);
              // simulate error set by returning true on isDataReady
              when(mockBlock.isDataReady()).thenReturn(true);
              return null;
            })
        .when(mockBlock)
        .setError(any(IOException.class));

    return mockBlock;
  }
}
