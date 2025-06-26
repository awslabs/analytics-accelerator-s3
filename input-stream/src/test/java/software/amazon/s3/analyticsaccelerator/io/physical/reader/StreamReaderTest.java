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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Block;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class StreamReaderTest {

  private ObjectClient mockObjectClient;
  private ObjectKey mockObjectKey;
  private ExecutorService mockExecutorService;
  private Consumer<List<Block>> mockRemoveBlocksFunc;
  private OpenStreamInformation mockOpenStreamInfo;
  private Metrics mockMetrics;

  private StreamReader streamReader;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    mockObjectClient = mock(ObjectClient.class);
    mockObjectKey = mock(ObjectKey.class);
    mockExecutorService = mock(ExecutorService.class);
    mockRemoveBlocksFunc = mock(Consumer.class);
    mockMetrics = mock(Metrics.class);
    mockOpenStreamInfo = mock(OpenStreamInformation.class);

    streamReader =
        new StreamReader(
            mockObjectClient,
            mockObjectKey,
            mockExecutorService,
            mockRemoveBlocksFunc,
            mockMetrics,
            mockOpenStreamInfo);
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
  void processReadTask_successfulRead_populatesBlocks() throws Exception {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream testStream = new ByteArrayInputStream(testData);

    ObjectContent mockContent = mock(ObjectContent.class);
    when(mockContent.getStream()).thenReturn(testStream);
    when(mockObjectClient.getObject(any(GetRequest.class), eq(mockOpenStreamInfo)))
        .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mockContent));

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc, never()).accept(any());
    verify(block).setData(testData);
  }

  @Test
  void processReadTask_fetchObjectContentFails_callsRemoveBlocks() {
    Block block = createMockBlock(0, 4);
    List<Block> blocks = Collections.singletonList(block);

    when(mockObjectClient.getObject(any(GetRequest.class), eq(mockOpenStreamInfo)))
        .thenThrow(new RuntimeException("fail"));

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
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
    when(mockObjectClient.getObject(any(GetRequest.class), eq(mockOpenStreamInfo)))
        .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mockContent));

    Runnable readTask = invokeProcessReadTask(blocks, ReadMode.SYNC);
    readTask.run();

    verify(mockRemoveBlocksFunc).accept(blocks);
  }

  @Test
  void close_callsObjectClientCloseAndShutsDownExecutor() throws IOException {
    streamReader.close();

    verify(mockObjectClient).close();
    verify(mockExecutorService).shutdown();
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

    doAnswer(
            invocation -> {
              byte[] data = invocation.getArgument(0);
              // simulate data set by returning true on isDataReady
              when(mockBlock.isDataReady()).thenReturn(true);
              return null;
            })
        .when(mockBlock)
        .setData(any(byte[].class));

    return mockBlock;
  }
}
