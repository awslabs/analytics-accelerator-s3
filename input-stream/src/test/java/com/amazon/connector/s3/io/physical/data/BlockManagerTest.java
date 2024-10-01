package com.amazon.connector.s3.io.physical.data;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.request.ObjectContent;
import com.amazon.connector.s3.request.ObjectMetadata;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class BlockManagerTest {
  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                null,
                mock(ObjectClient.class),
                mock(MetadataStore.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(MemoryTracker.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(S3URI.class),
                null,
                mock(MetadataStore.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(MemoryTracker.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(S3URI.class),
                mock(ObjectClient.class),
                null,
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                mock(MemoryTracker.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(S3URI.class),
                mock(ObjectClient.class),
                mock(MetadataStore.class),
                null,
                mock(PhysicalIOConfiguration.class),
                mock(MemoryTracker.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(S3URI.class),
                mock(ObjectClient.class),
                mock(MetadataStore.class),
                mock(Telemetry.class),
                null,
                mock(MemoryTracker.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(S3URI.class),
                mock(ObjectClient.class),
                mock(MetadataStore.class),
                mock(Telemetry.class),
                mock(PhysicalIOConfiguration.class),
                null));
  }

  @Test
  void testGetBlockIsEmpty() {
    // Given
    BlockManager blockManager = getTestBlockManager(42);

    // When: nothing

    // Then
    assertFalse(blockManager.getBlock(0).isPresent());
  }

  @Test
  void testGetBlockReturnsAvailableBlock() {
    // Given
    BlockManager blockManager = getTestBlockManager(65 * ONE_KB);

    // When: have a 64KB block available from 0
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertTrue(blockManager.getBlock(0).isPresent());
    assertFalse(blockManager.getBlock(64 * ONE_KB).isPresent());
  }

  @Test
  void testMakePositionAvailableRespectsReadAhead() {
    // Given
    final int objectSize = (int) PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() + ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() - 1,
        requestCaptor.getValue().getRange().getEnd());
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
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(objectSize - 1, requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  void testMakeRangeAvailableDoesNotOverread() {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 128 * ONE_KB);
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(64 * ONE_KB + 1, ReadMode.SYNC);

    // When: requesting the byte at 64KB
    blockManager.makeRangeAvailable(64 * ONE_KB, 100, ReadMode.SYNC);
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, times(3)).getObject(requestCaptor.capture());

    // Then: request size is a single byte as more is not needed
    GetRequest firstRequest = requestCaptor.getAllValues().get(0);
    GetRequest secondRequest = requestCaptor.getAllValues().get(1);
    GetRequest lastRequest = requestCaptor.getAllValues().get(2);

    assertEquals(65_536, firstRequest.getRange().getLength());
    assertEquals(65_535, secondRequest.getRange().getLength());
    assertEquals(1, lastRequest.getRange().getLength());
  }

  private BlockManager getTestBlockManager(int size) {
    return getTestBlockManager(mock(ObjectClient.class), size);
  }

  private BlockManager getTestBlockManager(ObjectClient objectClient, int size) {
    S3URI testUri = S3URI.of("foo", "bar");
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[size])).build()));

    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any())).thenReturn(ObjectMetadata.builder().contentLength(size).build());
    return new BlockManager(
        testUri,
        objectClient,
        metadataStore,
        TestTelemetry.DEFAULT,
        PhysicalIOConfiguration.DEFAULT,
        mock(MemoryTracker.class));
  }
}
