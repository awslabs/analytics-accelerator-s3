package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.request.ObjectMetadata;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class IOPlannerTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testCreateBoundaries() {
    assertThrows(NullPointerException.class, () -> new IOPlanner(null));
  }

  @Test
  void testPlanReadBoundaries() {
    // Given: an empty BlockStore
    final int OBJECT_SIZE = 10_000;
    MetadataStore mockMetadataStore = mock(MetadataStore.class);
    when(mockMetadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(OBJECT_SIZE).build());

    BlockStore blockStore = new BlockStore(TEST_URI, mockMetadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);


    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(-5, 10, 100));
    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(10, 5, 100));
    assertThrows(IllegalArgumentException.class, () -> ioPlanner.planRead(5, 5, 2));
  }

  @Test
  public void testPlanReadNoopWhenBlockStoreEmpty() {
    // Given: an empty BlockStore
    final int OBJECT_SIZE = 10_000;
    MetadataStore mockMetadataStore = mock(MetadataStore.class);
    when(mockMetadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(OBJECT_SIZE).build());

    BlockStore blockStore = new BlockStore(TEST_URI, mockMetadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range
    List<Range> missingRanges = ioPlanner.planRead(10, 100, OBJECT_SIZE - 1);

    // Then: it just falls through
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(10, 100));

    assertEquals(expected, missingRanges);
  }

  @Test
  public void testPlanReadDoesNotDoubleRead() {
    // Given: a BlockStore with a (100,200) block in it
    final int OBJECT_SIZE = 10_000;
    byte[] content = new byte[OBJECT_SIZE];
    MetadataStore metadataStore = getTestMetadataStoreWithContentLength(OBJECT_SIZE);
    BlockStore blockStore =
        new BlockStore(
            TEST_URI, metadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));
    FakeObjectClient fakeObjectClient =
        new FakeObjectClient(new String(content, StandardCharsets.UTF_8));
    blockStore.add(
        new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 100, 200, 0, ReadMode.SYNC));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we only request (0, 99) and (201, 400)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 99));
    expected.add(new Range(201, 400));

    assertEquals(expected, missingRanges);
  }

  @Test
  public void testPlanReadRegressionSingleByteObject() {
    // Given: a single byte object and an empty block store
    final int OBJECT_SIZE = 1;
    MetadataStore metadataStore = getTestMetadataStoreWithContentLength(OBJECT_SIZE);

    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore, PhysicalIOConfiguration.DEFAULT, mock(MemoryTracker.class));
    IOPlanner ioPlanner = new IOPlanner(blockStore);


    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we request the single byte range (0, 0)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 0));

    assertEquals(expected, missingRanges);
  }

  private MetadataStore getTestMetadataStoreWithContentLength(long contentLength) {
    MetadataStore mockMetadataStore = mock(MetadataStore.class);
    when(mockMetadataStore.get(any()))
        .thenReturn(ObjectMetadata.builder().contentLength(contentLength).build());

    return mockMetadataStore;
  }
}
