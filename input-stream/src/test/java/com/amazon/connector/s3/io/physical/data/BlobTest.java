package com.amazon.connector.s3.io.physical.data;

import static com.amazon.connector.s3.io.physical.plan.IOPlanState.SUBMITTED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.request.Range;
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
public class BlobTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String TEST_DATA = "test-data-0123456789";

  @Test
  void testCreateBoundaries() {
    assertThrows(
        NullPointerException.class,
        () ->
            new Blob(
                null, mock(MetadataStore.class), mock(BlockManager.class), TestTelemetry.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new Blob(TEST_URI, null, mock(BlockManager.class), TestTelemetry.DEFAULT));

    assertThrows(
        NullPointerException.class,
        () -> new Blob(TEST_URI, mock(MetadataStore.class), null, TestTelemetry.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new Blob(TEST_URI, mock(MetadataStore.class), mock(BlockManager.class), null));
  }

  @Test
  public void testSingleByteReadReturnsCorrectByte() {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: single byte reads are performed
    int r1 = blob.read(0);
    int r2 = blob.read(5);
    int r3 = blob.read(10);
    int r4 = blob.read(TEST_DATA.length() - 1);

    // Then: correct bytes are returned
    assertEquals(116, r1); // 't' = 116
    assertEquals(100, r2); // 'd' = 100
    assertEquals(48, r3); // '0' = 48
    assertEquals(57, r4); // '9' = 57
  }

  @Test
  public void testBufferedReadReturnsCorrectByte() {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: buffered reads are performed
    byte[] b1 = new byte[4];
    blob.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    blob.read(b2, 0, b2.length, 5);

    // Then: correct bytes are returned
    assertEquals("test", new String(b1, StandardCharsets.UTF_8));
    assertEquals("data", new String(b2, StandardCharsets.UTF_8));
  }

  @Test
  public void testBufferedReadTestOverlappingRanges() {
    // Given: test Blob
    Blob blob = getTestBlob(TEST_DATA);

    // When: buffered reads are performed
    byte[] b1 = new byte[4];
    blob.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    blob.read(b2, 0, b2.length, 2);

    // Then: correct bytes are returned
    assertEquals("test", new String(b1, StandardCharsets.UTF_8));
    assertEquals("st-d", new String(b2, StandardCharsets.UTF_8));
  }

  @Test
  public void testBufferedReadValidatesArguments() {
    // Given: test Blob
    Blob blob = getTestBlob("abc");

    // When & Then: read is called with illegal arguments, IllegalArgumentException is thrown
    byte[] b = new byte[4];

    assertThrows(IllegalArgumentException.class, () -> blob.read(-100));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, b.length, -100));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, b.length, b.length + 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, -1, b.length, 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, 0, -1, 1));
    assertThrows(IllegalArgumentException.class, () -> blob.read(b, b.length + 1, b.length, 1));
  }

  @Test
  public void testExecuteSubmitsCorrectRanges() {
    // Given: test blob and an IOPlan
    MetadataStore metadataStore = mock(MetadataStore.class);
    BlockManager blockManager = mock(BlockManager.class);
    Blob blob = new Blob(TEST_URI, metadataStore, blockManager, TestTelemetry.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 100));
    ranges.add(new Range(999, 1000));
    IOPlan ioPlan = new IOPlan(ranges);

    // When: the IOPlan is executed
    IOPlanExecution execution = blob.execute(ioPlan);

    // Then: correct ranges are submitted
    assertEquals(SUBMITTED, execution.getState());
    verify(blockManager).makeRangeAvailable(0, 101, ReadMode.ASYNC);
    verify(blockManager).makeRangeAvailable(999, 2, ReadMode.ASYNC);
  }

  @Test
  public void testCloseClosesBlockManager() {
    // Given: test blob
    MetadataStore metadataStore = mock(MetadataStore.class);
    BlockManager blockManager = mock(BlockManager.class);
    Blob blob = new Blob(TEST_URI, metadataStore, blockManager, TestTelemetry.DEFAULT);

    // When: blob is closed
    blob.close();

    // Then:
    verify(blockManager, times(1)).close();
  }

  private Blob getTestBlob(String data) {
    FakeObjectClient fakeObjectClient = new FakeObjectClient(data);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlockManager blockManager =
        new BlockManager(
            TEST_URI,
            fakeObjectClient,
            metadataStore,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(MemoryTracker.class));

    return new Blob(TEST_URI, metadataStore, blockManager, TestTelemetry.DEFAULT);
  }
}
