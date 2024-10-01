package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class MemoryTrackerTest {

  @Test
  void testDefaultConstructor() {
    assertNotNull(new MemoryTracker());
  }

  @Test
  void testMemoryUsed() {
    MemoryTracker memoryTracker = new MemoryTracker();
    assertEquals(0, memoryTracker.getMemoryUsed());
    assertEquals(500, memoryTracker.incrementMemoryUsed(500));
    assertEquals(200, memoryTracker.freeMemory(300));
    assertEquals(200, memoryTracker.getMemoryUsed());
  }
}
